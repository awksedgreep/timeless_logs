defmodule TimelessLogs.ReleaseMigrationTest do
  use ExUnit.Case, async: false

  alias TimelessLogs.{DB, DB.Migrations, Entry, LibsqlCandidate, Writer}

  test "8,192-entry public checkpoints resume at every crash boundary with exact cold parity" do
    root = temp_dir("logs_release_migration")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))

    entries = fixtures(8_193)
    block = write_block(root, entries, :raw)
    create_sqlite_index(root, [block])
    before = tree_snapshot(root)

    assert {:error, disk_error} =
             TimelessLogs.ReleaseMigration.stage(root, available_bytes: 0)

    assert disk_error =~ "insufficient disk"
    refute File.exists?(TimelessLogs.ReleaseMigration.candidate_path(root))

    for point <- [
          :before_batch,
          :disk_full,
          :after_batch_before_journal,
          :after_journal_before_commit
        ] do
      assert {:error, error} =
               TimelessLogs.ReleaseMigration.stage(root,
                 failpoint: {point, 1},
                 extension_path: extension_path()
               )

      assert error =~ "injected migration failure"
      assert journal_count(root) == 0
      assert tree_snapshot(root) == before
    end

    assert {:error, error} =
             TimelessLogs.ReleaseMigration.stage(root,
               failpoint: {:after_checkpoint, 1},
               extension_path: extension_path()
             )

    assert error =~ "committed work is resumable"
    assert journal_count(root) == 8_192
    assert tree_snapshot(root) == before

    assert {:ok, report} =
             TimelessLogs.ReleaseMigration.stage(root, extension_path: extension_path())

    assert report.phase == :verified
    assert report.records == 8_193
    assert report.checkpoints == 2
    assert report.retries == 5
    assert report.wal_bytes == 0
    assert report.candidate_bytes > 0
    assert report.process_hwm_bytes > 0
    assert tree_snapshot(root) == before

    assert {:ok, conn, _} =
             LibsqlCandidate.open_connection(
               TimelessLogs.ReleaseMigration.candidate_path(root),
               extension_path()
             )

    try do
      assert {:ok, [[8_193]]} = DB.execute(conn, "SELECT COUNT(*) FROM logs", [])

      assert {:ok, rows} =
               DB.execute(
                 conn,
                 "SELECT ts,level,message,metadata FROM logs WHERE ts=?1 ORDER BY ts ASC",
                 [1_700_000_000_000_000]
               )

      assert Enum.map(rows, &Enum.at(&1, 2)) == ["message-0", "message-1"]
      assert Enum.map(rows, &Enum.at(&1, 1)) == ["emergency", "notice"]

      [first | _] = rows
      metadata = first |> Enum.at(3) |> :json.decode()
      assert metadata["bool"] == true
      assert metadata["nil"] == :null
      assert metadata["nested"] == %{"index" => 0}
    after
      Exqlite.Sqlite3.close(conn)
    end

    assert {:ok, retry} =
             TimelessLogs.ReleaseMigration.stage(root, extension_path: extension_path())

    assert retry.records == report.records
    assert retry.identity_digest == report.identity_digest
    assert retry.checkpoints == report.checkpoints
    assert retry.retries == report.retries + 1
    assert tree_snapshot(root) == before
  end

  test "snapshot plus disk-log generation migrates through the same public rich batch" do
    root = temp_dir("logs_snapshot_migration")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))

    [first, second, third | _] = fixtures(3)
    old = write_block(root, [first], :raw)
    replacement = write_block(root, [second, third], :zstd)

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(
        %{
          version: 1,
          timestamp: 100,
          blocks: [block_row(old)],
          term_index: [],
          compression_stats: [],
          block_data: []
        },
        [:compressed]
      )
    )

    name = :timeless_logs_release_snapshot_fixture

    {:ok, ^name} =
      :disk_log.open(
        name: name,
        file: String.to_charlist(Path.join(root, "index.log")),
        type: :halt,
        format: :internal
      )

    :ok = :disk_log.log(name, {:delete_blocks, 101, [old.block_id]})
    :ok = :disk_log.log(name, {:index_block, 102, block_map(replacement), []})
    :ok = :disk_log.sync(name)
    :ok = :disk_log.close(name)
    before = tree_snapshot(root)

    assert {:ok, report} =
             TimelessLogs.ReleaseMigration.stage(root,
               generation: :snapshot_log,
               extension_path: extension_path()
             )

    assert report.records == 2
    assert report.phase == :verified
    assert tree_snapshot(root) == before
  end

  test "fresh migration reports scan, public write, maintenance, storage, and HWM costs" do
    root = temp_dir("logs_release_migration_benchmark")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    block = write_block(root, fixtures(8_193), :raw)
    create_sqlite_index(root, [block])

    assert {:ok, report} =
             TimelessLogs.ReleaseMigration.stage(root, extension_path: extension_path())

    assert report.records == 8_193
    assert report.source_scan_ns > 0
    assert report.public_write_ns > 0
    assert report.optimize_ns > 0
    assert report.checkpoint_ns > 0
    assert report.physical_bytes >= report.candidate_bytes
    assert report.process_hwm_bytes > 0

    if System.get_env("TIMELESS_MIGRATION_BENCH") == "1",
      do: IO.inspect(report, label: "logs migration benchmark")
  end

  defp fixtures(count) do
    severities = [:emergency, :notice, :warning, :error, :critical, :alert, :info, :debug]
    base = 1_700_000_000_000_000

    for index <- 0..(count - 1) do
      %Entry{
        timestamp: base + div(index, 2),
        level: Enum.at(severities, rem(index, length(severities))),
        message: "message-#{index}",
        metadata: %{
          "service" => "api",
          "bool" => rem(index, 2) == 0,
          "nil" => nil,
          "number" => index,
          "nested" => %{"index" => index}
        }
      }
    end
  end

  defp write_block(root, entries, format) do
    {:ok, block} = Writer.write_block(entries, root, format)
    block
  end

  defp create_sqlite_index(root, blocks) do
    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(root, "logs_index.db"))
    Migrations.run(conn)

    Enum.each(blocks, fn block ->
      {:ok, _} =
        DB.execute(
          conn,
          "INSERT INTO blocks(block_id,file_path,byte_size,entry_count,ts_min,ts_max,format,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
          [
            block.block_id,
            block.file_path,
            block.byte_size,
            block.entry_count,
            block.ts_min,
            block.ts_max,
            Atom.to_string(block.format),
            System.system_time(:second)
          ]
        )
    end)

    Exqlite.Sqlite3.close(conn)
  end

  defp block_row(block) do
    {
      block.block_id,
      block.file_path,
      block.byte_size,
      block.entry_count,
      block.ts_min,
      block.ts_max,
      block.format,
      System.system_time(:second)
    }
  end

  defp block_map(block) do
    %{
      block_id: block.block_id,
      file_path: block.file_path,
      byte_size: block.byte_size,
      entry_count: block.entry_count,
      ts_min: block.ts_min,
      ts_max: block.ts_max,
      format: block.format
    }
  end

  defp journal_count(root) do
    {:ok, conn} =
      Exqlite.Sqlite3.open(TimelessLogs.ReleaseMigration.candidate_path(root), mode: :readonly)

    try do
      {:ok, [[count]]} =
        DB.execute(
          conn,
          "SELECT records_completed FROM _timeless_migration WHERE singleton=1",
          []
        )

      count
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp tree_snapshot(root) do
    root
    |> regular_files()
    |> Enum.reject(&String.contains?(&1, "/.timeless-migration/"))
    |> Enum.sort()
    |> Enum.map(fn path ->
      stat = File.stat!(path, time: :posix)

      {Path.relative_to(path, root), stat.size, stat.mtime,
       :crypto.hash(:sha256, File.read!(path))}
    end)
  end

  defp regular_files(root) do
    root
    |> File.ls!()
    |> Enum.flat_map(fn name ->
      path = Path.join(root, name)
      if File.dir?(path), do: regular_files(path), else: [path]
    end)
  end

  defp extension_path do
    System.get_env("TIMELESS_EXT_PATH") ||
      Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)
  end

  defp temp_dir(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    path
  end
end
