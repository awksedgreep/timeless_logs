defmodule TimelessLogs.LegacyReaderTest do
  use ExUnit.Case, async: false

  alias TimelessLogs.{DB, DB.Migrations, Entry, LegacyReader, Writer}

  test "SQLite generation streams one validated block at a time and stays immutable" do
    root = temp_dir("logs_sqlite_reader")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))

    entries = fixtures()

    blocks =
      [
        write_block(root, Enum.slice(entries, 0, 2), :raw),
        write_block(root, Enum.slice(entries, 2, 2), :zstd),
        write_block(root, Enum.slice(entries, 4, 2), :openzl)
      ]

    create_sqlite_index(root, blocks)
    before = tree_snapshot(root)

    assert {:ok, reader} = LegacyReader.open(root, generation: :sqlite)
    assert {:ok, %{blocks: 3, records: 6}} = LegacyReader.inventory(reader)

    assert {:ok, first, cursor, true} = LegacyReader.page(reader, nil, 2)
    assert {:ok, second, cursor, true} = LegacyReader.page(reader, cursor, 2)
    assert {:ok, third, _cursor, false} = LegacyReader.page(reader, cursor, 2)

    actual = first ++ second ++ third
    assert Enum.map(actual, & &1.message) == Enum.map(entries, & &1.message)
    # The released OpenZL generation stored the historical four-bucket level
    # byte, so :critical in that block is already indistinguishable from
    # :error. The reader preserves the source's query-visible value exactly.
    assert Enum.map(actual, & &1.level) == [
             :debug,
             :info,
             :notice,
             :warning,
             :error,
             :error
           ]

    assert Enum.map(actual, & &1.metadata) == Enum.map(entries, & &1.metadata)
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before

    assert {:ok, reader} = LegacyReader.open(root, generation: :sqlite, max_stored_bytes: 1)
    assert {:error, {:oversized, _path, _size, 1}} = LegacyReader.page(reader, nil, 2)
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before
  end

  test "snapshot and disk_log replay is read-only and honors deletes and replacements" do
    root = temp_dir("logs_snapshot_reader")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))

    [first, second, third | _] = fixtures()
    old = write_block(root, [first], :raw)
    replacement = write_block(root, [second, third], :zstd)

    snapshot = %{
      version: 1,
      timestamp: 100,
      blocks: [block_row(old)],
      term_index: [],
      compression_stats: [],
      block_data: []
    }

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(snapshot, [:compressed])
    )

    log_path = Path.join(root, "index.log")
    name = :timeless_logs_legacy_reader_fixture

    {:ok, ^name} =
      :disk_log.open(
        name: name,
        file: String.to_charlist(log_path),
        type: :halt,
        format: :internal
      )

    :ok = :disk_log.log(name, {:delete_blocks, 101, [old.block_id]})

    :ok =
      :disk_log.log(
        name,
        {:index_block, 102, block_map(replacement), ["level:info"]}
      )

    :ok = :disk_log.sync(name)
    :ok = :disk_log.close(name)
    before = tree_snapshot(root)

    assert {:ok, reader} = LegacyReader.open(root, generation: :snapshot_log)
    assert {:ok, %{blocks: 1, records: 2}} = LegacyReader.inventory(reader)
    assert {:ok, rows, _cursor, false} = LegacyReader.page(reader, nil, 8_192)
    assert Enum.map(rows, & &1.message) == [second.message, third.message]
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before
  end

  test "snapshot generation reads legacy inline block data without starting its owner" do
    root = temp_dir("logs_snapshot_inline_reader")
    on_exit(fn -> File.rm_rf!(root) end)
    [first, second | _] = fixtures()
    {:ok, block} = Writer.write_block([first, second], :memory, :raw)

    snapshot = %{
      version: 1,
      timestamp: 100,
      blocks: [block_row(block)],
      term_index: [],
      compression_stats: [],
      block_data: [{block.block_id, block.data}]
    }

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(snapshot, [:compressed])
    )

    before = tree_snapshot(root)

    assert {:ok, reader} = LegacyReader.open(root, generation: :snapshot_log)
    assert {:ok, [^first, ^second], _cursor, false} = LegacyReader.page(reader)
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before
  end

  defp fixtures do
    base = 1_700_000_000_000_000

    for {level, index} <- Enum.with_index([:debug, :info, :notice, :warning, :error, :critical]) do
      %Entry{
        timestamp: base + index,
        level: level,
        message: "message-#{index}",
        metadata: %{
          "service" => "api",
          "index" => index,
          "nested" => %{"ok" => rem(index, 2) == 0}
        }
      }
    end
  end

  defp write_block(root, entries, format) do
    {:ok, meta} = Writer.write_block(entries, root, format)
    meta
  end

  defp create_sqlite_index(root, blocks) do
    path = Path.join(root, "logs_index.db")
    {:ok, conn} = Exqlite.Sqlite3.open(path)
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

  defp tree_snapshot(root) do
    root
    |> regular_files()
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

  defp temp_dir(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    path
  end
end
