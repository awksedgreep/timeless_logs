defmodule TimelessLogs.ReleaseStartupTest do
  use ExUnit.Case, async: false

  alias TimelessLogs.{
    DB,
    DB.Migrations,
    Entry,
    LegacyReader,
    LibsqlCandidate,
    ReleaseStartup,
    Writer
  }

  test "fresh startup creates one valid public libSQL target and is idempotent" do
    root = temp_dir("logs_startup_fresh")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, %{state: :fresh, ready: false}} = ReleaseStartup.detect(root, opts())
    File.write!(Path.join(root, "empty-placeholder"), "")

    assert {:ok, %{state: :valid_libsql, ready: true, target_path: target}} =
             ReleaseStartup.prepare(root, opts())

    assert File.regular?(target)
    assert_retention(target, 7 * 86_400 * 1_000_000)

    assert {:ok, %{state: :valid_libsql, ready: true}} =
             ReleaseStartup.prepare(root, opts())

    incompatible = temp_dir("logs_startup_incompatible_extension")
    on_exit(fn -> File.rm_rf!(incompatible) end)

    assert {:error, %{state: :incompatible_version, ready: false, error: error}} =
             ReleaseStartup.prepare(incompatible, extension_path: "/missing/timeless-ext.so")

    assert error =~ "capability handshake failed"
    refute File.exists?(Path.join(incompatible, "logs.db"))
  end

  test "fresh startup persists the configured retention in the public virtual table" do
    root = temp_dir("logs_startup_retention")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, %{target_path: target}} =
             ReleaseStartup.prepare(root, Keyword.put(opts(), :retention_seconds, 90))

    assert_retention(target, 90 * 1_000_000)

    assert {:ok, %{state: :incompatible_version, error: error}} =
             ReleaseStartup.detect(root, opts())

    assert error =~ "retention mismatch"
  end

  test "legacy, resumable, cutover, and post-rename crash states are exact" do
    root = temp_dir("logs_startup_cutover")
    on_exit(fn -> File.rm_rf!(root) end)
    entries = entries(17)
    create_legacy(root, entries)
    source_before = source_snapshot(root)

    assert {:ok, %{state: :legacy, generation: :sqlite}} = ReleaseStartup.detect(root, opts())

    assert {:error, error} =
             ReleaseStartup.prepare(
               root,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    assert error.state == :legacy
    assert error.error =~ "committed work is resumable"

    migration_before_detection = migration_fingerprint(root)
    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())

    assert %{
             records_completed: 17,
             records_total: 17,
             ready: false,
             durable_records_per_second: rate,
             candidate_physical_bytes: candidate_bytes,
             process_hwm_bytes: hwm
           } = ReleaseStartup.stats(root, opts())

    assert rate > 0
    assert candidate_bytes > 0
    assert hwm > 0
    assert migration_fingerprint(root) == migration_before_detection

    assert source_snapshot(root) == source_before

    assert {:error, before_seal_error} =
             ReleaseStartup.prepare(root, Keyword.merge(opts(), failpoint: :before_seal))

    assert before_seal_error.state == :resumable_migration
    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())

    assert {:error, sealed_error} =
             ReleaseStartup.prepare(root, Keyword.merge(opts(), failpoint: :after_seal))

    assert sealed_error.state == :resumable_migration
    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())
    assert source_snapshot(root) == source_before

    assert {:error, renamed_error} =
             ReleaseStartup.prepare(
               root,
               Keyword.merge(opts(), failpoint: :after_rename_before_fsync)
             )

    assert renamed_error.state == :completed_cutover

    assert {:ok,
            %{
              state: :completed_cutover,
              ready: true,
              source_retained: true,
              source_manifest_digest: digest,
              target_path: target
            }} = ReleaseStartup.detect(root, opts())

    assert source_snapshot(root) == source_before
    refute File.exists?(TimelessLogs.ReleaseMigration.candidate_path(root))

    assert {:ok, legacy_reader} = LegacyReader.open(root)
    assert {:ok, %{records: 17}} = LegacyReader.inventory(legacy_reader)
    assert :ok = LegacyReader.close(legacy_reader)

    assert {:ok, %{state: :completed_cutover, ready: true}} =
             ReleaseStartup.prepare(root, opts())

    assert {:ok, conn, _} = LibsqlCandidate.open_connection(target, extension_path())

    try do
      assert {:ok, [[17]]} = DB.execute(conn, "SELECT COUNT(*) FROM logs", [])
    after
      Exqlite.Sqlite3.close(conn)
    end

    assert {:error, wrong_digest} = ReleaseStartup.cleanup_legacy(root, "wrong", opts())
    assert wrong_digest =~ "digest mismatch"

    assert {:error, interrupted} =
             ReleaseStartup.cleanup_legacy(
               root,
               digest,
               Keyword.merge(opts(), failpoint: {:cleanup_after_file, 1})
             )

    assert interrupted =~ "explicit logs cleanup interrupted"
    assert {:ok, %{source_retained: false}} = ReleaseStartup.cleanup_legacy(root, digest, opts())
    refute File.exists?(Path.join(root, "logs_index.db"))
    refute File.exists?(Path.join(root, "blocks"))

    assert {:ok, %{state: :completed_cutover, source_retained: false}} =
             ReleaseStartup.detect(root, opts())
  end

  test "source drift, conflicting targets, and incompatible versions fail closed" do
    drift = temp_dir("logs_startup_drift")
    on_exit(fn -> File.rm_rf!(drift) end)
    create_legacy(drift, entries(3))

    assert {:error, _} =
             ReleaseStartup.prepare(
               drift,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    [block] = Path.wildcard(Path.join([drift, "blocks", "*.*"]))
    File.write!(block, "source drift", [:append])

    assert {:ok, %{state: :corruption, error: source_error}} =
             ReleaseStartup.detect(drift, opts())

    assert source_error =~ "payload validation failed"

    dual = temp_dir("logs_startup_dual")
    on_exit(fn -> File.rm_rf!(dual) end)
    create_legacy(dual, entries(2))
    create_target(Path.join(dual, "logs.db"))

    assert {:ok, %{state: :ambiguous_dual_store, error: dual_error}} =
             ReleaseStartup.detect(dual, opts())

    assert dual_error =~ "unlinked legacy"

    future = temp_dir("logs_startup_future")
    on_exit(fn -> File.rm_rf!(future) end)
    target = Path.join(future, "logs.db")
    create_target(target)
    {:ok, conn} = Exqlite.Sqlite3.open(target)

    assert {:ok, _} =
             DB.execute(
               conn,
               "INSERT INTO _timeless_schema_migrations VALUES ('logs',2,unixepoch(),'future','future',1)",
               []
             )

    Exqlite.Sqlite3.close(conn)

    assert {:ok, %{state: :incompatible_version, error: version_error}} =
             ReleaseStartup.detect(future, opts())

    assert version_error =~ "newer than supported"

    checkpoint = temp_dir("logs_startup_checkpoint_corrupt")
    on_exit(fn -> File.rm_rf!(checkpoint) end)
    create_legacy(checkpoint, entries(2))

    assert {:error, _} =
             ReleaseStartup.prepare(
               checkpoint,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    candidate = TimelessLogs.ReleaseMigration.candidate_path(checkpoint)
    {:ok, conn} = Exqlite.Sqlite3.open(candidate)
    assert {:ok, _} = DB.execute(conn, "UPDATE _timeless_migration SET records_completed=3", [])
    Exqlite.Sqlite3.close(conn)

    assert {:ok, %{state: :corruption, error: checkpoint_error}} =
             ReleaseStartup.detect(checkpoint, opts())

    assert checkpoint_error =~ "checkpoint failed semantic validation"
  end

  test "corrupt, wrong-signal, ambiguous generations, and active owners never start" do
    corrupt = temp_dir("logs_startup_corrupt")
    on_exit(fn -> File.rm_rf!(corrupt) end)
    File.write!(Path.join(corrupt, "logs.db"), "not sqlite")

    assert {:ok, %{state: :corruption}} = ReleaseStartup.detect(corrupt, opts())
    assert {:error, %{state: :corruption, ready: false}} = ReleaseStartup.prepare(corrupt, opts())

    wrong = temp_dir("logs_startup_wrong_signal")
    on_exit(fn -> File.rm_rf!(wrong) end)
    create_wrong_signal_target(Path.join(wrong, "logs.db"))

    assert {:ok, %{state: :corruption, error: wrong_error}} = ReleaseStartup.detect(wrong, opts())
    assert wrong_error =~ "wrong-signal"

    generations = temp_dir("logs_startup_generations")
    on_exit(fn -> File.rm_rf!(generations) end)
    create_legacy(generations, entries(1))

    File.write!(
      Path.join(generations, "index.snapshot"),
      :erlang.term_to_binary(%{version: 1, timestamp: 0, blocks: []})
    )

    assert {:ok, %{state: :ambiguous_dual_store}} = ReleaseStartup.detect(generations, opts())

    owner = temp_dir("logs_startup_owner")
    on_exit(fn -> File.rm_rf!(owner) end)
    owner_dir = Path.join([owner, ".timeless-migration", "logs"])
    File.mkdir_p!(owner_dir)
    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(owner_dir, "owner.db"))
    assert {:ok, _} = DB.execute(conn, "CREATE TABLE owner(singleton INTEGER PRIMARY KEY)", [])
    assert {:ok, _} = DB.execute(conn, "BEGIN EXCLUSIVE", [])

    assert {:error, %{state: :corruption, error: owner_error}} =
             ReleaseStartup.prepare(owner, opts())

    assert owner_error =~ "owner is active"
    assert {:ok, _} = DB.execute(conn, "ROLLBACK", [])
    Exqlite.Sqlite3.close(conn)

    locked = temp_dir("logs_startup_legacy_owner")
    on_exit(fn -> File.rm_rf!(locked) end)
    create_legacy(locked, entries(1))
    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(locked, "logs_index.db"))
    assert {:ok, _} = DB.execute(conn, "BEGIN EXCLUSIVE", [])
    assert {:error, %{error: legacy_owner_error}} = ReleaseStartup.prepare(locked, opts())
    assert legacy_owner_error =~ "active legacy logs SQLite owner"
    assert {:ok, _} = DB.execute(conn, "ROLLBACK", [])
    Exqlite.Sqlite3.close(conn)

    missing = temp_dir("logs_startup_missing_block")
    on_exit(fn -> File.rm_rf!(missing) end)
    create_legacy(missing, entries(1))
    [payload] = Path.wildcard(Path.join([missing, "blocks", "*.*"]))
    File.rm!(payload)

    assert {:ok, %{state: :corruption, error: missing_error}} =
             ReleaseStartup.detect(missing, opts())

    assert missing_error =~ "payload validation failed"
  end

  test "zero-byte canonical placeholders are ignored but recognized symlinks are refused" do
    root = temp_dir("logs_startup_placeholders")
    on_exit(fn -> File.rm_rf!(root) end)
    File.write!(Path.join(root, "logs.db"), "")
    assert {:ok, %{state: :fresh}} = ReleaseStartup.detect(root, opts())

    File.rm!(Path.join(root, "logs.db"))
    File.ln_s!("missing", Path.join(root, "logs.db"))
    assert {:ok, %{state: :corruption, error: error}} = ReleaseStartup.detect(root, opts())
    assert error =~ "symlink"
  end

  test "an untrappable process death after sealing resumes like a host restart" do
    root = temp_dir("logs_startup_kill")
    on_exit(fn -> File.rm_rf!(root) end)
    create_legacy(root, entries(5))
    source_before = source_snapshot(root)
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        result =
          ReleaseStartup.prepare(
            root,
            Keyword.merge(opts(), pause_at: :after_seal, notify: parent)
          )

        send(parent, {:unexpected_startup_result, result})
      end)

    assert_receive {:startup_paused, ^pid, :after_seal}, 5_000
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000
    refute_receive {:unexpected_startup_result, _}

    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())
    assert {:ok, %{state: :completed_cutover}} = ReleaseStartup.prepare(root, opts())
    assert source_snapshot(root) == source_before
  end

  test "fresh-create and final-fsync boundaries are independently restartable" do
    before = temp_dir("logs_startup_before_fresh")
    on_exit(fn -> File.rm_rf!(before) end)

    assert {:error, _} =
             ReleaseStartup.prepare(
               before,
               Keyword.merge(opts(), failpoint: :before_fresh_create)
             )

    assert {:ok, %{state: :fresh}} = ReleaseStartup.detect(before, opts())
    assert {:ok, %{state: :valid_libsql}} = ReleaseStartup.prepare(before, opts())

    after_create = temp_dir("logs_startup_after_fresh")
    on_exit(fn -> File.rm_rf!(after_create) end)

    assert {:error, _} =
             ReleaseStartup.prepare(
               after_create,
               Keyword.merge(opts(), failpoint: :after_fresh_create)
             )

    assert {:ok, %{state: :valid_libsql}} = ReleaseStartup.detect(after_create, opts())

    after_fsync = temp_dir("logs_startup_after_fsync")
    on_exit(fn -> File.rm_rf!(after_fsync) end)
    create_legacy(after_fsync, entries(2))
    File.write!(Path.join(after_fsync, "logs.db"), "")

    assert {:error, _} =
             ReleaseStartup.prepare(after_fsync, Keyword.merge(opts(), failpoint: :after_fsync))

    assert {:ok, %{state: :completed_cutover}} = ReleaseStartup.detect(after_fsync, opts())
    assert {:ok, %{state: :completed_cutover}} = ReleaseStartup.prepare(after_fsync, opts())

    after_source_fsync = temp_dir("logs_startup_after_source_fsync")
    on_exit(fn -> File.rm_rf!(after_source_fsync) end)
    create_legacy(after_source_fsync, entries(2))

    assert {:error, _} =
             ReleaseStartup.prepare(
               after_source_fsync,
               Keyword.merge(opts(), failpoint: :after_source_parent_fsync)
             )

    assert {:ok, %{state: :completed_cutover}} =
             ReleaseStartup.detect(after_source_fsync, opts())

    assert {:ok, %{state: :completed_cutover}} =
             ReleaseStartup.prepare(after_source_fsync, opts())
  end

  test "the oldest snapshot-only generation converts automatically" do
    root = temp_dir("logs_startup_snapshot_only")
    on_exit(fn -> File.rm_rf!(root) end)

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(
        %{version: 1, timestamp: 0, blocks: [], term_index: [], compression_stats: []},
        [:compressed]
      )
    )

    assert {:ok, %{state: :legacy, generation: :snapshot_log, records_total: 0}} =
             ReleaseStartup.detect(root, opts())

    assert {:ok, %{state: :completed_cutover, source_retained: true}} =
             ReleaseStartup.prepare(root, opts())
  end

  defp create_legacy(root, entries) do
    File.mkdir_p!(Path.join(root, "blocks"))
    {:ok, block} = Writer.write_block(entries, root, :raw)
    path = Path.join(root, "logs_index.db")
    {:ok, conn} = Exqlite.Sqlite3.open(path)
    Migrations.run(conn)

    assert {:ok, _} =
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

    Exqlite.Sqlite3.close(conn)
  end

  defp create_target(path) do
    File.mkdir_p!(Path.dirname(path))

    {:ok, writer} =
      LibsqlCandidate.start_link(
        path: path,
        extension_path: extension_path(),
        retention_seconds: 7 * 86_400
      )

    GenServer.stop(writer)
  end

  defp create_wrong_signal_target(path) do
    File.mkdir_p!(Path.dirname(path))
    {:ok, conn} = Exqlite.Sqlite3.open(path)
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, true)
    assert {:ok, _} = DB.execute(conn, "SELECT load_extension(?1)", [extension_path()])
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, false)
    assert {:ok, _} = DB.execute(conn, "CREATE VIRTUAL TABLE traces USING timeless_traces", [])
    Exqlite.Sqlite3.close(conn)
  end

  defp entries(count) do
    for index <- 0..(count - 1) do
      %Entry{
        timestamp: 1_700_000_000_000_000 + index,
        level: if(rem(index, 2) == 0, do: :info, else: :error),
        message: "startup-#{index}",
        metadata: %{"service" => "api", "index" => index}
      }
    end
  end

  defp source_snapshot(root) do
    [Path.join(root, "logs_index.db"), Path.join(root, "blocks")]
    |> Enum.flat_map(&regular_files/1)
    |> Enum.sort()
    |> Enum.map(fn path ->
      stat = File.stat!(path, time: :posix)

      {Path.relative_to(path, root), stat.size, stat.mtime,
       :crypto.hash(:sha256, File.read!(path))}
    end)
  end

  defp migration_fingerprint(root) do
    path = TimelessLogs.ReleaseMigration.candidate_path(root)
    assert {:ok, conn, _} = LibsqlCandidate.open_readonly_connection(path, extension_path())

    try do
      for sql <- [
            "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
            "SELECT * FROM _timeless_migration",
            "SELECT * FROM _timeless_migration_events ORDER BY sequence",
            "SELECT COUNT(*) FROM logs"
          ] do
        assert {:ok, rows} = DB.execute(conn, sql, [])
        rows
      end
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp regular_files(path) do
    if File.dir?(path) do
      path |> File.ls!() |> Enum.flat_map(&regular_files(Path.join(path, &1)))
    else
      [path]
    end
  end

  defp assert_retention(path, expected) do
    assert {:ok, conn, _} = LibsqlCandidate.open_readonly_connection(path, extension_path())

    try do
      assert {:ok, [[^expected]]} =
               DB.execute(
                 conn,
                 "SELECT CAST(v AS INTEGER) FROM logs_meta WHERE k='retention'",
                 []
               )
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp opts, do: [extension_path: extension_path(), retention_seconds: 7 * 86_400]

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
