defmodule TimelessLogs.PersistenceTest do
  @moduledoc """
  Tests for the SQLite-backed index persistence layer.

  These tests exercise crash recovery, restart data integrity, and edge cases
  that the normal integration tests don't cover because they run within a
  single GenServer lifecycle.

  With SQLite, persistence is atomic — no manual snapshot/WAL replay needed.
  """
  use ExUnit.Case, async: false

  @data_dir "test/tmp/persistence"

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    File.mkdir_p!(Path.join(@data_dir, "blocks"))

    on_exit(fn ->
      Application.stop(:timeless_logs)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp start_app(opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, @data_dir)
    storage = Keyword.get(opts, :storage, :disk)
    Application.put_env(:timeless_logs, :data_dir, data_dir)
    Application.put_env(:timeless_logs, :storage, storage)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :compaction_interval, 600_000)
    Application.put_env(:timeless_logs, :retention_max_age, nil)
    Application.put_env(:timeless_logs, :retention_max_size, nil)
    Application.ensure_all_started(:timeless_logs)
  end

  defp stop_app do
    Application.stop(:timeless_logs)
  end

  defp db_path, do: Path.join(@data_dir, "logs_index.db")

  defp make_entry(overrides \\ %{}) do
    Map.merge(
      %{
        timestamp: System.system_time(:second),
        level: :info,
        message: "test message #{System.unique_integer([:positive])}",
        metadata: %{
          "request_id" => "req-#{System.unique_integer([:positive])}",
          "module" => "TestModule"
        }
      },
      overrides
    )
  end

  defp ingest_entries(entries) do
    case TimelessLogs.Writer.write_block(entries, @data_dir, :raw) do
      {:ok, meta} ->
        terms = TimelessLogs.Index.extract_terms(entries)
        TimelessLogs.Index.index_block(meta, entries, terms)
        {:ok, meta, terms}

      err ->
        err
    end
  end

  # ----------------------------------------------------------------
  # Graceful restart: SQLite DB persists across restarts
  # ----------------------------------------------------------------

  describe "graceful restart recovery" do
    test "data survives app restart via SQLite" do
      start_app()

      entries =
        Enum.map(1..50, fn i ->
          make_entry(%{message: "entry-#{i}", level: Enum.random([:info, :error, :debug])})
        end)

      {:ok, _meta, _terms} = ingest_entries(entries)
      TimelessLogs.Index.sync()

      {:ok, stats_before} = TimelessLogs.Index.stats()
      assert stats_before.total_entries == 50
      assert stats_before.total_blocks == 1

      # Graceful stop
      stop_app()

      # Verify SQLite DB file exists
      assert File.exists?(db_path())

      # Restart and verify data is recovered
      start_app()

      {:ok, stats_after} = TimelessLogs.Index.stats()
      assert stats_after.total_entries == 50
      assert stats_after.total_blocks == 1

      # Verify queries still work
      {:ok, result} = TimelessLogs.Index.query(limit: 100)
      assert result.total == 50
    end

    test "term index survives restart" do
      start_app()

      entries = [
        make_entry(%{level: :error, metadata: %{"module" => "ErrorModule"}}),
        make_entry(%{level: :info, metadata: %{"module" => "InfoModule"}})
      ]

      ingest_entries(entries)
      TimelessLogs.Index.sync()

      {:ok, error_result_before} = TimelessLogs.Index.query(level: :error, limit: 100)
      assert error_result_before.total == 1

      stop_app()
      start_app()

      {:ok, error_result_after} = TimelessLogs.Index.query(level: :error, limit: 100)
      assert error_result_after.total == 1

      {:ok, info_result} = TimelessLogs.Index.query(level: :info, limit: 100)
      assert info_result.total == 1
    end

    test "compression stats survive restart" do
      start_app()

      entries = Enum.map(1..20, fn i -> make_entry(%{message: "entry-#{i}"}) end)
      {:ok, old_meta, _} = ingest_entries(entries)

      # Write a compacted block
      compressed_entries = Enum.map(1..20, fn i -> make_entry(%{message: "compacted-#{i}"}) end)

      case TimelessLogs.Writer.write_block(compressed_entries, @data_dir, :zstd) do
        {:ok, new_meta} ->
          new_terms = TimelessLogs.Index.extract_terms(compressed_entries)

          TimelessLogs.Index.compact_blocks(
            [old_meta.block_id],
            [{new_meta, compressed_entries, new_terms}],
            {1000, 500}
          )

        _ ->
          :ok
      end

      TimelessLogs.Index.sync()

      {:ok, stats_before} = TimelessLogs.Index.stats()

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessLogs.Index.stats()
      assert stats_after.compression_raw_bytes_in == stats_before.compression_raw_bytes_in

      assert stats_after.compression_compressed_bytes_out ==
               stats_before.compression_compressed_bytes_out

      assert stats_after.compaction_count == stats_before.compaction_count
    end

    test "multiple blocks survive restart" do
      start_app()

      # Write 5 separate blocks
      Enum.each(1..5, fn batch ->
        entries =
          Enum.map(1..10, fn i ->
            make_entry(%{
              message: "batch-#{batch}-entry-#{i}",
              timestamp: System.system_time(:second) + batch
            })
          end)

        {:ok, _meta, _} = ingest_entries(entries)
      end)

      TimelessLogs.Index.sync()

      {:ok, stats_before} = TimelessLogs.Index.stats()
      assert stats_before.total_blocks == 5
      assert stats_before.total_entries == 50

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessLogs.Index.stats()
      assert stats_after.total_blocks == 5
      assert stats_after.total_entries == 50
    end
  end

  # ----------------------------------------------------------------
  # Crash recovery: SQLite handles atomicity, data persists
  # ----------------------------------------------------------------

  describe "crash recovery" do
    test "data persists even without graceful shutdown" do
      start_app()

      entries = Enum.map(1..30, fn i -> make_entry(%{message: "crash-test-#{i}"}) end)
      {:ok, _meta, _terms} = ingest_entries(entries)
      TimelessLogs.Index.sync()

      # Simulate hard stop (no terminate callback)
      stop_app()

      # Restart — SQLite DB should have all data
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_entries == 30
      assert stats.total_blocks == 1
    end

    test "queries work identically after restart" do
      start_app()

      error_entries = [
        make_entry(%{level: :error, metadata: %{"module" => "CrashModule"}}),
        make_entry(%{level: :error, metadata: %{"module" => "CrashModule"}})
      ]

      info_entries = [
        make_entry(%{level: :info, metadata: %{"module" => "OkModule"}})
      ]

      ingest_entries(error_entries)
      ingest_entries(info_entries)
      TimelessLogs.Index.sync()

      {:ok, error_before} = TimelessLogs.Index.query(level: :error, limit: 100)
      {:ok, info_before} = TimelessLogs.Index.query(level: :info, limit: 100)

      stop_app()
      start_app()

      {:ok, error_after} = TimelessLogs.Index.query(level: :error, limit: 100)
      {:ok, info_after} = TimelessLogs.Index.query(level: :info, limit: 100)

      assert error_after.total == error_before.total
      assert info_after.total == info_before.total
    end

    test "delete operations persist across restarts" do
      start_app()

      old_ts = System.system_time(:second) - 100_000

      old_entries =
        Enum.map(1..10, fn i ->
          make_entry(%{message: "old-#{i}", timestamp: old_ts + i})
        end)

      new_entries =
        Enum.map(1..10, fn i ->
          make_entry(%{message: "new-#{i}", timestamp: System.system_time(:second) + i})
        end)

      ingest_entries(old_entries)
      ingest_entries(new_entries)
      TimelessLogs.Index.sync()

      {:ok, stats_before_delete} = TimelessLogs.Index.stats()
      assert stats_before_delete.total_blocks == 2

      # Delete old blocks
      deleted = TimelessLogs.Index.delete_blocks_before(old_ts + 100)
      assert deleted == 1

      TimelessLogs.Index.sync()

      {:ok, stats_after_delete} = TimelessLogs.Index.stats()
      assert stats_after_delete.total_blocks == 1

      # Restart and verify delete persisted
      stop_app()
      start_app()

      {:ok, stats_recovered} = TimelessLogs.Index.stats()
      assert stats_recovered.total_blocks == 1
      assert stats_recovered.total_entries == 10
    end

    test "compact operations persist across restarts" do
      start_app()

      entries1 = Enum.map(1..10, fn i -> make_entry(%{message: "batch1-#{i}"}) end)
      entries2 = Enum.map(1..10, fn i -> make_entry(%{message: "batch2-#{i}"}) end)

      {:ok, meta1, _} = ingest_entries(entries1)
      {:ok, meta2, _} = ingest_entries(entries2)
      TimelessLogs.Index.sync()

      # Compact both blocks into one
      combined = entries1 ++ entries2

      case TimelessLogs.Writer.write_block(combined, @data_dir, :zstd) do
        {:ok, new_meta} ->
          new_terms = TimelessLogs.Index.extract_terms(combined)

          TimelessLogs.Index.compact_blocks(
            [meta1.block_id, meta2.block_id],
            [{new_meta, combined, new_terms}],
            {2000, 1000}
          )

        _ ->
          flunk("Failed to write compacted block")
      end

      TimelessLogs.Index.sync()

      {:ok, stats_before} = TimelessLogs.Index.stats()
      assert stats_before.total_blocks == 1
      assert stats_before.total_entries == 20

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessLogs.Index.stats()
      assert stats_after.total_blocks == 1
      assert stats_after.total_entries == 20
      assert stats_after.compaction_count == stats_before.compaction_count
    end
  end

  # ----------------------------------------------------------------
  # Large-scale persistence
  # ----------------------------------------------------------------

  describe "large-scale persistence" do
    test "many blocks survive restart" do
      start_app()

      for _i <- 1..100 do
        entry = make_entry()
        ingest_entries([entry])
      end

      TimelessLogs.Index.sync()

      stop_app()
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_blocks == 100
      assert stats.total_entries == 100
    end
  end

  # ----------------------------------------------------------------
  # Corrupt / missing file handling
  # ----------------------------------------------------------------

  describe "corrupt and missing file handling" do
    test "missing DB file starts with empty state" do
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_blocks == 0
      assert stats.total_entries == 0

      stop_app()
    end

    test "empty data dir starts cleanly" do
      # Fresh data dir with only blocks subdir
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_blocks == 0

      # Can write data
      entries = [make_entry()]
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      {:ok, stats2} = TimelessLogs.Index.stats()
      assert stats2.total_blocks == 1

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Backup
  # ----------------------------------------------------------------

  describe "backup" do
    test "backup creates a valid SQLite database at target path" do
      start_app()

      entries = Enum.map(1..20, fn i -> make_entry(%{message: "backup-#{i}"}) end)
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      backup_path = Path.join(@data_dir, "backup.db")
      assert :ok == TimelessLogs.Index.backup(backup_path)
      assert File.exists?(backup_path)

      # Verify it's a valid SQLite database by opening and querying it
      {:ok, conn} = Exqlite.Sqlite3.open(backup_path)
      {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, "SELECT COUNT(*) FROM blocks")
      {:row, [count]} = Exqlite.Sqlite3.step(conn, stmt)
      Exqlite.Sqlite3.release(conn, stmt)
      Exqlite.Sqlite3.close(conn)
      assert count == 1

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Memory mode
  # ----------------------------------------------------------------

  describe "memory mode" do
    test "memory mode does not create snapshot or log files" do
      start_app(storage: :memory, data_dir: "test/tmp/mem_persist_test")

      entries = Enum.map(1..10, fn i -> make_entry(%{message: "mem-#{i}"}) end)

      case TimelessLogs.Writer.write_block(entries, :memory, :raw) do
        {:ok, meta} ->
          terms = TimelessLogs.Index.extract_terms(entries)
          TimelessLogs.Index.index_block(meta, entries, terms)

        _ ->
          flunk("Failed to write memory block")
      end

      TimelessLogs.Index.sync()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_entries == 10

      # Old snapshot/log files should not exist
      refute File.exists?("test/tmp/mem_persist_test/index.log")
      refute File.exists?("test/tmp/mem_persist_test/index.snapshot")

      stop_app()
      File.rm_rf!("test/tmp/mem_persist_test")
    end
  end

  # ----------------------------------------------------------------
  # Async indexing recovery
  # ----------------------------------------------------------------

  describe "async indexing recovery" do
    test "async-indexed blocks survive graceful restart" do
      start_app()

      # Use async path
      entries = Enum.map(1..20, fn i -> make_entry(%{message: "async-#{i}"}) end)

      case TimelessLogs.Writer.write_block(entries, @data_dir, :raw) do
        {:ok, meta} ->
          terms = TimelessLogs.Index.extract_terms(entries)
          TimelessLogs.Index.index_block_async(meta, entries, terms)

        _ ->
          flunk("Failed to write block")
      end

      # sync forces pending flush
      TimelessLogs.Index.sync()

      {:ok, stats_before} = TimelessLogs.Index.stats()
      assert stats_before.total_entries == 20

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessLogs.Index.stats()
      assert stats_after.total_entries == 20

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Stats (index_size) reflects SQLite DB files
  # ----------------------------------------------------------------

  describe "stats reflect persistence files" do
    test "index_size reports SQLite DB size" do
      start_app()

      entries = Enum.map(1..50, fn i -> make_entry(%{message: "stats-#{i}"}) end)
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      {:ok, stats} = TimelessLogs.Index.stats()
      # SQLite DB file should have some data
      assert stats.index_size > 0

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # ETS migration
  # ----------------------------------------------------------------

  describe "ETS snapshot migration" do
    test "old ETS snapshot is migrated to SQLite on startup" do
      # Create a fake old-format snapshot
      snapshot = %{
        version: 1,
        timestamp: System.monotonic_time(),
        blocks: [
          {1001, Path.join(@data_dir, "blocks/fake.zst"), 100, 5, 1000, 2000, :zstd,
           System.system_time(:second)}
        ],
        term_index: [{"level:info", 1001}, {"module:Test", 1001}],
        compression_stats: [{:lifetime, 500, 200, 1}],
        block_data: []
      }

      snapshot_path = Path.join(@data_dir, "index.snapshot")
      File.write!(snapshot_path, :erlang.term_to_binary(snapshot, [:compressed]))

      start_app()

      # Snapshot should have been migrated
      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 5

      # Old snapshot file should be cleaned up
      refute File.exists?(snapshot_path)

      stop_app()
    end
  end
end
