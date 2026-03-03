defmodule TimelessLogs.PersistenceTest do
  @moduledoc """
  Tests for the disk_log + ETS snapshot persistence layer.

  These tests exercise crash recovery, log replay, snapshot correctness,
  and edge cases that the normal integration tests don't cover because
  they run within a single GenServer lifecycle.

  Crash recovery is simulated by: graceful stop (writes snapshot + keeps log),
  then deleting the snapshot, then restarting (forces log-only replay).
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

  defp snapshot_path, do: Path.join(@data_dir, "index.snapshot")
  defp log_path, do: Path.join(@data_dir, "index.log")

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
  # Graceful restart: snapshot written on terminate, loaded on init
  # ----------------------------------------------------------------

  describe "graceful restart recovery" do
    test "data survives app restart via snapshot" do
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

      # Graceful stop writes snapshot
      stop_app()

      # Verify snapshot file exists
      assert File.exists?(snapshot_path())

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
  # Log-only recovery: simulate crash by deleting snapshot after stop
  # ----------------------------------------------------------------

  describe "crash recovery via log replay" do
    test "data recoverable from log when snapshot is lost" do
      start_app()

      entries = Enum.map(1..30, fn i -> make_entry(%{message: "crash-test-#{i}"}) end)
      {:ok, _meta, _terms} = ingest_entries(entries)
      TimelessLogs.Index.sync()

      # Graceful stop writes snapshot and keeps log (log is NOT truncated)
      stop_app()

      # Delete snapshot to force log-only replay
      File.rm!(snapshot_path())
      refute File.exists?(snapshot_path())
      assert File.exists?(log_path())

      # Restart — should replay from log
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_entries == 30
      assert stats.total_blocks == 1
    end

    test "log replay produces same query results as before" do
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

      # Stop gracefully, then remove snapshot
      stop_app()
      File.rm!(snapshot_path())

      start_app()

      {:ok, error_after} = TimelessLogs.Index.query(level: :error, limit: 100)
      {:ok, info_after} = TimelessLogs.Index.query(level: :info, limit: 100)

      assert error_after.total == error_before.total
      assert info_after.total == info_before.total
    end

    test "delete operations in log are replayed correctly" do
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

      # Stop gracefully, then remove snapshot to force log replay
      stop_app()
      File.rm!(snapshot_path())

      start_app()

      # After replay, delete should still be reflected
      {:ok, stats_recovered} = TimelessLogs.Index.stats()
      assert stats_recovered.total_blocks == 1
      assert stats_recovered.total_entries == 10
    end

    test "compact operations in log are replayed correctly" do
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

      # Stop gracefully, then remove snapshot to force log replay
      stop_app()
      File.rm!(snapshot_path())

      start_app()

      {:ok, stats_after} = TimelessLogs.Index.stats()
      assert stats_after.total_blocks == 1
      assert stats_after.total_entries == 20
      assert stats_after.compaction_count == stats_before.compaction_count
    end
  end

  # ----------------------------------------------------------------
  # Snapshot + log interaction
  # ----------------------------------------------------------------

  describe "snapshot + log interaction" do
    test "entries written after snapshot are recovered from log" do
      start_app()

      # Write first batch
      entries1 = Enum.map(1..10, fn i -> make_entry(%{message: "pre-snapshot-#{i}"}) end)
      ingest_entries(entries1)
      TimelessLogs.Index.sync()

      # Graceful stop writes snapshot (batch 1 only)
      stop_app()

      # Save a copy of the snapshot (contains only batch 1)
      old_snapshot = File.read!(snapshot_path())

      # Restart and write second batch
      start_app()
      entries2 = Enum.map(1..10, fn i -> make_entry(%{message: "post-snapshot-#{i}"}) end)
      ingest_entries(entries2)
      TimelessLogs.Index.sync()

      {:ok, stats_running} = TimelessLogs.Index.stats()
      assert stats_running.total_entries == 20

      # Graceful stop writes new snapshot (both batches), log has all entries
      stop_app()

      # Restore the OLD snapshot (only batch 1) — log still has batch 2 entries
      File.write!(snapshot_path(), old_snapshot)

      # Restart: loads old snapshot (batch 1) + replays log (batch 2 entries are newer)
      start_app()

      {:ok, stats_recovered} = TimelessLogs.Index.stats()
      assert stats_recovered.total_entries == 20
      assert stats_recovered.total_blocks == 2
    end

    test "log entries already in snapshot are not double-applied" do
      start_app()

      entries = Enum.map(1..10, fn i -> make_entry(%{message: "idempotent-#{i}"}) end)
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      # Graceful stop writes snapshot (log still has entries)
      stop_app()

      # Do NOT delete the log — it still has entries from before the snapshot
      # On restart, replay should skip them (ts <= snapshot_ts)
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_entries == 10
      assert stats.total_blocks == 1
    end

    test "mixed operations in log replay: index + delete + index" do
      start_app()

      # Index old entries
      old_entries =
        Enum.map(1..5, fn i ->
          make_entry(%{message: "old-#{i}", timestamp: System.system_time(:second) - 100_000 + i})
        end)

      ingest_entries(old_entries)
      TimelessLogs.Index.sync()

      # Graceful stop writes snapshot with the old entries
      stop_app()

      # Save snapshot with only old entries
      old_snapshot = File.read!(snapshot_path())

      start_app()

      # Delete old entries (logged to disk_log)
      TimelessLogs.Index.delete_blocks_before(System.system_time(:second) - 50_000)
      TimelessLogs.Index.sync()

      # Index new entries (also logged)
      new_entries =
        Enum.map(1..5, fn i ->
          make_entry(%{message: "new-#{i}", timestamp: System.system_time(:second) + i})
        end)

      ingest_entries(new_entries)
      TimelessLogs.Index.sync()

      {:ok, stats_before} = TimelessLogs.Index.stats()
      assert stats_before.total_blocks == 1
      assert stats_before.total_entries == 5

      # Stop gracefully, restore old snapshot to force log replay of delete + new index
      stop_app()
      File.write!(snapshot_path(), old_snapshot)

      start_app()

      {:ok, stats_after} = TimelessLogs.Index.stats()
      assert stats_after.total_blocks == 1
      assert stats_after.total_entries == 5
    end
  end

  # ----------------------------------------------------------------
  # Snapshot threshold (auto-snapshot after N log entries)
  # ----------------------------------------------------------------

  describe "snapshot threshold" do
    test "snapshot is triggered after threshold log entries" do
      start_app()

      # The threshold is 1000. Write enough blocks to exceed it.
      # Each index_block call = 1 log entry. Write 1100 blocks.
      for _i <- 1..1100 do
        entry = make_entry()
        ingest_entries([entry])
      end

      TimelessLogs.Index.sync()

      # Snapshot should have been written at entry 1000
      assert File.exists?(snapshot_path())

      # Graceful restart should recover all data (snapshot + log replay)
      stop_app()
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_blocks == 1100
      assert stats.total_entries == 1100
    end

    test "log is truncated after auto-snapshot" do
      start_app()

      # Write 1100 blocks: auto-snapshot fires at 1000, truncates log
      for _i <- 1..1100 do
        ingest_entries([make_entry()])
      end

      TimelessLogs.Index.sync()

      # Stop, delete snapshot — only post-truncation log entries survive
      stop_app()
      File.rm!(snapshot_path())

      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      # Only the 100 entries written after the auto-snapshot truncation are in the log
      assert stats.total_blocks == 100
      assert stats.total_entries == 100
    end
  end

  # ----------------------------------------------------------------
  # Corrupt / missing file handling
  # ----------------------------------------------------------------

  describe "corrupt and missing file handling" do
    test "missing snapshot file starts with empty state" do
      # No snapshot, no log — fresh start
      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_blocks == 0
      assert stats.total_entries == 0

      stop_app()
    end

    test "corrupt snapshot file recovers from log" do
      start_app()

      entries = Enum.map(1..10, fn i -> make_entry(%{message: "corrupt-test-#{i}"}) end)
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      stop_app()

      # Corrupt the snapshot
      File.write!(snapshot_path(), "this is not valid erlang term binary")

      # Restart — should handle corrupt snapshot gracefully and replay from log
      start_app()

      # Log has all entries (not truncated on graceful stop), so data should be recovered
      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 10

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

    test "partial snapshot (.tmp file) does not corrupt state" do
      start_app()

      entries = Enum.map(1..10, fn i -> make_entry(%{message: "tmp-test-#{i}"}) end)
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      stop_app()

      # Simulate a crash during snapshot write: .tmp exists but real snapshot is good
      tmp_path = snapshot_path() <> ".tmp"
      File.write!(tmp_path, "partial garbage data")

      start_app()

      {:ok, stats} = TimelessLogs.Index.stats()
      assert stats.total_entries == 10

      # .tmp should not interfere
      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Backup
  # ----------------------------------------------------------------

  describe "backup" do
    test "backup creates a valid snapshot at target path" do
      start_app()

      entries = Enum.map(1..20, fn i -> make_entry(%{message: "backup-#{i}"}) end)
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      backup_path = Path.join(@data_dir, "backup.snapshot")
      assert :ok == TimelessLogs.Index.backup(backup_path)
      assert File.exists?(backup_path)

      # Verify it's valid by loading it
      binary = File.read!(backup_path)
      snapshot = :erlang.binary_to_term(binary)
      assert snapshot.version == 1
      assert length(snapshot.blocks) == 1
      assert length(snapshot.term_index) > 0

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Memory mode
  # ----------------------------------------------------------------

  describe "memory mode" do
    test "memory mode skips log and snapshot" do
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

      # No files should be created
      refute File.exists?("test/tmp/mem_persist_test/index.log")
      refute File.exists?("test/tmp/mem_persist_test/index.snapshot")

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Async indexing (cast path) recovery
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
  # Stats (index_size) reflects new file format
  # ----------------------------------------------------------------

  describe "stats reflect persistence files" do
    test "index_size reports snapshot + log size" do
      start_app()

      entries = Enum.map(1..50, fn i -> make_entry(%{message: "stats-#{i}"}) end)
      ingest_entries(entries)
      TimelessLogs.Index.sync()

      {:ok, stats} = TimelessLogs.Index.stats()
      # Log should have some data (entries were logged)
      assert stats.index_size > 0

      stop_app()
    end
  end
end
