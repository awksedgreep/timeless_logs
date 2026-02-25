defmodule TimelessLogs.CompactorTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/compactor"

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :retention_max_age, nil)
    Application.put_env(:timeless_logs, :retention_max_size, nil)
    # Prevent auto-compaction during tests
    Application.put_env(:timeless_logs, :compaction_interval, 600_000)
    Application.put_env(:timeless_logs, :compaction_threshold, 500)
    Application.put_env(:timeless_logs, :compaction_max_raw_age, 3600)
    Application.delete_env(:timeless_logs, :compaction_format)
    Application.ensure_all_started(:timeless_logs)

    on_exit(fn ->
      Application.stop(:timeless_logs)
      Application.delete_env(:timeless_logs, :compaction_interval)
      Application.delete_env(:timeless_logs, :compaction_threshold)
      Application.delete_env(:timeless_logs, :compaction_max_raw_age)
      Application.delete_env(:timeless_logs, :compaction_format)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  describe "compact_now/0" do
    test "compacts multiple raw blocks into openzl blocks (default format)" do
      # Create 3 separate raw blocks
      Logger.info("block one entry")
      TimelessLogs.flush()

      Logger.info("block two entry")
      TimelessLogs.flush()

      Logger.info("block three entry")
      TimelessLogs.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_before = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_before) == 3

      # Lower threshold to trigger compaction
      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      assert :ok = TimelessLogs.Compactor.compact_now()

      # Raw blocks should be gone, replaced by openzl block(s)
      raw_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      ozl_after = Path.wildcard(Path.join(blocks_dir, "*.ozl"))
      assert length(raw_after) == 0
      assert length(ozl_after) >= 1

      # All entries should still be queryable
      {:ok, %TimelessLogs.Result{total: 3}} = TimelessLogs.query([])
    end

    test "returns :noop when below threshold and not old enough" do
      Logger.info("single entry")
      TimelessLogs.flush()

      assert :noop = TimelessLogs.Compactor.compact_now()
    end

    test "compacts on age even when below threshold" do
      Logger.info("old entry")
      TimelessLogs.flush()

      # Set max age to 0 so everything is "old"
      Application.put_env(:timeless_logs, :compaction_max_raw_age, 0)
      Process.sleep(1100)

      assert :ok = TimelessLogs.Compactor.compact_now()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      ozl_after = Path.wildcard(Path.join(blocks_dir, "*.ozl"))
      assert length(raw_after) == 0
      assert length(ozl_after) == 1
    end

    test "preserves entry order after compaction" do
      for i <- 1..5 do
        Logger.info("entry #{i}")
        TimelessLogs.flush()
      end

      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      TimelessLogs.Compactor.compact_now()

      {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query(order: :asc)
      messages = Enum.map(entries, & &1.message)

      for i <- 1..5 do
        assert Enum.any?(messages, &(&1 =~ "entry #{i}"))
      end
    end

    test "queries work across mixed raw and openzl blocks" do
      # Create raw blocks
      for i <- 1..3 do
        Logger.info("raw #{i}")
        TimelessLogs.flush()
      end

      # Compact them to openzl
      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      TimelessLogs.Compactor.compact_now()

      # Create more raw blocks
      Application.put_env(:timeless_logs, :compaction_threshold, 500)

      for i <- 4..6 do
        Logger.info("raw #{i}")
        TimelessLogs.flush()
      end

      # Now we have mixed: openzl + 3 raw
      blocks_dir = Path.join(@data_dir, "blocks")
      ozl_files = Path.wildcard(Path.join(blocks_dir, "*.ozl"))
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(ozl_files) >= 1
      assert length(raw_files) == 3

      # All 6 entries should be queryable
      {:ok, %TimelessLogs.Result{total: 6}} = TimelessLogs.query([])
    end

    test "stream works across mixed raw and openzl blocks" do
      # Create and compact some blocks
      for i <- 1..3 do
        Logger.info("stream #{i}")
        TimelessLogs.flush()
      end

      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      TimelessLogs.Compactor.compact_now()

      # Create more raw blocks
      Application.put_env(:timeless_logs, :compaction_threshold, 500)

      for i <- 4..5 do
        Logger.info("stream #{i}")
        TimelessLogs.flush()
      end

      # Stream should return all entries
      entries = TimelessLogs.stream() |> Enum.to_list()
      assert length(entries) == 5
    end

    test "compacts to zstd when compaction_format is overridden" do
      Logger.info("zstd override entry")
      TimelessLogs.flush()

      Application.put_env(:timeless_logs, :compaction_format, :zstd)
      Application.put_env(:timeless_logs, :compaction_max_raw_age, 0)
      Process.sleep(1100)

      assert :ok = TimelessLogs.Compactor.compact_now()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      zstd_after = Path.wildcard(Path.join(blocks_dir, "*.zst"))
      ozl_after = Path.wildcard(Path.join(blocks_dir, "*.ozl"))
      assert length(raw_after) == 0
      assert length(zstd_after) == 1
      assert length(ozl_after) == 0

      # Entry should still be queryable
      {:ok, %TimelessLogs.Result{total: 1}} = TimelessLogs.query([])
    end
  end

  describe "merge compaction" do
    test "merges multiple small compressed blocks into fewer larger ones" do
      # Create 10 separate raw blocks with 3 entries each
      for i <- 1..10 do
        Logger.info("merge test entry #{i}")
        TimelessLogs.flush()
      end

      # Compact them into 10 small compressed blocks
      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      assert :ok = TimelessLogs.Compactor.compact_now()

      {:ok, stats_before} = TimelessLogs.stats()
      assert stats_before.raw_blocks == 0
      assert stats_before.openzl_blocks >= 1

      blocks_before = stats_before.openzl_blocks

      # Set merge config: target 50 entries, min 2 blocks
      Application.put_env(:timeless_logs, :merge_compaction_target_size, 50)
      Application.put_env(:timeless_logs, :merge_compaction_min_blocks, 2)

      assert :ok = TimelessLogs.Compactor.merge_now()

      {:ok, stats_after} = TimelessLogs.stats()
      # Should have fewer blocks after merge
      assert stats_after.total_blocks < blocks_before
      # All entries still present
      assert stats_after.total_entries == 10

      # All entries still queryable
      {:ok, %TimelessLogs.Result{total: 10}} = TimelessLogs.query([])
    end

    test "returns :noop when not enough small blocks" do
      Logger.info("single entry")
      TimelessLogs.flush()

      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      TimelessLogs.Compactor.compact_now()

      # Only 1 compressed block, min_blocks is 4
      Application.put_env(:timeless_logs, :merge_compaction_min_blocks, 4)
      Application.put_env(:timeless_logs, :merge_compaction_target_size, 50)

      assert :noop = TimelessLogs.Compactor.merge_now()
    end

    test "preserves query/search after merge" do
      # Create entries with distinct metadata
      Logger.info("alpha", domain: [:app])
      TimelessLogs.flush()
      Logger.warning("beta", domain: [:web])
      TimelessLogs.flush()
      Logger.error("gamma", domain: [:db])
      TimelessLogs.flush()
      Logger.info("delta", domain: [:app])
      TimelessLogs.flush()

      # Compact raw → compressed
      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      assert :ok = TimelessLogs.Compactor.compact_now()

      # Merge compressed blocks
      Application.put_env(:timeless_logs, :merge_compaction_target_size, 50)
      Application.put_env(:timeless_logs, :merge_compaction_min_blocks, 2)
      assert :ok = TimelessLogs.Compactor.merge_now()

      # Query by level should still work
      {:ok, %TimelessLogs.Result{total: total_errors}} = TimelessLogs.query(level: :error)
      assert total_errors >= 1

      # All entries queryable
      {:ok, %TimelessLogs.Result{total: total}} = TimelessLogs.query([])
      assert total == 4
    end

    test "works in memory mode" do
      Application.stop(:timeless_logs)
      Application.put_env(:timeless_logs, :storage, :memory)
      Application.ensure_all_started(:timeless_logs)

      for i <- 1..8 do
        Logger.info("memory merge #{i}")
        TimelessLogs.flush()
      end

      # Compact raw → compressed
      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      assert :ok = TimelessLogs.Compactor.compact_now()

      {:ok, stats_before} = TimelessLogs.stats()
      assert stats_before.raw_blocks == 0
      blocks_before = stats_before.total_blocks

      # Merge compressed blocks
      Application.put_env(:timeless_logs, :merge_compaction_target_size, 50)
      Application.put_env(:timeless_logs, :merge_compaction_min_blocks, 2)
      assert :ok = TimelessLogs.Compactor.merge_now()

      {:ok, stats_after} = TimelessLogs.stats()
      assert stats_after.total_blocks < blocks_before
      assert stats_after.total_entries == 8

      {:ok, %TimelessLogs.Result{total: 8}} = TimelessLogs.query([])
    after
      Application.stop(:timeless_logs)
      Application.put_env(:timeless_logs, :storage, :disk)
      Application.ensure_all_started(:timeless_logs)
    end
  end
end
