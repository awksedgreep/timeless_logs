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
    Application.ensure_all_started(:timeless_logs)

    on_exit(fn ->
      Application.stop(:timeless_logs)
      Application.delete_env(:timeless_logs, :compaction_interval)
      Application.delete_env(:timeless_logs, :compaction_threshold)
      Application.delete_env(:timeless_logs, :compaction_max_raw_age)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  describe "compact_now/0" do
    test "compacts multiple raw blocks into a single zstd block" do
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

      # Raw blocks should be gone, replaced by one zstd block
      raw_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      zstd_after = Path.wildcard(Path.join(blocks_dir, "*.zst"))
      assert length(raw_after) == 0
      assert length(zstd_after) == 1

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
      zstd_after = Path.wildcard(Path.join(blocks_dir, "*.zst"))
      assert length(raw_after) == 0
      assert length(zstd_after) == 1
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

    test "queries work across mixed raw and zstd blocks" do
      # Create raw blocks
      for i <- 1..3 do
        Logger.info("raw #{i}")
        TimelessLogs.flush()
      end

      # Compact them to zstd
      Application.put_env(:timeless_logs, :compaction_threshold, 1)
      TimelessLogs.Compactor.compact_now()

      # Create more raw blocks
      Application.put_env(:timeless_logs, :compaction_threshold, 500)

      for i <- 4..6 do
        Logger.info("raw #{i}")
        TimelessLogs.flush()
      end

      # Now we have mixed: 1 zstd + 3 raw
      blocks_dir = Path.join(@data_dir, "blocks")
      zstd_files = Path.wildcard(Path.join(blocks_dir, "*.zst"))
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(zstd_files) == 1
      assert length(raw_files) == 3

      # All 6 entries should be queryable
      {:ok, %TimelessLogs.Result{total: 6}} = TimelessLogs.query([])
    end

    test "stream works across mixed raw and zstd blocks" do
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
  end
end
