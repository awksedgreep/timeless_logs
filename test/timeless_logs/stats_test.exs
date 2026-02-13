defmodule TimelessLogs.StatsTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/stats"

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :retention_max_age, nil)
    Application.put_env(:timeless_logs, :retention_max_size, nil)
    Application.ensure_all_started(:timeless_logs)

    on_exit(fn ->
      Application.stop(:timeless_logs)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "stats/0 returns zeros on empty state" do
    {:ok, stats} = TimelessLogs.stats()
    assert stats.total_blocks == 0
    assert stats.total_entries == 0
    assert stats.total_bytes == 0
    assert stats.oldest_timestamp == nil
    assert stats.newest_timestamp == nil
  end

  test "stats/0 reflects written blocks" do
    Logger.info("entry one")
    Logger.info("entry two")
    TimelessLogs.flush()

    {:ok, stats} = TimelessLogs.stats()
    assert stats.total_blocks == 1
    assert stats.total_entries == 2
    assert stats.total_bytes > 0
    assert stats.disk_size > 0
    assert stats.oldest_timestamp != nil
    assert stats.newest_timestamp != nil
    assert stats.index_size > 0
  end

  test "stats/0 across multiple blocks" do
    Logger.info("block 1")
    TimelessLogs.flush()
    Logger.info("block 2")
    TimelessLogs.flush()
    Logger.info("block 3")
    TimelessLogs.flush()

    {:ok, stats} = TimelessLogs.stats()
    assert stats.total_blocks == 3
    assert stats.total_entries == 3
  end

  test "stats/0 returns correct struct type" do
    {:ok, stats} = TimelessLogs.stats()
    assert %TimelessLogs.Stats{} = stats
  end
end
