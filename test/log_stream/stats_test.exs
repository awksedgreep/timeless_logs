defmodule LogStream.StatsTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/stats"

  setup do
    Application.stop(:log_stream)
    File.rm_rf!(@data_dir)
    Application.put_env(:log_stream, :data_dir, @data_dir)
    Application.put_env(:log_stream, :flush_interval, 60_000)
    Application.put_env(:log_stream, :max_buffer_size, 10_000)
    Application.put_env(:log_stream, :retention_max_age, nil)
    Application.put_env(:log_stream, :retention_max_size, nil)
    Application.ensure_all_started(:log_stream)

    on_exit(fn ->
      Application.stop(:log_stream)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "stats/0 returns zeros on empty state" do
    {:ok, stats} = LogStream.stats()
    assert stats.total_blocks == 0
    assert stats.total_entries == 0
    assert stats.total_bytes == 0
    assert stats.oldest_timestamp == nil
    assert stats.newest_timestamp == nil
  end

  test "stats/0 reflects written blocks" do
    Logger.info("entry one")
    Logger.info("entry two")
    LogStream.flush()

    {:ok, stats} = LogStream.stats()
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
    LogStream.flush()
    Logger.info("block 2")
    LogStream.flush()
    Logger.info("block 3")
    LogStream.flush()

    {:ok, stats} = LogStream.stats()
    assert stats.total_blocks == 3
    assert stats.total_entries == 3
  end

  test "stats/0 returns correct struct type" do
    {:ok, stats} = LogStream.stats()
    assert %LogStream.Stats{} = stats
  end
end
