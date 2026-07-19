defmodule TimelessLogs.HotTailTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/hot_tail"

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :retention_max_age, nil)
    Application.put_env(:timeless_logs, :retention_max_size, nil)
    Application.put_env(:timeless_logs, :ingest_shard_count, 1)
    Application.ensure_all_started(:timeless_logs)

    on_exit(fn ->
      Application.stop(:timeless_logs)
      Application.delete_env(:timeless_logs, :ingest_shard_count)
      Application.delete_env(:timeless_logs, :hot_tail_max_entries)
      Application.delete_env(:timeless_logs, :hot_tail_lag_ms)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp entries(n, msg) do
    for i <- 1..n do
      %{
        timestamp: System.os_time(:microsecond),
        level: :info,
        message: "#{msg} #{i}",
        metadata: %{"service" => "tail"}
      }
    end
  end

  test "entries are queryable before any flush" do
    TimelessLogs.ingest(entries(50, "unflushed"))

    # No flush: entries live only in buffers + hot tail
    {:ok, %{entries: results, total: total}} =
      TimelessLogs.query(metadata: %{"service" => "tail"}, count_total: true)

    assert total == 50
    assert length(results) == 50

    {:ok, count} = TimelessLogs.count(metadata: %{"service" => "tail"})
    assert count == 50
  end

  test "no duplicates once entries are flushed and indexed" do
    TimelessLogs.ingest(entries(80, "flushed"))
    :ok = TimelessLogs.flush()
    TimelessLogs.Index.sync()

    # Entries are now on disk AND still inside the tail window; the
    # boundary partition must serve each exactly once.
    {:ok, %{total: total}} =
      TimelessLogs.query(metadata: %{"service" => "tail"}, count_total: true)

    assert total == 80

    {:ok, count} = TimelessLogs.count(metadata: %{"service" => "tail"})
    assert count == 80
  end

  test "pagination and order hold across the tail/disk boundary" do
    # Old entries: flushed, timestamps pushed behind the boundary lag
    old_ts = System.os_time(:microsecond) - 60_000_000

    old =
      for i <- 1..30 do
        %{
          timestamp: old_ts + i,
          level: :info,
          message: "old #{i}",
          metadata: %{"service" => "tail"}
        }
      end

    TimelessLogs.ingest(old)
    :ok = TimelessLogs.flush()
    TimelessLogs.Index.sync()

    # Fresh entries stay tail-side
    TimelessLogs.ingest(entries(30, "fresh"))

    {:ok, %{entries: page1, total: total}} =
      TimelessLogs.query(metadata: %{"service" => "tail"}, limit: 40, count_total: true)

    assert total == 60
    assert length(page1) == 40

    # Descending: all 30 fresh first, then the newest 10 old
    assert Enum.take(page1, 30) |> Enum.all?(&String.starts_with?(&1.message, "fresh"))
    assert Enum.drop(page1, 30) |> Enum.all?(&String.starts_with?(&1.message, "old"))
    timestamps = Enum.map(page1, & &1.timestamp)
    assert timestamps == Enum.sort(timestamps, :desc)

    {:ok, %{entries: page2}} =
      TimelessLogs.query(metadata: %{"service" => "tail"}, limit: 40, offset: 40)

    assert length(page2) == 20
    assert Enum.all?(page2, &String.starts_with?(&1.message, "old"))
  end

  test "size cap prunes oldest tail entries" do
    Application.put_env(:timeless_logs, :hot_tail_max_entries, 100)
    TimelessLogs.ingest(entries(500, "capped"))

    # Sweep runs every second
    Process.sleep(1_500)

    size = :ets.info(TimelessLogs.HotTail, :size)
    assert size <= 100
  end

  test "retention purge removes tail entries too" do
    TimelessLogs.ingest(entries(20, "doomed"))
    :ok = TimelessLogs.flush()
    TimelessLogs.Index.sync()

    Application.put_env(:timeless_logs, :retention_max_age, 0)
    Process.sleep(1_100)
    {:ok, deleted} = TimelessLogs.Retention.run_now()
    assert deleted >= 1

    {:ok, %{total: total}} =
      TimelessLogs.query(metadata: %{"service" => "tail"}, count_total: true)

    assert total == 0
  after
    Application.put_env(:timeless_logs, :retention_max_age, nil)
  end

  test "disabled hot tail keeps pre-flush entries invisible (old behavior)" do
    Application.stop(:timeless_logs)
    Application.put_env(:timeless_logs, :hot_tail, false)
    Application.ensure_all_started(:timeless_logs)

    TimelessLogs.ingest(entries(10, "invisible"))

    {:ok, %{total: total}} =
      TimelessLogs.query(metadata: %{"service" => "tail"}, count_total: true)

    assert total == 0

    :ok = TimelessLogs.flush()
    TimelessLogs.Index.sync()

    {:ok, %{total: total_after}} =
      TimelessLogs.query(metadata: %{"service" => "tail"}, count_total: true)

    assert total_after == 10
  after
    Application.delete_env(:timeless_logs, :hot_tail)
  end
end
