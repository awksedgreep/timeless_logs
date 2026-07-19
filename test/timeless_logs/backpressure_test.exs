defmodule TimelessLogs.BackpressureTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/backpressure"

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
      Application.delete_env(:timeless_logs, :ingest_soft_watermark)
      Application.delete_env(:timeless_logs, :ingest_backpressure_timeout)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp entries(n, prefix) do
    for i <- 1..n do
      %{
        timestamp: System.os_time(:microsecond),
        level: :info,
        message: "#{prefix} #{i}",
        metadata: %{"service" => "bp"}
      }
    end
  end

  test "queued gauge tracks buffer depth and drains on flush" do
    assert TimelessLogs.IngestPressure.queued(0) == 0

    TimelessLogs.ingest(entries(100, "gauge"))
    # cast is async; wait for the shard to process it
    wait_until(fn -> TimelessLogs.IngestPressure.queued(0) == 100 end)

    :ok = TimelessLogs.flush()
    wait_until(fn -> TimelessLogs.IngestPressure.queued(0) == 0 end)

    {:ok, %{total: total}} = TimelessLogs.query(count_total: true, limit: 1)
    assert total == 100
  end

  test "ingest above the watermark paces the producer, loses nothing" do
    Application.put_env(:timeless_logs, :ingest_soft_watermark, 50)
    # Nothing drains until the explicit flush below, so cap the wait
    Application.put_env(:timeless_logs, :ingest_backpressure_timeout, 200)

    handler_id = {:bp_test, make_ref()}
    parent = self()

    :telemetry.attach(
      handler_id,
      [:timeless_logs, :ingest, :backpressure],
      fn _event, measurements, meta, _cfg ->
        send(parent, {:backpressure, measurements.entry_count, meta.shard})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    # First batch goes in via cast and pushes the gauge past the watermark
    TimelessLogs.ingest(entries(100, "first"))
    wait_until(fn -> TimelessLogs.IngestPressure.queued(0) >= 50 end)

    # Second batch must take the synchronous path and still be accepted
    TimelessLogs.ingest(entries(100, "second"))
    assert_receive {:backpressure, 100, 0}, 2_000

    :ok = TimelessLogs.flush()
    {:ok, total} = TimelessLogs.count(metadata: %{"service" => "bp"})
    assert total == 200
  end

  test "raw debt gauge participates in overload" do
    Application.put_env(:timeless_logs, :ingest_soft_watermark, 1_000_000)
    refute TimelessLogs.IngestPressure.overloaded?(0)

    TimelessLogs.IngestPressure.set_raw_debt(3_000_000_000)
    assert TimelessLogs.IngestPressure.overloaded?(0)

    TimelessLogs.IngestPressure.set_raw_debt(0)
    refute TimelessLogs.IngestPressure.overloaded?(0)
  end

  defp wait_until(fun, attempts \\ 200) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition never became true")

      true ->
        Process.sleep(10)
        wait_until(fun, attempts - 1)
    end
  end
end
