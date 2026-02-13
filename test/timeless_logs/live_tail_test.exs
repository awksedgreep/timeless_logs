defmodule TimelessLogs.LiveTailTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/live_tail"

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
      TimelessLogs.unsubscribe()
      Application.stop(:timeless_logs)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "subscribe receives new log entries" do
    TimelessLogs.subscribe()
    Logger.info("live tail test")
    assert_receive {:timeless_logs, :entry, %TimelessLogs.Entry{message: msg}}, 1000
    assert msg =~ "live tail test"
  end

  test "subscribe with level filter only gets matching entries" do
    TimelessLogs.subscribe(level: :error)
    Logger.info("should not receive this")
    Logger.error("should receive this")

    assert_receive {:timeless_logs, :entry, %TimelessLogs.Entry{level: :error}}, 1000
    refute_receive {:timeless_logs, :entry, %TimelessLogs.Entry{level: :info}}, 200
  end

  test "unsubscribe stops messages" do
    TimelessLogs.subscribe()
    TimelessLogs.unsubscribe()
    Logger.info("should not arrive")
    refute_receive {:timeless_logs, :entry, _}, 200
  end

  test "multiple subscribers each receive entries" do
    parent = self()

    spawn(fn ->
      TimelessLogs.subscribe()

      receive do
        {:timeless_logs, :entry, entry} -> send(parent, {:child1, entry})
      end
    end)

    spawn(fn ->
      TimelessLogs.subscribe()

      receive do
        {:timeless_logs, :entry, entry} -> send(parent, {:child2, entry})
      end
    end)

    Process.sleep(50)
    Logger.info("broadcast test")

    assert_receive {:child1, %TimelessLogs.Entry{}}, 1000
    assert_receive {:child2, %TimelessLogs.Entry{}}, 1000
  end

  test "subscribe with metadata filter" do
    TimelessLogs.subscribe(metadata: %{service: "api"})
    Logger.info("wrong service", service: "web")
    Logger.info("right service", service: "api")

    assert_receive {:timeless_logs, :entry, %TimelessLogs.Entry{} = entry}, 1000
    assert entry.metadata["service"] == "api"
    refute_receive {:timeless_logs, :entry, _}, 200
  end
end
