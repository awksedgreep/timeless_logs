defmodule LogStream.LiveTailTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/live_tail"

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
      LogStream.unsubscribe()
      Application.stop(:log_stream)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "subscribe receives new log entries" do
    LogStream.subscribe()
    Logger.info("live tail test")
    assert_receive {:log_stream, :entry, %LogStream.Entry{message: msg}}, 1000
    assert msg =~ "live tail test"
  end

  test "subscribe with level filter only gets matching entries" do
    LogStream.subscribe(level: :error)
    Logger.info("should not receive this")
    Logger.error("should receive this")

    assert_receive {:log_stream, :entry, %LogStream.Entry{level: :error}}, 1000
    refute_receive {:log_stream, :entry, %LogStream.Entry{level: :info}}, 200
  end

  test "unsubscribe stops messages" do
    LogStream.subscribe()
    LogStream.unsubscribe()
    Logger.info("should not arrive")
    refute_receive {:log_stream, :entry, _}, 200
  end

  test "multiple subscribers each receive entries" do
    parent = self()

    spawn(fn ->
      LogStream.subscribe()

      receive do
        {:log_stream, :entry, entry} -> send(parent, {:child1, entry})
      end
    end)

    spawn(fn ->
      LogStream.subscribe()

      receive do
        {:log_stream, :entry, entry} -> send(parent, {:child2, entry})
      end
    end)

    Process.sleep(50)
    Logger.info("broadcast test")

    assert_receive {:child1, %LogStream.Entry{}}, 1000
    assert_receive {:child2, %LogStream.Entry{}}, 1000
  end

  test "subscribe with metadata filter" do
    LogStream.subscribe(metadata: %{service: "api"})
    Logger.info("wrong service", service: "web")
    Logger.info("right service", service: "api")

    assert_receive {:log_stream, :entry, %LogStream.Entry{} = entry}, 1000
    assert entry.metadata["service"] == "api"
    refute_receive {:log_stream, :entry, _}, 200
  end
end
