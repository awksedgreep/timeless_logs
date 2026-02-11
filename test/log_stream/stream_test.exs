defmodule LogStream.StreamTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/stream"

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

  test "stream/1 lazily yields entries" do
    for i <- 1..20 do
      Logger.info("stream entry #{i}")
    end

    LogStream.flush()

    entries = LogStream.stream([]) |> Enum.to_list()
    assert length(entries) == 20
    assert %LogStream.Entry{} = hd(entries)
  end

  test "stream/1 with level filter" do
    Logger.info("good")
    Logger.error("bad")
    LogStream.flush()

    errors = LogStream.stream(level: :error) |> Enum.to_list()
    assert length(errors) == 1
    assert hd(errors).level == :error
  end

  test "stream/1 with Enum.take limits consumption" do
    for i <- 1..50 do
      Logger.info("entry #{i}")
    end

    LogStream.flush()

    first_five = LogStream.stream([]) |> Enum.take(5)
    assert length(first_five) == 5
  end

  test "stream/1 across multiple blocks" do
    Logger.info("block 1")
    LogStream.flush()
    Logger.info("block 2")
    LogStream.flush()
    Logger.info("block 3")
    LogStream.flush()

    entries = LogStream.stream([]) |> Enum.to_list()
    assert length(entries) == 3
  end

  test "stream/1 with message filter" do
    Logger.info("timeout occurred")
    Logger.info("all good")
    LogStream.flush()

    results = LogStream.stream(message: "timeout") |> Enum.to_list()
    assert length(results) == 1
    assert hd(results).message =~ "timeout"
  end

  test "stream/1 with metadata filter" do
    Logger.info("api call", service: "api")
    Logger.info("web call", service: "web")
    LogStream.flush()

    results = LogStream.stream(metadata: %{service: "api"}) |> Enum.to_list()
    assert length(results) == 1
    assert hd(results).metadata["service"] == "api"
  end

  test "stream/1 on empty state returns empty" do
    entries = LogStream.stream([]) |> Enum.to_list()
    assert entries == []
  end
end
