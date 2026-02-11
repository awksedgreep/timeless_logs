defmodule LogStreamTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/log_stream"

  setup do
    # Stop app so we can reconfigure with test data dir
    Application.stop(:log_stream)
    File.rm_rf!(@data_dir)
    Application.put_env(:log_stream, :data_dir, @data_dir)
    Application.put_env(:log_stream, :flush_interval, 60_000)
    Application.put_env(:log_stream, :max_buffer_size, 10_000)
    Application.ensure_all_started(:log_stream)

    on_exit(fn ->
      Application.stop(:log_stream)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  describe "full pipeline" do
    test "logs are compressed, indexed, and queryable" do
      Logger.info("test message one", domain: [:test])
      Logger.error("something broke", request_id: "req-123")
      Logger.warning("disk almost full", service: "storage")

      LogStream.flush()

      {:ok, %LogStream.Result{entries: errors, total: 1}} =
        LogStream.query(level: :error)

      assert length(errors) == 1
      assert hd(errors).message =~ "something broke"
      assert %LogStream.Entry{} = hd(errors)

      {:ok, %LogStream.Result{entries: all, total: 3}} =
        LogStream.query([])

      assert length(all) == 3
    end

    test "metadata filtering works" do
      Logger.error("timeout", service: "api", request_id: "abc")
      Logger.error("timeout", service: "web", request_id: "def")

      LogStream.flush()

      {:ok, %LogStream.Result{entries: results}} =
        LogStream.query(level: :error, metadata: %{service: "api"})

      assert length(results) == 1
      assert hd(results).metadata["request_id"] == "abc"
    end

    test "message substring search" do
      Logger.info("user logged in successfully")
      Logger.info("user logged out")
      Logger.info("database connection established")

      LogStream.flush()

      {:ok, %LogStream.Result{entries: results, total: 2}} =
        LogStream.query(message: "logged")

      assert length(results) == 2
    end

    test "blocks are initially written as raw" do
      for i <- 1..10 do
        Logger.info("log entry number #{i}", iteration: "#{i}")
      end

      LogStream.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_files) >= 1

      # Raw blocks use term_to_binary without zstd compression
      file = hd(raw_files)
      data = File.read!(file)
      assert {:ok, entries} = LogStream.Writer.decompress_block(data, :raw)
      assert length(entries) >= 1
    end

    test "time range filtering" do
      now = System.system_time(:second)

      Logger.info("recent log")
      LogStream.flush()

      {:ok, %LogStream.Result{entries: results}} =
        LogStream.query(since: now - 10)

      assert length(results) >= 1

      {:ok, %LogStream.Result{entries: []}} =
        LogStream.query(since: now + 3600)
    end

    test "pagination with limit and offset" do
      for i <- 1..20 do
        Logger.info("entry #{String.pad_leading(Integer.to_string(i), 2, "0")}")
      end

      LogStream.flush()

      {:ok, %LogStream.Result{entries: page1, total: 20, limit: 5, offset: 0}} =
        LogStream.query(limit: 5)

      assert length(page1) == 5

      {:ok, %LogStream.Result{entries: page2, total: 20, offset: 5}} =
        LogStream.query(limit: 5, offset: 5)

      assert length(page2) == 5
      assert hd(page1).timestamp != hd(page2).timestamp || hd(page1).message != hd(page2).message
    end

    test "ordering asc and desc" do
      Logger.info("first")
      Process.sleep(1100)
      Logger.info("second")
      LogStream.flush()

      {:ok, %LogStream.Result{entries: desc}} =
        LogStream.query(order: :desc)

      {:ok, %LogStream.Result{entries: asc}} =
        LogStream.query(order: :asc)

      assert hd(desc).timestamp >= hd(asc).timestamp
    end
  end
end
