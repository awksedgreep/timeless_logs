defmodule LogStream.MemoryTest do
  use ExUnit.Case, async: false

  require Logger

  setup do
    Application.stop(:log_stream)
    Application.put_env(:log_stream, :storage, :memory)
    Application.put_env(:log_stream, :data_dir, "test/tmp/memory_should_not_exist")
    Application.put_env(:log_stream, :flush_interval, 60_000)
    Application.put_env(:log_stream, :max_buffer_size, 10_000)
    Application.put_env(:log_stream, :retention_max_age, nil)
    Application.put_env(:log_stream, :retention_max_size, nil)
    Application.ensure_all_started(:log_stream)

    on_exit(fn ->
      Application.stop(:log_stream)
      Application.put_env(:log_stream, :storage, :disk)
    end)

    :ok
  end

  describe "basic pipeline" do
    test "log, flush, query works in memory mode" do
      Logger.info("memory test")
      LogStream.flush()

      {:ok, %LogStream.Result{entries: [entry], total: 1}} = LogStream.query([])
      assert entry.message =~ "memory test"
      assert %LogStream.Entry{} = entry
    end

    test "no files created on disk" do
      Logger.info("no disk")
      LogStream.flush()

      refute File.exists?("test/tmp/memory_should_not_exist")
    end

    test "multiple entries in one flush" do
      Logger.info("one")
      Logger.info("two")
      Logger.info("three")
      LogStream.flush()

      {:ok, %LogStream.Result{total: 3}} = LogStream.query([])
    end
  end

  describe "filtering" do
    test "level filter" do
      Logger.info("info msg")
      Logger.error("error msg")
      LogStream.flush()

      {:ok, %LogStream.Result{entries: entries}} = LogStream.query(level: :error)
      assert length(entries) == 1
      assert hd(entries).level == :error
    end

    test "message filter" do
      Logger.info("timeout occurred")
      Logger.info("all good")
      LogStream.flush()

      {:ok, %LogStream.Result{entries: entries}} = LogStream.query(message: "timeout")
      assert length(entries) == 1
      assert hd(entries).message =~ "timeout"
    end

    test "metadata filter" do
      Logger.info("api call", service: "api")
      Logger.info("web call", service: "web")
      LogStream.flush()

      {:ok, %LogStream.Result{entries: entries}} =
        LogStream.query(metadata: %{service: "api"})

      assert length(entries) == 1
      assert hd(entries).metadata["service"] == "api"
    end
  end

  describe "multiple blocks" do
    test "queries across multiple flushes" do
      Logger.info("block one")
      LogStream.flush()

      Logger.info("block two")
      LogStream.flush()

      Logger.info("block three")
      LogStream.flush()

      {:ok, %LogStream.Result{total: 3}} = LogStream.query([])
    end
  end

  describe "streaming" do
    test "stream/1 works in memory mode" do
      for i <- 1..10 do
        Logger.info("stream #{i}")
      end

      LogStream.flush()

      entries = LogStream.stream([]) |> Enum.to_list()
      assert length(entries) == 10
      assert %LogStream.Entry{} = hd(entries)
    end

    test "stream/1 with filters" do
      Logger.info("good")
      Logger.error("bad")
      LogStream.flush()

      errors = LogStream.stream(level: :error) |> Enum.to_list()
      assert length(errors) == 1
      assert hd(errors).level == :error
    end

    test "stream/1 across multiple blocks" do
      Logger.info("a")
      LogStream.flush()
      Logger.info("b")
      LogStream.flush()

      entries = LogStream.stream([]) |> Enum.to_list()
      assert length(entries) == 2
    end
  end

  describe "stats" do
    test "stats/0 works in memory mode" do
      {:ok, stats} = LogStream.stats()
      assert stats.total_blocks == 0
      assert stats.index_size == 0

      Logger.info("entry")
      LogStream.flush()

      {:ok, stats} = LogStream.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 1
      assert stats.total_bytes > 0
    end
  end

  describe "live tail" do
    test "subscribe works in memory mode" do
      LogStream.subscribe()
      Logger.info("live memory")

      assert_receive {:log_stream, :entry, %LogStream.Entry{message: msg}}, 1000
      assert msg =~ "live memory"
      LogStream.unsubscribe()
    end
  end

  describe "retention" do
    test "age-based retention in memory mode" do
      Logger.info("will be deleted")
      LogStream.flush()

      {:ok, %{total: 1}} = LogStream.query([])

      Application.put_env(:log_stream, :retention_max_age, 0)
      Process.sleep(1100)

      {:ok, deleted} = LogStream.Retention.run_now()
      assert deleted >= 1

      {:ok, %{total: 0}} = LogStream.query([])
    end

    test "size-based retention in memory mode" do
      for i <- 1..5 do
        Logger.info("block #{i}")
        LogStream.flush()
      end

      {:ok, %{total: before_count}} = LogStream.query([])
      assert before_count == 5

      Application.put_env(:log_stream, :retention_max_size, 1)
      {:ok, deleted} = LogStream.Retention.run_now()
      assert deleted >= 1

      {:ok, %{total: after_count}} = LogStream.query([])
      assert after_count < before_count
    end
  end

  describe "pagination" do
    test "limit and offset work in memory mode" do
      for i <- 1..20 do
        Logger.info("entry #{i}")
      end

      LogStream.flush()

      {:ok, %LogStream.Result{entries: page, total: 20}} =
        LogStream.query(limit: 5, offset: 0, order: :asc)

      assert length(page) == 5
    end
  end
end
