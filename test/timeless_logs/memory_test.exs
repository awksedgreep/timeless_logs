defmodule TimelessLogs.MemoryTest do
  use ExUnit.Case, async: false

  require Logger

  setup do
    Application.stop(:timeless_logs)
    Application.put_env(:timeless_logs, :storage, :memory)
    Application.put_env(:timeless_logs, :data_dir, "test/tmp/memory_should_not_exist")
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :retention_max_age, nil)
    Application.put_env(:timeless_logs, :retention_max_size, nil)
    Application.ensure_all_started(:timeless_logs)

    on_exit(fn ->
      Application.stop(:timeless_logs)
      Application.put_env(:timeless_logs, :storage, :disk)
    end)

    :ok
  end

  describe "basic pipeline" do
    test "log, flush, query works in memory mode" do
      Logger.info("memory test")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: [entry], total: 1}} = TimelessLogs.query([])
      assert entry.message =~ "memory test"
      assert %TimelessLogs.Entry{} = entry
    end

    test "no files created on disk" do
      Logger.info("no disk")
      TimelessLogs.flush()

      refute File.exists?("test/tmp/memory_should_not_exist")
    end

    test "multiple entries in one flush" do
      Logger.info("one")
      Logger.info("two")
      Logger.info("three")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{total: 3}} = TimelessLogs.query([])
    end
  end

  describe "filtering" do
    test "level filter" do
      Logger.info("info msg")
      Logger.error("error msg")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query(level: :error)
      assert length(entries) == 1
      assert hd(entries).level == :error
    end

    test "message filter" do
      Logger.info("timeout occurred")
      Logger.info("all good")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query(message: "timeout")
      assert length(entries) == 1
      assert hd(entries).message =~ "timeout"
    end

    test "metadata filter" do
      Logger.info("api call", service: "api")
      Logger.info("web call", service: "web")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: entries}} =
        TimelessLogs.query(metadata: %{service: "api"})

      assert length(entries) == 1
      assert hd(entries).metadata["service"] == "api"
    end
  end

  describe "multiple blocks" do
    test "queries across multiple flushes" do
      Logger.info("block one")
      TimelessLogs.flush()

      Logger.info("block two")
      TimelessLogs.flush()

      Logger.info("block three")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{total: 3}} = TimelessLogs.query([])
    end
  end

  describe "streaming" do
    test "stream/1 works in memory mode" do
      for i <- 1..10 do
        Logger.info("stream #{i}")
      end

      TimelessLogs.flush()

      entries = TimelessLogs.stream([]) |> Enum.to_list()
      assert length(entries) == 10
      assert %TimelessLogs.Entry{} = hd(entries)
    end

    test "stream/1 with filters" do
      Logger.info("good")
      Logger.error("bad")
      TimelessLogs.flush()

      errors = TimelessLogs.stream(level: :error) |> Enum.to_list()
      assert length(errors) == 1
      assert hd(errors).level == :error
    end

    test "stream/1 across multiple blocks" do
      Logger.info("a")
      TimelessLogs.flush()
      Logger.info("b")
      TimelessLogs.flush()

      entries = TimelessLogs.stream([]) |> Enum.to_list()
      assert length(entries) == 2
    end
  end

  describe "stats" do
    test "stats/0 works in memory mode" do
      {:ok, stats} = TimelessLogs.stats()
      assert stats.total_blocks == 0
      assert stats.index_size == 0

      Logger.info("entry")
      TimelessLogs.flush()

      {:ok, stats} = TimelessLogs.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 1
      assert stats.total_bytes > 0
    end
  end

  describe "live tail" do
    test "subscribe works in memory mode" do
      TimelessLogs.subscribe()
      Logger.info("live memory")

      assert_receive {:timeless_logs, :entry, %TimelessLogs.Entry{message: msg}}, 1000
      assert msg =~ "live memory"
      TimelessLogs.unsubscribe()
    end
  end

  describe "retention" do
    test "age-based retention in memory mode" do
      Logger.info("will be deleted")
      TimelessLogs.flush()

      {:ok, %{total: 1}} = TimelessLogs.query([])

      Application.put_env(:timeless_logs, :retention_max_age, 0)
      Process.sleep(1100)

      {:ok, deleted} = TimelessLogs.Retention.run_now()
      assert deleted >= 1

      {:ok, %{total: 0}} = TimelessLogs.query([])
    end

    test "size-based retention in memory mode" do
      for i <- 1..5 do
        Logger.info("block #{i}")
        TimelessLogs.flush()
      end

      {:ok, %{total: before_count}} = TimelessLogs.query([])
      assert before_count == 5

      Application.put_env(:timeless_logs, :retention_max_size, 1)
      {:ok, deleted} = TimelessLogs.Retention.run_now()
      assert deleted >= 1

      {:ok, %{total: after_count}} = TimelessLogs.query([])
      assert after_count < before_count
    end
  end

  describe "pagination" do
    test "limit and offset work in memory mode" do
      for i <- 1..20 do
        Logger.info("entry #{i}")
      end

      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: page, total: 20}} =
        TimelessLogs.query(limit: 5, offset: 0, order: :asc)

      assert length(page) == 5
    end
  end
end
