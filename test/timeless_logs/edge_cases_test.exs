defmodule TimelessLogs.EdgeCasesTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/edge_cases"

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

  describe "empty state" do
    test "querying with no logs returns empty result" do
      {:ok, %TimelessLogs.Result{entries: [], total: 0}} = TimelessLogs.query([])
    end

    test "querying with filters on empty state returns empty result" do
      {:ok, %TimelessLogs.Result{entries: [], total: 0}} =
        TimelessLogs.query(level: :error, message: "nothing")
    end

    test "flushing empty buffer is a no-op" do
      assert :ok = TimelessLogs.flush()
    end
  end

  describe "special characters" do
    test "handles unicode in messages" do
      Logger.info("Ошибка подключения к серверу 日本語テスト")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: [entry]}} = TimelessLogs.query([])
      assert entry.message =~ "Ошибка"
      assert entry.message =~ "日本語"
    end

    test "handles unicode in metadata values" do
      Logger.info("test", user_name: "Ünïcödé")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: [entry]}} = TimelessLogs.query([])
      assert entry.metadata["user_name"] == "Ünïcödé"
    end

    test "handles empty message" do
      Logger.info("")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query([])
      assert length(entries) >= 1
    end

    test "handles very long messages" do
      long_msg = String.duplicate("x", 100_000)
      Logger.info(long_msg)
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: [entry]}} = TimelessLogs.query([])
      assert byte_size(entry.message) == 100_000
    end
  end

  describe "multiple flushes" do
    test "logs across multiple blocks are all queryable" do
      Logger.info("block one")
      TimelessLogs.flush()

      Logger.info("block two")
      TimelessLogs.flush()

      Logger.info("block three")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{total: 3}} = TimelessLogs.query([])

      blocks_dir = Path.join(@data_dir, "blocks")
      block_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(block_files) == 3
    end
  end

  describe "auto-flush on buffer size" do
    test "buffer flushes automatically at max_buffer_size" do
      Application.stop(:timeless_logs)
      Application.put_env(:timeless_logs, :max_buffer_size, 5)
      Application.ensure_all_started(:timeless_logs)

      for i <- 1..10 do
        Logger.info("auto flush #{i}")
      end

      # Give time for the auto-flush to process
      Process.sleep(100)

      # There should be blocks on disk without calling flush
      blocks_dir = Path.join(@data_dir, "blocks")
      block_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(block_files) >= 1
    end
  end

  describe "combined filters" do
    test "level + message + metadata + time range" do
      now = System.system_time(:second)

      Logger.error("database timeout", service: "db", region: "us-east")
      Logger.error("network timeout", service: "api", region: "eu-west")
      Logger.info("database timeout", service: "db", region: "us-east")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: results}} =
        TimelessLogs.query(
          level: :error,
          message: "database",
          metadata: %{service: "db"},
          since: now - 10
        )

      assert length(results) == 1
      assert hd(results).message =~ "database timeout"
      assert hd(results).metadata["region"] == "us-east"
    end
  end

  describe "pagination edge cases" do
    test "offset beyond total returns empty" do
      Logger.info("only one")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: [], total: 1, offset: 100}} =
        TimelessLogs.query(offset: 100)
    end

    test "limit of 0 returns empty entries with correct total" do
      Logger.info("test")
      TimelessLogs.flush()

      {:ok, %TimelessLogs.Result{entries: [], total: 1, limit: 0}} =
        TimelessLogs.query(limit: 0)
    end
  end

  describe "entry struct" do
    test "from_map creates proper Entry struct" do
      entry =
        TimelessLogs.Entry.from_map(%{
          timestamp: 12345,
          level: :warning,
          message: "test msg",
          metadata: %{"key" => "val"}
        })

      assert %TimelessLogs.Entry{} = entry
      assert entry.timestamp == 12345
      assert entry.level == :warning
      assert entry.message == "test msg"
      assert entry.metadata == %{"key" => "val"}
    end
  end

  describe "query with corrupt blocks" do
    test "skips corrupt blocks and returns results from good ones" do
      Logger.info("good entry one")
      TimelessLogs.flush()

      Logger.info("good entry two")
      TimelessLogs.flush()

      # Corrupt the first block
      blocks_dir = Path.join(@data_dir, "blocks")
      [first_block | _] = Path.wildcard(Path.join(blocks_dir, "*.raw")) |> Enum.sort()
      File.write!(first_block, "corrupted")

      {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query([])
      # Should get at least the entry from the non-corrupt block
      assert length(entries) >= 1
    end
  end
end
