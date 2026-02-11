defmodule LogStream.RetentionTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/retention"

  setup do
    Application.stop(:log_stream)
    File.rm_rf!(@data_dir)
    Application.put_env(:log_stream, :data_dir, @data_dir)
    Application.put_env(:log_stream, :flush_interval, 60_000)
    Application.put_env(:log_stream, :max_buffer_size, 10_000)
    Application.put_env(:log_stream, :retention_max_age, nil)
    Application.put_env(:log_stream, :retention_max_size, nil)
    Application.put_env(:log_stream, :retention_check_interval, 600_000)
    Application.ensure_all_started(:log_stream)

    on_exit(fn ->
      Application.stop(:log_stream)
      Application.put_env(:log_stream, :retention_max_age, nil)
      Application.put_env(:log_stream, :retention_max_size, nil)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  describe "age-based retention" do
    test "deletes blocks older than max_age" do
      # Create some log entries
      Logger.info("old log")
      Logger.info("another old log")
      LogStream.flush()

      {:ok, %{total: before_count}} = LogStream.query([])
      assert before_count == 2

      # Set max age to 0 seconds (everything is "old")
      Application.put_env(:log_stream, :retention_max_age, 0)

      # Wait a second so the blocks are definitely older than 0
      Process.sleep(1100)

      assert {:ok, deleted} = LogStream.Retention.run_now()
      assert deleted >= 1

      {:ok, %{total: after_count}} = LogStream.query([])
      assert after_count == 0
    end

    test "keeps blocks within max_age" do
      Logger.info("fresh log")
      LogStream.flush()

      # Set max age to 1 hour - blocks should be kept
      Application.put_env(:log_stream, :retention_max_age, 3600)

      assert {:ok, 0} = LogStream.Retention.run_now()

      {:ok, %{total: count}} = LogStream.query([])
      assert count == 1
    end
  end

  describe "size-based retention" do
    test "deletes oldest blocks when over size limit" do
      # Create multiple blocks
      for i <- 1..5 do
        Logger.info("block #{i} entry", batch: "#{i}")
        LogStream.flush()
      end

      {:ok, %{total: before_count}} = LogStream.query([])
      assert before_count == 5

      # Set a very small size limit to force deletion
      Application.put_env(:log_stream, :retention_max_size, 1)

      assert {:ok, deleted} = LogStream.Retention.run_now()
      assert deleted >= 1

      {:ok, %{total: after_count}} = LogStream.query([])
      assert after_count < before_count
    end
  end

  describe "no retention configured" do
    test "does nothing when both max_age and max_size are nil" do
      Logger.info("keeper")
      LogStream.flush()

      assert :noop = LogStream.Retention.run_now()

      {:ok, %{total: 1}} = LogStream.query([])
    end
  end

  describe "block file cleanup" do
    test "removes block files from disk when deleting" do
      Logger.info("will be deleted")
      LogStream.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      block_files_before = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(block_files_before) == 1

      Application.put_env(:log_stream, :retention_max_age, 0)
      Process.sleep(1100)

      LogStream.Retention.run_now()

      block_files_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(block_files_after) == 0
    end
  end
end
