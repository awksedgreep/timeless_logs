defmodule LogStream.TelemetryTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/telemetry"

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

  describe "flush telemetry" do
    test "emits [:log_stream, :flush, :stop] on flush" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:log_stream, :flush, :stop]
        ])

      Logger.info("telemetry test")
      LogStream.flush()

      assert_receive {[:log_stream, :flush, :stop], ^ref, measurements, metadata}
      assert is_integer(measurements.duration)
      assert measurements.entry_count >= 1
      assert measurements.byte_size > 0
      assert is_integer(metadata.block_id)
    end
  end

  describe "query telemetry" do
    test "emits [:log_stream, :query, :stop] on query" do
      Logger.info("query telemetry test")
      LogStream.flush()

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:log_stream, :query, :stop]
        ])

      LogStream.query(level: :info)

      assert_receive {[:log_stream, :query, :stop], ^ref, measurements, metadata}
      assert is_integer(measurements.duration)
      assert measurements.total >= 1
      assert measurements.blocks_read >= 1
      assert metadata.filters == [level: :info]
    end
  end

  describe "block error telemetry" do
    test "emits [:log_stream, :block, :error] for corrupt blocks" do
      # Write a log, flush it, then corrupt the block file
      Logger.info("will be corrupted")
      LogStream.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      [block_file] = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      File.write!(block_file, "corrupt data")

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:log_stream, :block, :error]
        ])

      LogStream.query([])

      assert_receive {[:log_stream, :block, :error], ^ref, _measurements, metadata}
      assert metadata.reason == :corrupt_block
      assert metadata.file_path == block_file
    end
  end
end
