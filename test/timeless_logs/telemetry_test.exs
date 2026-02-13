defmodule TimelessLogs.TelemetryTest do
  use ExUnit.Case, async: false

  require Logger

  @data_dir "test/tmp/telemetry"

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

  describe "flush telemetry" do
    test "emits [:timeless_logs, :flush, :stop] on flush" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:timeless_logs, :flush, :stop]
        ])

      Logger.info("telemetry test")
      TimelessLogs.flush()

      assert_receive {[:timeless_logs, :flush, :stop], ^ref, measurements, metadata}
      assert is_integer(measurements.duration)
      assert measurements.entry_count >= 1
      assert measurements.byte_size > 0
      assert is_integer(metadata.block_id)
    end
  end

  describe "query telemetry" do
    test "emits [:timeless_logs, :query, :stop] on query" do
      Logger.info("query telemetry test")
      TimelessLogs.flush()

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:timeless_logs, :query, :stop]
        ])

      TimelessLogs.query(level: :info)

      assert_receive {[:timeless_logs, :query, :stop], ^ref, measurements, metadata}
      assert is_integer(measurements.duration)
      assert measurements.total >= 1
      assert measurements.blocks_read >= 1
      assert metadata.filters == [level: :info]
    end
  end

  describe "block error telemetry" do
    test "emits [:timeless_logs, :block, :error] for corrupt blocks" do
      # Write a log, flush it, then corrupt the block file
      Logger.info("will be corrupted")
      TimelessLogs.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      [block_file] = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      File.write!(block_file, "corrupt data")

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:timeless_logs, :block, :error]
        ])

      TimelessLogs.query([])

      assert_receive {[:timeless_logs, :block, :error], ^ref, _measurements, metadata}
      assert metadata.reason == :corrupt_block
      assert metadata.file_path == block_file
    end
  end
end
