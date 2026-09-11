defmodule TimelessLogs.FieldDiscoveryTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/field_discovery"

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :flush_interval, 60_000)
    Application.put_env(:timeless_logs, :max_buffer_size, 10_000)
    Application.put_env(:timeless_logs, :retention_max_age, nil)
    Application.put_env(:timeless_logs, :retention_max_size, nil)
    Application.put_env(:timeless_logs, :ingest_shard_count, 1)
    Application.ensure_all_started(:timeless_logs)

    base = System.os_time(:microsecond) - 60_000_000

    :ok =
      TimelessLogs.ingest([
        %{timestamp: base, level: :info, message: "one", metadata: %{"common" => "yes"}},
        %{timestamp: base + 1, level: :info, message: "two", metadata: %{"common" => "yes"}},
        %{
          timestamp: base + 2,
          level: :info,
          message: "three",
          metadata: %{"common" => "yes", "late_field" => "visible"}
        }
      ])

    :ok = TimelessLogs.flush()
    :ok = TimelessLogs.Index.sync()

    on_exit(fn ->
      Application.stop(:timeless_logs)
      Application.delete_env(:timeless_logs, :ingest_shard_count)
      Application.delete_env(:timeless_logs, :field_scan_limit)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "field value discovery honors an explicit bounded sample" do
    assert {:ok, sampled} = TimelessLogs.field_values("_msg", scan_limit: 2)
    assert Enum.map(sampled, & &1["value"]) |> Enum.sort() == ["one", "two"]

    assert {:ok, complete} = TimelessLogs.field_values("_msg", full_scan: true)
    assert Enum.map(complete, & &1["value"]) |> Enum.sort() == ["one", "three", "two"]
  end

  test "field name discovery is bounded by the configured default" do
    Application.put_env(:timeless_logs, :field_scan_limit, 2)
    assert {:ok, sampled} = TimelessLogs.field_names()
    refute Enum.any?(sampled, &(&1["value"] == "late_field"))

    assert {:ok, complete} = TimelessLogs.field_names(full_scan: true)
    assert Enum.any?(complete, &(&1["value"] == "late_field"))
  end
end
