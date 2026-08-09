defmodule TimelessLogs.LibsqlEngineTest do
  use ExUnit.Case, async: false

  @extension System.get_env("TIMELESS_EXT_PATH") ||
               Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)

  setup do
    dir = Path.join(System.tmp_dir!(), "tl_libsql_engine_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp start_engine!(dir) do
    start_supervised!({TimelessLogs.LibsqlEngine, data_dir: dir, extension_path: @extension})
  end

  test "write path round-trips through the vtab", %{dir: dir} do
    start_engine!(dir)

    entries = [
      %{
        timestamp: 1_700_000_000_000_000,
        level: :info,
        message: "user 4821 logged in from 10.0.0.5",
        metadata: %{"service" => "auth", "status" => 200}
      },
      %{timestamp: 1_700_000_000_000_500, level: "error", message: "boom", metadata: nil}
    ]

    assert :ok = TimelessLogs.LibsqlEngine.ingest(entries)
    assert {:ok, _} = TimelessLogs.LibsqlEngine.flush()

    assert {:ok, [[2]]} = TimelessLogs.LibsqlEngine.sql("SELECT COUNT(*) FROM logs")

    assert {:ok, [["boom"]]} =
             TimelessLogs.LibsqlEngine.sql("SELECT message FROM logs WHERE level = 'error'")

    assert {:ok, [[1_700_000_000_000_000, "user 4821 logged in from 10.0.0.5"]]} =
             TimelessLogs.LibsqlEngine.sql(
               "SELECT ts, message FROM logs WHERE level = 'info' ORDER BY ts"
             )
  end

  test "cold reopen preserves entries ingested without an explicit flush", %{dir: dir} do
    start_engine!(dir)

    entries =
      for i <- 1..5 do
        %{
          timestamp: 1_700_000_000_000_000 + i,
          level: :info,
          message: "entry #{i}",
          metadata: %{}
        }
      end

    assert :ok = TimelessLogs.LibsqlEngine.ingest(entries)
    # No flush: terminate/2 must issue the final flush on clean shutdown.
    :ok = stop_supervised!(TimelessLogs.LibsqlEngine)

    start_engine!(dir)
    assert {:ok, [[5]]} = TimelessLogs.LibsqlEngine.sql("SELECT COUNT(*) FROM logs")
  end

  test "refuses an unmigrated legacy block store", %{dir: dir} do
    File.mkdir_p!(dir)
    File.touch!(Path.join(dir, "logs_index.db"))

    assert {:error, _} =
             start_supervised(
               {TimelessLogs.LibsqlEngine, data_dir: dir, extension_path: @extension}
             )
  end
end
