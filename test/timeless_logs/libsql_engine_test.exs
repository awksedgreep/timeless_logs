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

  test "query matches the facade contract: filters, residuals, pagination", %{dir: dir} do
    start_engine!(dir)

    entries =
      for i <- 1..10 do
        %{
          timestamp: 1_700_000_000_000_000 + i * 1_000,
          level: if(rem(i, 2) == 0, do: :error, else: :info),
          message: "request #{i} #{if rem(i, 5) == 0, do: "timeout", else: "ok"}",
          metadata: %{"service" => if(rem(i, 3) == 0, do: "api", else: "web"), "i" => i}
        }
      end

    assert :ok = TimelessLogs.LibsqlEngine.ingest(entries)

    # Buffered entries must be query-visible before any flush.
    assert {:ok, %TimelessLogs.Result{total: 10}} = TimelessLogs.LibsqlEngine.query([])

    assert {:ok, _} = TimelessLogs.LibsqlEngine.flush()

    # Level pushdown + Entry struct round-trip.
    assert {:ok, %TimelessLogs.Result{entries: errors, total: 5}} =
             TimelessLogs.LibsqlEngine.query(level: :error)

    assert Enum.all?(errors, &match?(%TimelessLogs.Entry{level: :error}, &1))

    # Residual metadata filter (non-indexed key) through the shared Filter.
    assert {:ok, %TimelessLogs.Result{total: 3}} =
             TimelessLogs.LibsqlEngine.query(metadata: %{"service" => "api"})

    # Message-contains searches message text (shared Filter semantics).
    assert {:ok, %TimelessLogs.Result{entries: [timeout_entry | _], total: 2}} =
             TimelessLogs.LibsqlEngine.query(message: "TIMEOUT")

    assert timeout_entry.message =~ "timeout"

    # Time range + pagination + ordering.
    assert {:ok, %TimelessLogs.Result{entries: page, total: 10, has_more: true, offset: 4}} =
             TimelessLogs.LibsqlEngine.query(limit: 3, offset: 4)

    assert length(page) == 3
    assert Enum.map(page, & &1.metadata["i"]) == [5, 6, 7]

    assert {:ok, %TimelessLogs.Result{entries: [newest | _]}} =
             TimelessLogs.LibsqlEngine.query(order: :desc, limit: 1)

    assert newest.metadata["i"] == 10

    assert {:ok, %TimelessLogs.Result{total: 4}} =
             TimelessLogs.LibsqlEngine.query(since: 1_700_000_000_000_000 + 7_000)

    # count/1 agrees with query totals.
    assert {:ok, 5} = TimelessLogs.LibsqlEngine.count(level: :error)
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
