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
    assert :ok = TimelessLogs.LibsqlEngine.flush()

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

  test "stats reports compressed blocks after optimize (dashboard tile contract)", %{dir: dir} do
    start_engine!(dir)

    entries =
      for i <- 1..50 do
        %{
          timestamp: 1_700_000_000_000_000 + i * 1_000,
          level: :info,
          message: "entry #{i}",
          metadata: %{"service" => "api"}
        }
      end

    assert :ok = TimelessLogs.LibsqlEngine.ingest(entries)
    assert :ok = TimelessLogs.LibsqlEngine.flush()

    assert {:ok, %TimelessLogs.Stats{} = raw_stats} = TimelessLogs.LibsqlEngine.stats()
    assert raw_stats.raw_blocks > 0
    assert raw_stats.compressed_blocks == 0

    assert {:ok, _} = TimelessLogs.LibsqlEngine.optimize()

    assert {:ok, %TimelessLogs.Stats{} = stats} = TimelessLogs.LibsqlEngine.stats()
    # The dashboard's compressed tile must see this work in the struct, not
    # have it stay extension-only. Note compressed_*, not zstd_*: the libSQL
    # engine writes adaptive columnar blocks, and the per-format zstd/openzl
    # fields belong to the legacy engine.
    assert stats.raw_blocks == 0
    assert stats.compressed_blocks > 0
    assert stats.compressed_bytes > 0
    assert stats.zstd_blocks == 0
    assert stats.storage_mode == :libsql
    assert stats.compaction_count > 0
    assert stats.total_blocks == stats.compressed_blocks
    assert stats.compression_raw_bytes_in > 0
    assert stats.compression_compressed_bytes_out > 0

    # The ratio pair is persisted in the store, not the process: a fresh
    # engine over the same db must still report it (the tile once read
    # process-local counters and showed "pending" after every restart).
    stop_supervised!(TimelessLogs.LibsqlEngine)
    start_engine!(dir)
    assert {:ok, %TimelessLogs.Stats{} = reopened} = TimelessLogs.LibsqlEngine.stats()
    assert reopened.compression_raw_bytes_in == stats.compression_raw_bytes_in
    assert reopened.compression_compressed_bytes_out == stats.compression_compressed_bytes_out
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

    assert :ok = TimelessLogs.LibsqlEngine.flush()

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

  test "refuses an unmigrated legacy block store when auto_migrate is off", %{dir: dir} do
    File.mkdir_p!(dir)
    File.touch!(Path.join(dir, "logs_index.db"))

    assert {:error, _} =
             start_supervised(
               {TimelessLogs.LibsqlEngine,
                data_dir: dir, extension_path: @extension, auto_migrate: false}
             )
  end

  test "auto-converts a legacy block store at startup", %{dir: dir} do
    # Build a real legacy store through the running app.
    Application.stop(:timeless_logs)

    previous = Application.get_env(:timeless_logs, :data_dir)
    Application.put_env(:timeless_logs, :data_dir, dir)
    {:ok, _} = Application.ensure_all_started(:timeless_logs)

    :ok =
      TimelessLogs.ingest([
        %{timestamp: 1_700_000_000_000_000, level: :info, message: "legacy one", metadata: %{}},
        %{timestamp: 1_700_000_000_000_001, level: :error, message: "legacy two", metadata: %{}}
      ])

    :ok = TimelessLogs.flush()
    Application.stop(:timeless_logs)

    case previous do
      nil -> Application.delete_env(:timeless_logs, :data_dir)
      _ -> Application.put_env(:timeless_logs, :data_dir, previous)
    end

    on_exit(fn -> {:ok, _} = Application.ensure_all_started(:timeless_logs) end)

    assert File.exists?(Path.join(dir, "logs_index.db"))

    # Default startup on :libsql converts automatically, then serves it.
    start_engine!(dir)

    assert {:ok, %TimelessLogs.Result{total: 2}} = TimelessLogs.LibsqlEngine.query([])

    assert {:ok, %TimelessLogs.Result{entries: [%TimelessLogs.Entry{message: "legacy two"}]}} =
             TimelessLogs.LibsqlEngine.query(level: :error)

    # The source is retained for rollback.
    assert File.exists?(Path.join(dir, "logs_index.db"))
  end
end
