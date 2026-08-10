defmodule TimelessLogs.LibsqlVacuumTest do
  @moduledoc """
  Returning freed pages to the filesystem.

  Retention runs inside the extension, so blocks are deleted continuously. With
  SQLite's default `auto_vacuum=NONE` those pages go on the freelist and stay
  there: the file only ever grows to its high-water mark. A production store
  reached 1.86 GB holding 813 KB of blocks — 99.8% freelist — which is not a
  compression problem but an administration one.

  Startup puts the store on incremental auto-vacuum (reclaiming whatever has
  already accumulated), and a timer returns pages in bounded batches after
  that, so it stays healthy without anyone running maintenance by hand.
  """

  use ExUnit.Case, async: false

  alias TimelessLogs.LibsqlCandidate

  @data_dir "test/tmp/libsql_vacuum"
  @extension System.get_env("TIMELESS_EXT_PATH") ||
               Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)

  setup do
    Application.stop(:timeless_logs)
    File.rm_rf!(@data_dir)
    File.mkdir_p!(@data_dir)

    previous = %{
      engine: Application.get_env(:timeless_logs, :engine),
      data_dir: Application.get_env(:timeless_logs, :data_dir),
      extension_path: Application.get_env(:timeless_logs, :extension_path)
    }

    on_exit(fn ->
      Application.stop(:timeless_logs)

      for {key, value} <- previous do
        case value do
          nil -> Application.delete_env(:timeless_logs, key)
          _ -> Application.put_env(:timeless_logs, key, value)
        end
      end

      File.rm_rf!(@data_dir)
      {:ok, _} = Application.ensure_all_started(:timeless_logs)
    end)

    :ok
  end

  defp start_engine do
    Application.put_env(:timeless_logs, :engine, :libsql)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :extension_path, @extension)
    {:ok, _} = Application.ensure_all_started(:timeless_logs)
  end

  defp pragma(name) do
    path = Path.join(@data_dir, "logs.db")
    {:ok, conn, _} = LibsqlCandidate.open_connection(path, @extension)
    {:ok, [[value]]} = TimelessLogs.DB.execute(conn, "PRAGMA #{name}", [])
    Exqlite.Sqlite3.close(conn)
    value
  end

  test "a fresh store is put on incremental auto-vacuum" do
    start_engine()

    assert pragma("auto_vacuum") in [2, "2"],
           "expected incremental auto-vacuum so freed pages can be returned"
  end

  test "a store created without auto-vacuum is converted and reclaimed" do
    path = Path.join(@data_dir, "logs.db")

    # Build a store the way SQLite would by default: auto_vacuum NONE, with a
    # freelist left behind by deletes.
    {:ok, conn, caps} = LibsqlCandidate.open_connection(path, @extension)
    :ok = LibsqlCandidate.initialize_database(conn, caps, nil)
    assert pragma("auto_vacuum") in [0, "0"]

    {:ok, _} = TimelessLogs.DB.execute(conn, "CREATE TABLE ballast (id INTEGER, blob BLOB)", [])

    for i <- 1..400 do
      {:ok, _} =
        TimelessLogs.DB.execute(conn, "INSERT INTO ballast VALUES (?1, ?2)", [
          i,
          :crypto.strong_rand_bytes(8_192)
        ])
    end

    {:ok, _} = TimelessLogs.DB.execute(conn, "DELETE FROM ballast", [])
    Exqlite.Sqlite3.close(conn)

    before_bytes = File.stat!(path).size
    freelist_before = pragma("freelist_count")

    assert freelist_before > 0, "the fixture should leave pages on the freelist"

    start_engine()

    assert pragma("auto_vacuum") in [2, "2"]

    after_bytes = File.stat!(path).size

    assert after_bytes < before_bytes,
           "startup should have reclaimed the freelist: #{before_bytes} -> #{after_bytes}"
  end

  test "reclaiming does not lose entries" do
    start_engine()

    base = System.os_time(:microsecond)

    :ok =
      TimelessLogs.ingest(
        for i <- 1..50 do
          %{
            timestamp: base + i,
            level: :info,
            message: "entry #{i}",
            metadata: %{"service" => "api"}
          }
        end
      )

    :ok = TimelessLogs.flush()

    # Force a vacuum turn the way the timer would.
    send(Process.whereis(TimelessLogs.LibsqlEngine), :vacuum)
    Process.sleep(200)

    assert {:ok, %TimelessLogs.Stats{total_entries: 50}} = TimelessLogs.stats()
    assert {:ok, %TimelessLogs.Result{total: 50}} = TimelessLogs.query([])
  end

  test "the engine keeps serving after a vacuum turn" do
    start_engine()

    send(Process.whereis(TimelessLogs.LibsqlEngine), :vacuum)
    Process.sleep(100)

    :ok =
      TimelessLogs.ingest([
        %{
          timestamp: System.os_time(:microsecond),
          level: :info,
          message: "after vacuum",
          metadata: %{}
        }
      ])

    :ok = TimelessLogs.flush()
    assert {:ok, %TimelessLogs.Result{total: 1}} = TimelessLogs.query([])
  end
end
