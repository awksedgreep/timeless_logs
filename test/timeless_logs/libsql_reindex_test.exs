defmodule TimelessLogs.LibsqlReindexTest do
  @moduledoc """
  Upgrading a store created before the indexed-key list was widened.

  `index_keys` is fixed when the virtual table is created and persisted in
  `logs_meta`; `CREATE VIRTUAL TABLE IF NOT EXISTS` will not change it. Because
  postings are written at insert time from that persisted list, pruning on a
  newly listed key would skip every block written before the change — the
  entries are still stored, but the search stops returning them.

  These tests build a store the way an older release would have, then start the
  engine over it and assert the entries are findable by the newly indexed
  spellings.
  """

  use ExUnit.Case, async: false

  alias TimelessLogs.LibsqlCandidate

  @data_dir "test/tmp/libsql_reindex"
  @extension System.get_env("TIMELESS_EXT_PATH") ||
               Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)

  # What the store was created with before the alias spellings were indexed.
  @old_keys "service,path,status,host"

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

  defp seed_old_store(entries) do
    path = Path.join(@data_dir, "logs.db")
    {:ok, conn, _caps} = LibsqlCandidate.open_connection(path, @extension)

    {:ok, _} =
      TimelessLogs.DB.execute(
        conn,
        "CREATE VIRTUAL TABLE IF NOT EXISTS logs USING timeless_logs(" <>
          "index_keys='#{@old_keys}',timestamp_unit='us')",
        []
      )

    for e <- entries do
      {:ok, _} =
        TimelessLogs.DB.execute(
          conn,
          "INSERT INTO logs(ts, level, message, metadata) VALUES (?1, ?2, ?3, ?4)",
          [e.timestamp, to_string(e.level), e.message, :json.encode(e.metadata) |> to_string()]
        )
    end

    {:ok, _} = TimelessLogs.DB.execute(conn, "INSERT INTO logs(logs) VALUES ('flush')", [])
    Exqlite.Sqlite3.close(conn)
    path
  end

  defp start_engine do
    Application.put_env(:timeless_logs, :engine, :libsql)
    Application.put_env(:timeless_logs, :data_dir, @data_dir)
    Application.put_env(:timeless_logs, :extension_path, @extension)
    {:ok, _} = Application.ensure_all_started(:timeless_logs)
  end

  defp persisted_keys do
    path = Path.join(@data_dir, "logs.db")
    {:ok, conn, _} = LibsqlCandidate.open_connection(path, @extension)

    {:ok, [[value]]} =
      TimelessLogs.DB.execute(conn, "SELECT v FROM logs_meta WHERE k = ?1", ["index_keys"])

    Exqlite.Sqlite3.close(conn)
    if is_list(value), do: IO.iodata_to_binary(value), else: value
  end

  defp messages(filters) do
    {:ok, %TimelessLogs.Result{entries: entries}} = TimelessLogs.query(filters)
    entries |> Enum.map(& &1.message) |> Enum.sort()
  end

  test "an older store is reindexed on startup and its keys become current" do
    base = System.os_time(:microsecond)

    seed_old_store([
      %{timestamp: base, level: :info, message: "old one", metadata: %{"service" => "api"}},
      %{timestamp: base + 1, level: :info, message: "old two", metadata: %{"service" => "worker"}}
    ])

    assert persisted_keys() == @old_keys

    start_engine()

    assert persisted_keys() == Enum.join(LibsqlCandidate.index_keys(), ","),
           "startup should have widened the persisted key list"
  end

  test "entries written before the widening are still found afterwards" do
    base = System.os_time(:microsecond)

    seed_old_store([
      %{timestamp: base, level: :info, message: "old one", metadata: %{"service" => "api"}},
      %{timestamp: base + 1, level: :info, message: "old two", metadata: %{"service" => "worker"}}
    ])

    start_engine()

    # This is the regression that matters: without the reindex these prune to
    # nothing, because the pre-widening blocks carry no postings for the alias
    # spellings the filter now expands into.
    assert messages(metadata: %{"service" => "worker"}) == ["old two"]
    assert messages(metadata: %{"service" => "api"}) == ["old one"]
  end

  test "old and new entries are both visible after the upgrade" do
    base = System.os_time(:microsecond)

    seed_old_store([
      %{timestamp: base, level: :info, message: "before", metadata: %{"service" => "api"}}
    ])

    start_engine()

    :ok =
      TimelessLogs.ingest([
        %{
          timestamp: base + 10,
          level: :info,
          message: "after",
          metadata: %{"service" => "api"}
        }
      ])

    :ok = TimelessLogs.flush()

    assert messages(metadata: %{"service" => "api"}) == ["after", "before"]
  end

  test "a second startup does not reindex again" do
    base = System.os_time(:microsecond)

    seed_old_store([
      %{timestamp: base, level: :info, message: "one", metadata: %{"service" => "api"}}
    ])

    start_engine()
    keys_after_first = persisted_keys()

    Application.stop(:timeless_logs)
    start_engine()

    assert persisted_keys() == keys_after_first
    assert messages(metadata: %{"service" => "api"}) == ["one"]
  end

  test "a store created fresh needs no reindex and is already current" do
    start_engine()

    assert persisted_keys() == Enum.join(LibsqlCandidate.index_keys(), ",")

    :ok =
      TimelessLogs.ingest([
        %{
          timestamp: System.os_time(:microsecond),
          level: :info,
          message: "fresh",
          metadata: %{"service" => "api"}
        }
      ])

    :ok = TimelessLogs.flush()
    assert messages(metadata: %{"service" => "api"}) == ["fresh"]
  end
end
