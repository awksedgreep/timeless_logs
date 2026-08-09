defmodule TimelessLogs.LibsqlEngine do
  @moduledoc """
  In-process storage engine over the timeless-libsql `timeless_logs`
  virtual table — Session 1 of the port plan
  (`notes/libsql_engine_port_plan_2026-08-09.md`).

  Opt-in via `config :timeless_logs, engine: :libsql`. The writer owns
  `<data_dir>/logs.db`: ingest encodes public rich-v1 batches (the
  migration candidate's validated encoder) and control commands ride the
  vtab's shadow-name channel — the same public surface the external Rust
  owner uses, so embedded and external share one on-disk format.

  Session 1 scope is the write path (ingest/flush/optimize) plus raw SQL
  access; the query surface routes here in Session 2.
  """

  use GenServer

  require Logger

  alias TimelessLogs.LibsqlCandidate

  @table "logs"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Ingest normalized entries (`%{timestamp:, level:, message:, metadata:}`)."
  def ingest([]), do: :ok
  def ingest(entries), do: GenServer.call(__MODULE__, {:ingest, entries}, :infinity)

  @doc "Persist buffered entries into blocks now."
  def flush, do: command("flush")

  @doc "Run a bounded background optimize pass."
  def optimize, do: command("optimize")

  @doc false
  def sql(sql, params \\ []), do: GenServer.call(__MODULE__, {:sql, sql, params}, :infinity)

  @doc """
  Query entries with the facade's filter vocabulary. Time range and a
  single level equality push down into the vtab scan; everything else
  (metadata, metadata_any, message-contains) applies the SHARED
  `TimelessLogs.Filter` residual — semantics identical to the Elixir
  engine by construction.
  """
  def query(filters), do: GenServer.call(__MODULE__, {:query, filters}, :infinity)

  @doc "Exact count of entries matching the filters."
  def count(filters), do: GenServer.call(__MODULE__, {:count, filters}, :infinity)

  defp command(cmd),
    do:
      GenServer.call(
        __MODULE__,
        {:sql, "INSERT INTO #{@table}(#{@table}) VALUES (?1)", [cmd]},
        :infinity
      )

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 (final flush + close) runs on ordinary
    # supervisor shutdown — the metrics engine's shutdown-durability
    # lesson, applied from day one.
    Process.flag(:trap_exit, true)
    data_dir = Keyword.get(opts, :data_dir, TimelessLogs.Config.data_dir())
    reject_unmigrated_legacy_store!(data_dir)
    File.mkdir_p!(data_dir)
    path = Path.join(data_dir, "logs.db")

    retention_seconds =
      Keyword.get(opts, :retention_seconds, TimelessLogs.Config.retention_max_age())

    with {:ok, conn, capabilities} <-
           LibsqlCandidate.open_connection(path, Keyword.get(opts, :extension_path)),
         :ok <- LibsqlCandidate.initialize_database(conn, capabilities, retention_seconds) do
      Logger.info(
        "timeless_logs libSQL engine: extension #{capabilities["extension_version"]} " <>
          "(data ABI #{capabilities["data_abi"]}) on #{path}"
      )

      flush_timer = schedule_flush()
      {:ok, %{conn: conn, path: path, flush_timer: flush_timer}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:ingest, entries}, _from, state) do
    result =
      with {:ok, blob} <- LibsqlCandidate.encode_batch(entries),
           {:ok, _} <-
             LibsqlCandidate.execute(
               state.conn,
               "INSERT INTO #{@table}(#{@table}) VALUES (?1)",
               [{:blob, blob}]
             ) do
        :ok
      end

    {:reply, result, state}
  end

  def handle_call({:sql, sql, params}, _from, state) do
    {:reply, LibsqlCandidate.execute(state.conn, sql, params), state}
  end

  def handle_call({:query, filters}, _from, state) do
    {:reply, run_query(state.conn, filters), state}
  end

  def handle_call({:count, filters}, _from, state) do
    result =
      case run_query(state.conn, Keyword.merge(filters, limit: 0, offset: 0)) do
        {:ok, %TimelessLogs.Result{total: total}} -> {:ok, total}
        {:error, _} = error -> error
      end

    {:reply, result, state}
  end

  @impl true
  def handle_info(:flush, state) do
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    {:noreply, %{state | flush_timer: schedule_flush()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    Exqlite.Sqlite3.close(state.conn)
  end

  defp schedule_flush do
    Process.send_after(self(), :flush, TimelessLogs.Config.flush_interval())
  end

  # -- Query path -----------------------------------------------------------

  @pagination_keys [:limit, :offset, :order, :count_total]

  # The extension only ever stores these severities (batch decode rejects
  # anything else), so the read-side atom mapping is a closed set — no
  # dynamic atom creation from stored data.
  @severities Map.new(
                ~w(debug info notice warning warn error critical alert emergency),
                &{&1, String.to_atom(&1)}
              )

  defp run_query(conn, filters) do
    {pagination, search} = Enum.split_with(filters, fn {k, _} -> k in @pagination_keys end)
    order = Keyword.get(pagination, :order, :asc)

    with {:ok, rows} <- select_entries(conn, search, order) do
      matched =
        rows
        |> Enum.map(&decode_row/1)
        |> TimelessLogs.Filter.filter(search)

      total = length(matched)
      limit = Keyword.get(pagination, :limit, 100)
      offset = Keyword.get(pagination, :offset, 0)

      entries =
        matched
        |> Enum.drop(offset)
        |> Enum.take(limit)
        |> Enum.map(&TimelessLogs.Entry.from_map/1)

      {:ok,
       %TimelessLogs.Result{
         entries: entries,
         total: total,
         limit: limit,
         offset: offset,
         has_more: offset + limit < total
       }}
    end
  end

  # ts range and single-level equality prune inside the extension (block
  # ts_min/ts_max + the level partition term index); the shared Filter
  # re-checks everything, so pushdown is purely an optimization.
  defp select_entries(conn, search, order) do
    {where, params} =
      Enum.reduce(search, {[], []}, fn
        {:since, ts}, {w, p} ->
          {["ts >= ?#{length(p) + 1}" | w], p ++ [TimelessLogs.Timestamp.to_microseconds(ts)]}

        {:until, ts}, {w, p} ->
          {["ts <= ?#{length(p) + 1}" | w], p ++ [TimelessLogs.Timestamp.to_microseconds(ts)]}

        {:level, level}, {w, p} when is_atom(level) ->
          {["level = ?#{length(p) + 1}" | w], p ++ [Atom.to_string(level)]}

        _other, acc ->
          acc
      end)

    where_sql = if where == [], do: "", else: " WHERE " <> Enum.join(Enum.reverse(where), " AND ")
    order_sql = if order == :desc, do: " ORDER BY ts DESC", else: " ORDER BY ts ASC"

    LibsqlCandidate.execute(
      conn,
      "SELECT ts, level, message, metadata FROM #{@table}#{where_sql}#{order_sql}",
      params
    )
  end

  defp decode_row([ts, level, message, metadata_json]) do
    %{
      timestamp: ts,
      level: Map.get(@severities, level, :info),
      message: message,
      metadata: decode_metadata(metadata_json)
    }
  end

  defp decode_metadata(nil), do: %{}
  defp decode_metadata(""), do: %{}

  defp decode_metadata(json) when is_binary(json) do
    case :json.decode(json) do
      metadata when is_map(metadata) -> metadata
      _ -> %{}
    end
  rescue
    _ -> %{}
  end

  # A data_dir carrying the legacy block-store layout must be converted
  # with TimelessLogs.ReleaseMigration before the libSQL engine will
  # touch it — never silently ignore existing data (the metrics
  # reject_unmigrated_rust_store! precedent).
  defp reject_unmigrated_legacy_store!(data_dir) do
    legacy? =
      File.exists?(Path.join(data_dir, "logs_index.db")) or
        File.dir?(Path.join(data_dir, "blocks"))

    migrated? = File.exists?(Path.join(data_dir, "logs.db"))

    if legacy? and not migrated? do
      raise "timeless_logs engine: :libsql refuses to start against the unmigrated " <>
              "legacy block store in #{data_dir} — run the TimelessLogs.ReleaseMigration " <>
              "conversion first, or configure engine: :elixir"
    end

    :ok
  end
end
