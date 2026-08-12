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
  def flush do
    case command("flush") do
      {:ok, _} -> :ok
      error -> error
    end
  end

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

  @doc "Single-snapshot backup: flush, then VACUUM INTO <target>/logs.db."
  def backup(target_dir), do: GenServer.call(__MODULE__, {:backup, target_dir}, :infinity)

  @doc "Aggregate statistics from timeless_stats('logs') — no block reads."
  def stats, do: GenServer.call(__MODULE__, :stats, :infinity)

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
    maybe_auto_migrate_legacy_store!(data_dir, opts)
    File.mkdir_p!(data_dir)
    path = Path.join(data_dir, "logs.db")

    retention_seconds =
      Keyword.get(opts, :retention_seconds, TimelessLogs.Config.retention_max_age())

    extension_path = Keyword.get(opts, :extension_path)

    with {:ok, conn, capabilities} <- LibsqlCandidate.open_connection(path, extension_path),
         :ok <- LibsqlCandidate.initialize_database(conn, capabilities, retention_seconds),
         {:ok, conn} <- apply_index_keys(conn, path, extension_path, capabilities),
         {:ok, _} <- LibsqlCandidate.ensure_auto_vacuum(conn) do
      Logger.info(
        "timeless_logs libSQL engine: extension #{capabilities["extension_version"]} " <>
          "(data ABI #{capabilities["data_abi"]}) on #{path}"
      )

      flush_timer = schedule_flush()
      vacuum_timer = schedule_vacuum()

      {:ok, %{conn: conn, path: path, flush_timer: flush_timer, vacuum_timer: vacuum_timer}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  # A store created by an older release carries the narrower index_keys it was
  # created with, and CREATE VIRTUAL TABLE IF NOT EXISTS will not widen it.
  # Postings are written at insert time, so pruning on a newly indexed key
  # would skip every older block: the entries remain, the search stops finding
  # them. Reindexing rewrites those postings, one decoded block at a time.
  #
  # The virtual table reads index_keys at connect, so a reindex only takes
  # effect for this session after reopening.
  defp apply_index_keys(conn, path, extension_path, capabilities) do
    case LibsqlCandidate.ensure_index_keys(conn) do
      {:ok, status} when status in [:current, :unsupported] ->
        {:ok, conn}

      {:ok, :reindexed} ->
        Exqlite.Sqlite3.close(conn)

        with {:ok, conn, _caps} <- LibsqlCandidate.open_connection(path, extension_path),
             :ok <- LibsqlCandidate.initialize_database(conn, capabilities, nil) do
          Logger.info("timeless_logs: reindex complete; indexed keys are now current")
          {:ok, conn}
        end

      {:error, _} = error ->
        error
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

  def handle_call({:backup, target_dir}, _from, state) do
    result =
      with {:ok, _} <-
             LibsqlCandidate.execute(
               state.conn,
               "INSERT INTO #{@table}(#{@table}) VALUES ('flush')"
             ),
           :ok <- File.mkdir_p(target_dir),
           target = Path.join(target_dir, "logs.db"),
           {:ok, _} <- LibsqlCandidate.execute(state.conn, "VACUUM INTO ?1", [target]),
           {:ok, %{size: size}} <- File.stat(target) do
        {:ok, %{path: target_dir, files: ["logs.db"], total_bytes: size}}
      end

    {:reply, result, state}
  end

  def handle_call(:stats, _from, state) do
    result =
      with {:ok, rows} <-
             LibsqlCandidate.execute(state.conn, "SELECT * FROM timeless_stats('logs')") do
        kv = Map.new(rows, fn [k, v] -> {k, v} end)
        int = fn key -> stat_int(Map.get(kv, key)) end

        {:ok,
         %TimelessLogs.Stats{
           storage_mode: :libsql,
           total_blocks: int.("blocks") || 0,
           total_entries: int.("total_entries") || 0,
           total_bytes: int.("bytes_on_disk") || 0,
           disk_size: int.("bytes_on_disk") || 0,
           index_size: int.("index_bytes") || 0,
           raw_blocks: int.("raw_blocks") || 0,
           raw_bytes: int.("raw_bytes") || 0,
           compressed_blocks: int.("compressed_blocks") || 0,
           compressed_bytes: int.("compressed_bytes") || 0,
           # The *_total keys are persisted in the store's _meta (extension
           # 0.6.2) — unlike the optimize_raw_* profile counters, they
           # survive restarts, which a compression-ratio display requires.
           compression_raw_bytes_in: int.("compression_input_bytes_total") || 0,
           compression_compressed_bytes_out: int.("compression_output_bytes_total") || 0,
           compaction_count: int.("optimize_count") || 0,
           oldest_timestamp: int.("ts_min"),
           newest_timestamp: int.("ts_max")
         }}
      end

    {:reply, result, state}
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

  # Retention deletes blocks inside the extension on its own schedule, so the
  # freelist grows continuously. Returning pages in bounded batches keeps the
  # file near its true size without ever taking the long exclusive lock a full
  # VACUUM needs — a store that only ever grew reached 1.86 GB holding 813 KB.
  def handle_info(:vacuum, state) do
    pages = TimelessLogs.Config.vacuum_pages_per_turn()

    # incremental_vacuum releases pages, but in WAL mode the file only shrinks
    # once the log is checked back in. PASSIVE yields rather than waiting on
    # readers, so a busy store simply reclaims on a later turn.
    _ = LibsqlCandidate.execute(state.conn, "PRAGMA incremental_vacuum(#{pages})")
    _ = LibsqlCandidate.execute(state.conn, "PRAGMA wal_checkpoint(PASSIVE)")

    {:noreply, %{state | vacuum_timer: schedule_vacuum()}}
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

  defp schedule_vacuum do
    Process.send_after(self(), :vacuum, TimelessLogs.Config.vacuum_interval())
  end

  # -- Query path -----------------------------------------------------------

  @pagination_keys [:limit, :offset, :order, :count_total]

  # Mirrors index_keys in LibsqlCandidate's CREATE VIRTUAL TABLE. Only these
  # metadata keys exist as columns the engine can prune on.
  @index_keys TimelessLogs.LibsqlCandidate.index_keys()

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
    limit = Keyword.get(pagination, :limit, 100)
    offset = Keyword.get(pagination, :offset, 0)

    # An exact total costs a full materialisation: every matching entry has to
    # cross into Elixir just to be counted. That is linear in matches, not in
    # store size — measured at 22s for 100k matches — so a caller that only
    # needs a page can ask for one with count_total: false.
    #
    # The bound is only sound when the engine can apply every predicate itself.
    # An unindexed metadata key falls through to the shared Filter, so rows the
    # engine returned may still be dropped here and a SQL LIMIT would cut the
    # page short.
    exact? = engine_exact?(search)
    count_total? = Keyword.get(pagination, :count_total, true) or not exact?
    fetch = if count_total?, do: nil, else: offset + limit + 1

    with {:ok, rows} <- select_entries(conn, search, order, fetch) do
      matched =
        rows
        |> Enum.map(&decode_row/1)
        |> TimelessLogs.Filter.filter(search)

      entries =
        matched
        |> Enum.drop(offset)
        |> Enum.take(limit)
        |> Enum.map(&TimelessLogs.Entry.from_map/1)

      {total, has_more} =
        if count_total? do
          t = length(matched)
          {t, offset + limit < t}
        else
          # One row beyond the page was requested; getting it means there is a
          # next page. `total` is the page's own size, not a global count.
          {length(entries), length(matched) > offset + limit}
        end

      {:ok,
       %TimelessLogs.Result{
         entries: entries,
         total: total,
         limit: limit,
         offset: offset,
         has_more: has_more
       }}
    end
  end

  # True when every filter is one the engine applies exactly, so SQL LIMIT can
  # be trusted. Unindexed metadata keys are the exception: they are re-checked
  # in Elixir.
  defp engine_exact?(search) do
    Enum.all?(search, fn
      {:since, _} ->
        true

      {:until, _} ->
        true

      {:level, level} ->
        is_atom(level)

      {:message, pattern} ->
        is_binary(pattern) and pattern != ""

      {:metadata, map} ->
        is_map(map) and Enum.all?(map, &(to_string(elem(&1, 0)) in @index_keys))

      {:metadata_any, pairs} ->
        is_list(pairs) and Enum.all?(pairs, &(to_string(elem(&1, 0)) in @index_keys))

      _ ->
        false
    end)
  end

  # ts range and single-level equality prune inside the extension (block
  # ts_min/ts_max + the level partition term index); the shared Filter
  # re-checks everything, so pushdown is purely an optimization.
  defp select_entries(conn, search, order, fetch) do
    {where, params} =
      Enum.reduce(search, {[], []}, fn
        {:since, ts}, {w, p} ->
          {["ts >= ?#{length(p) + 1}" | w], p ++ [TimelessLogs.Timestamp.to_microseconds(ts)]}

        {:until, ts}, {w, p} ->
          {["ts <= ?#{length(p) + 1}" | w], p ++ [TimelessLogs.Timestamp.to_microseconds(ts)]}

        {:level, level}, {w, p} when is_atom(level) ->
          {["level = ?#{length(p) + 1}" | w], p ++ [Atom.to_string(level)]}

        # Exact case-insensitive substring, matched inside the engine. Without
        # this every message search decoded the whole store and filtered in
        # Elixir — roughly 0.8s per 200k entries, against 5ms pushed down.
        {:message, pattern}, {w, p} when is_binary(pattern) and pattern != "" ->
          {["message_contains = ?#{length(p) + 1}" | w], p ++ [pattern]}

        # Indexed metadata keys are real (hidden) columns backed by the term
        # posting lists, so equality on them prunes blocks. Unindexed keys fall
        # through to the shared Filter, which still re-checks everything.
        # normalize_filters/1 rewrites `service` and `host` into a disjunction
        # over their alias spellings. Every alias is an indexed column, so the
        # whole OR prunes inside the engine; pushing only the subset that
        # happened to be indexed would drop rows matching the others.
        {:metadata_any, pairs}, {w, p} when is_list(pairs) and pairs != [] ->
          if Enum.all?(pairs, fn {k, _} -> to_string(k) in @index_keys end) do
            {clauses, params} =
              Enum.reduce(pairs, {[], p}, fn {k, v}, {cs, ps} ->
                {["\"#{k}\" = ?#{length(ps) + 1}" | cs], ps ++ [to_string(v)]}
              end)

            {["(" <> Enum.join(Enum.reverse(clauses), " OR ") <> ")" | w], params}
          else
            {w, p}
          end

        {:metadata, map}, {w, p} when is_map(map) ->
          Enum.reduce(map, {w, p}, fn {k, v}, {w2, p2} ->
            if to_string(k) in @index_keys do
              {["\"#{k}\" = ?#{length(p2) + 1}" | w2], p2 ++ [to_string(v)]}
            else
              {w2, p2}
            end
          end)

        _other, acc ->
          acc
      end)

    where_sql = if where == [], do: "", else: " WHERE " <> Enum.join(Enum.reverse(where), " AND ")
    order_sql = if order == :desc, do: " ORDER BY ts DESC", else: " ORDER BY ts ASC"
    limit_sql = if is_integer(fetch), do: " LIMIT #{fetch}", else: ""

    LibsqlCandidate.execute(
      conn,
      "SELECT ts, level, message, metadata FROM #{@table}#{where_sql}#{order_sql}#{limit_sql}",
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

  defp stat_int(nil), do: nil
  defp stat_int(""), do: nil
  defp stat_int(v) when is_integer(v), do: v

  defp stat_int(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, _} -> n
      :error -> nil
    end
  end

  defp stat_int(_), do: nil

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

  # A data_dir carrying the legacy block-store layout is AUTO-CONVERTED
  # at startup (the legacy engine is on a ~3-month deprecation clock):
  # ReleaseStartup.prepare/2 runs the journaled, resumable, digest-
  # verified conversion under an exclusive owner lock, retaining the
  # source for rollback. Set auto_migrate: false to restore the strict
  # refusal instead. Never silently ignore existing data.
  defp maybe_auto_migrate_legacy_store!(data_dir, opts) do
    legacy? =
      File.exists?(Path.join(data_dir, "logs_index.db")) or
        File.dir?(Path.join(data_dir, "blocks"))

    migrated? = File.exists?(Path.join(data_dir, "logs.db"))

    auto? =
      Keyword.get(opts, :auto_migrate, Application.get_env(:timeless_logs, :auto_migrate, true))

    cond do
      not legacy? or migrated? ->
        :ok

      not auto? ->
        raise "timeless_logs engine: :libsql refuses to start against the unmigrated " <>
                "legacy block store in #{data_dir} — run the TimelessLogs.ReleaseMigration " <>
                "conversion, enable auto_migrate, or configure engine: :elixir"

      true ->
        Logger.warning(
          "timeless_logs: auto-converting the legacy block store in #{data_dir} to the " <>
            "libSQL engine (journaled, verified, source retained for rollback). " <>
            "Set auto_migrate: false to disable."
        )

        case TimelessLogs.ReleaseStartup.prepare(data_dir,
               extension_path: Keyword.get(opts, :extension_path)
             ) do
          {:ok, result} ->
            Logger.info(
              "timeless_logs: legacy conversion ready (state: #{inspect(result[:state])})"
            )

            :ok

          {:error, result} ->
            raise "timeless_logs: automatic legacy conversion failed: #{inspect(result)}. " <>
                    "The journaled migration is resumable — restart to resume, or set " <>
                    "engine: :elixir to keep the legacy engine."
        end
    end
  end
end
