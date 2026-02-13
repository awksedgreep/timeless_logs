defmodule TimelessLogs.Index do
  @moduledoc false

  use GenServer

  @default_limit 100
  @default_offset 0

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec index_block(TimelessLogs.Writer.block_meta(), [map()]) :: :ok
  def index_block(block_meta, entries) do
    GenServer.call(__MODULE__, {:index_block, block_meta, entries})
  end

  @spec index_block_async(TimelessLogs.Writer.block_meta(), [map()]) :: :ok
  def index_block_async(block_meta, entries) do
    GenServer.cast(__MODULE__, {:index_block, block_meta, entries})
  end

  @spec query(keyword()) :: {:ok, TimelessLogs.Result.t()}
  def query(filters) do
    # Phase 1: GenServer does the cheap SQLite lookup only
    {block_ids, storage, pagination, search_filters} =
      GenServer.call(__MODULE__, {:query_plan, filters}, TimelessLogs.Config.query_timeout())

    # Phase 2: Parallel decompression + filtering in the caller's process
    do_query_parallel(block_ids, storage, pagination, search_filters)
  end

  @spec delete_blocks_before(integer()) :: non_neg_integer()
  def delete_blocks_before(cutoff_timestamp) do
    GenServer.call(__MODULE__, {:delete_before, cutoff_timestamp}, 60_000)
  end

  @spec delete_blocks_over_size(non_neg_integer()) :: non_neg_integer()
  def delete_blocks_over_size(max_bytes) do
    GenServer.call(__MODULE__, {:delete_over_size, max_bytes}, 60_000)
  end

  @spec stats() :: {:ok, TimelessLogs.Stats.t()}
  def stats do
    GenServer.call(__MODULE__, :stats, TimelessLogs.Config.query_timeout())
  end

  @spec matching_block_ids(keyword()) :: [{integer(), String.t() | nil, :raw | :zstd}]
  def matching_block_ids(filters) do
    GenServer.call(__MODULE__, {:matching_block_ids, filters}, TimelessLogs.Config.query_timeout())
  end

  @spec read_block_data(integer()) :: {:ok, [map()]} | {:error, term()}
  def read_block_data(block_id) do
    GenServer.call(__MODULE__, {:read_block_data, block_id}, TimelessLogs.Config.query_timeout())
  end

  @spec raw_block_stats() :: %{
          entry_count: integer(),
          block_count: integer(),
          oldest_created_at: integer() | nil
        }
  def raw_block_stats do
    GenServer.call(__MODULE__, :raw_block_stats, TimelessLogs.Config.query_timeout())
  end

  @spec raw_block_ids() :: [{integer(), String.t() | nil}]
  def raw_block_ids do
    GenServer.call(__MODULE__, :raw_block_ids, TimelessLogs.Config.query_timeout())
  end

  @spec compact_blocks([integer()], TimelessLogs.Writer.block_meta(), [map()]) :: :ok
  def compact_blocks(old_block_ids, new_meta, new_entries) do
    GenServer.call(__MODULE__, {:compact_blocks, old_block_ids, new_meta, new_entries}, 60_000)
  end

  @spec backup(String.t()) :: :ok | {:error, term()}
  def backup(target_path) do
    GenServer.call(__MODULE__, {:backup, target_path}, :infinity)
  end

  # Flush pending index operations after this interval
  @index_flush_interval 100

  @impl true
  def init(opts) do
    storage = Keyword.get(opts, :storage, :disk)

    {db, db_path} =
      case storage do
        :memory ->
          {:ok, db} = Exqlite.Sqlite3.open(":memory:")
          {db, nil}

        :disk ->
          data_dir = Keyword.fetch!(opts, :data_dir)
          db_path = Path.join(data_dir, "index.db")
          {:ok, db} = Exqlite.Sqlite3.open(db_path)
          {db, db_path}
      end

    create_tables(db)

    {:ok, %{db: db, db_path: db_path, storage: storage, pending: [], flush_timer: nil}}
  end

  @impl true
  def terminate(_reason, state) do
    flush_pending(state)
    Exqlite.Sqlite3.close(state.db)
  end

  @impl true
  def handle_call({:index_block, meta, entries}, _from, state) do
    # Sync call: flush any pending ops first, then index this block immediately
    state = flush_pending(state)
    result = do_index_block(state.db, meta, entries)
    {:reply, result, state}
  end

  def handle_call({:query_plan, filters}, _from, state) do
    state = flush_pending(state)
    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :desc)
    block_ids = find_matching_blocks(state.db, term_filters, time_filters, order)
    {:reply, {block_ids, state.storage, pagination, search_filters}, state}
  end

  @impl true
  def handle_call({:delete_before, cutoff}, _from, state) do
    state = flush_pending(state)
    count = do_delete_before(state.db, cutoff, state.storage)
    {:reply, count, state}
  end

  @impl true
  def handle_call({:delete_over_size, max_bytes}, _from, state) do
    state = flush_pending(state)
    count = do_delete_over_size(state.db, max_bytes, state.storage)
    {:reply, count, state}
  end

  @impl true
  def handle_call({:matching_block_ids, filters}, _from, state) do
    state = flush_pending(state)
    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :asc)
    block_ids = find_matching_blocks(state.db, term_filters, time_filters, order)
    {:reply, block_ids, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    state = flush_pending(state)
    result = do_stats(state.db, state.db_path)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:read_block_data, block_id}, _from, state) do
    state = flush_pending(state)
    result = read_block_from_db(state.db, block_id)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:raw_block_stats, _from, state) do
    state = flush_pending(state)
    result = do_raw_block_stats(state.db)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:raw_block_ids, _from, state) do
    state = flush_pending(state)
    result = do_raw_block_ids(state.db)
    {:reply, result, state}
  end

  def handle_call({:compact_blocks, old_ids, new_meta, new_entries}, _from, state) do
    state = flush_pending(state)
    result = do_compact_blocks(state.db, old_ids, new_meta, new_entries, state.storage)
    {:reply, result, state}
  end

  def handle_call({:backup, target_path}, _from, state) do
    state = flush_pending(state)
    {:ok, stmt} = Exqlite.Sqlite3.prepare(state.db, "VACUUM INTO ?1")
    Exqlite.Sqlite3.bind(stmt, [target_path])
    result = Exqlite.Sqlite3.step(state.db, stmt)
    Exqlite.Sqlite3.release(state.db, stmt)

    case result do
      :done -> {:reply, :ok, state}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  @impl true
  def handle_cast({:index_block, meta, entries}, state) do
    pending = [{meta, entries} | state.pending]
    state = schedule_index_flush(%{state | pending: pending})
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush_index, state) do
    state = %{state | flush_timer: nil}
    state = flush_pending(state)
    {:noreply, state}
  end

  defp do_stats(db, db_path) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT
        COUNT(*),
        COALESCE(SUM(entry_count), 0),
        COALESCE(SUM(byte_size), 0),
        MIN(ts_min),
        MAX(ts_max)
      FROM blocks
      """)

    {:row, [total_blocks, total_entries, total_bytes, oldest, newest]} =
      Exqlite.Sqlite3.step(db, stmt)

    Exqlite.Sqlite3.release(db, stmt)

    {:ok, fmt_stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT format, COUNT(*), COALESCE(SUM(byte_size), 0), COALESCE(SUM(entry_count), 0)
      FROM blocks GROUP BY format
      """)

    format_stats = collect_format_stats(db, fmt_stmt)
    Exqlite.Sqlite3.release(db, fmt_stmt)

    index_size =
      case db_path do
        nil ->
          0

        path ->
          case File.stat(path) do
            {:ok, %{size: size}} -> size
            _ -> 0
          end
      end

    {:ok,
     %TimelessLogs.Stats{
       total_blocks: total_blocks,
       total_entries: total_entries,
       total_bytes: total_bytes,
       oldest_timestamp: oldest,
       newest_timestamp: newest,
       disk_size: total_bytes,
       index_size: index_size,
       raw_blocks: format_stats["raw"][:blocks] || 0,
       raw_bytes: format_stats["raw"][:bytes] || 0,
       raw_entries: format_stats["raw"][:entries] || 0,
       zstd_blocks: format_stats["zstd"][:blocks] || 0,
       zstd_bytes: format_stats["zstd"][:bytes] || 0,
       zstd_entries: format_stats["zstd"][:entries] || 0
     }}
  end

  defp collect_format_stats(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [format, count, bytes, entries]} ->
        Map.put(collect_format_stats(db, stmt), format, %{
          blocks: count,
          bytes: bytes,
          entries: entries
        })

      :done ->
        %{}
    end
  end

  defp create_tables(db) do
    Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode=WAL")
    Exqlite.Sqlite3.execute(db, "PRAGMA synchronous=NORMAL")

    Exqlite.Sqlite3.execute(db, """
    CREATE TABLE IF NOT EXISTS blocks (
      block_id INTEGER PRIMARY KEY,
      file_path TEXT,
      byte_size INTEGER NOT NULL,
      entry_count INTEGER NOT NULL,
      ts_min INTEGER NOT NULL,
      ts_max INTEGER NOT NULL,
      data BLOB,
      format TEXT NOT NULL DEFAULT 'zstd',
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    )
    """)

    # Migration: add format column to existing databases
    migrate_add_format(db)

    Exqlite.Sqlite3.execute(db, """
    CREATE TABLE IF NOT EXISTS block_terms (
      term TEXT NOT NULL,
      block_id INTEGER NOT NULL REFERENCES blocks(block_id),
      PRIMARY KEY (term, block_id)
    ) WITHOUT ROWID
    """)

    Exqlite.Sqlite3.execute(db, """
    CREATE INDEX IF NOT EXISTS idx_blocks_ts ON blocks(ts_min, ts_max)
    """)
  end

  defp migrate_add_format(db) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "PRAGMA table_info(blocks)")
    columns = collect_column_names(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    unless "format" in columns do
      Exqlite.Sqlite3.execute(
        db,
        "ALTER TABLE blocks ADD COLUMN format TEXT NOT NULL DEFAULT 'zstd'"
      )
    end
  end

  defp collect_column_names(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [_cid, name | _rest]} -> [name | collect_column_names(db, stmt)]
      :done -> []
    end
  end

  defp flush_pending(%{pending: []} = state), do: state

  defp flush_pending(%{pending: pending, db: db} = state) do
    Exqlite.Sqlite3.execute(db, "BEGIN")

    for {meta, entries} <- Enum.reverse(pending) do
      index_block_inner(db, meta, entries)
    end

    Exqlite.Sqlite3.execute(db, "COMMIT")

    if state.flush_timer do
      Process.cancel_timer(state.flush_timer)
    end

    %{state | pending: [], flush_timer: nil}
  end

  defp schedule_index_flush(%{flush_timer: nil} = state) do
    ref = Process.send_after(self(), :flush_index, @index_flush_interval)
    %{state | flush_timer: ref}
  end

  defp schedule_index_flush(state), do: state

  defp do_index_block(db, meta, entries) do
    Exqlite.Sqlite3.execute(db, "BEGIN")
    index_block_inner(db, meta, entries)
    Exqlite.Sqlite3.execute(db, "COMMIT")
    :ok
  end

  # Index a single block's metadata + terms (caller manages transaction)
  defp index_block_inner(db, meta, entries) do
    format = Map.get(meta, :format, :zstd)
    format_str = Atom.to_string(format)

    {:ok, block_stmt} =
      Exqlite.Sqlite3.prepare(db, """
      INSERT INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, data, format)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
      """)

    Exqlite.Sqlite3.bind(block_stmt, [
      meta.block_id,
      meta[:file_path],
      meta.byte_size,
      meta.entry_count,
      meta.ts_min,
      meta.ts_max,
      meta[:data],
      format_str
    ])

    Exqlite.Sqlite3.step(db, block_stmt)
    Exqlite.Sqlite3.release(db, block_stmt)

    terms = extract_terms(entries)
    insert_terms_batch(db, terms, meta.block_id)
  end

  # Batch INSERT up to 400 terms per statement (800 params, well under SQLite's 999 limit)
  @terms_batch_size 400

  defp insert_terms_batch(_db, [], _block_id), do: :ok

  defp insert_terms_batch(db, terms, block_id) do
    terms
    |> Enum.chunk_every(@terms_batch_size)
    |> Enum.each(fn batch ->
      n = length(batch)

      placeholders =
        Enum.map_join(1..n, ", ", fn i ->
          "(?#{i * 2 - 1}, ?#{i * 2})"
        end)

      sql = "INSERT OR IGNORE INTO block_terms (term, block_id) VALUES #{placeholders}"
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, sql)
      params = Enum.flat_map(batch, fn term -> [term, block_id] end)
      Exqlite.Sqlite3.bind(stmt, params)
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)
    end)
  end

  defp extract_terms(entries) do
    entries
    |> Enum.flat_map(fn entry ->
      level_term = "level:#{entry.level}"

      metadata_terms =
        Enum.map(entry.metadata, fn {k, v} -> "#{k}:#{v}" end)

      [level_term | metadata_terms]
    end)
    |> Enum.uniq()
  end

  defp do_query_parallel(block_ids, storage, pagination, search_filters) do
    start_time = System.monotonic_time()

    limit = Keyword.get(pagination, :limit, @default_limit)
    offset = Keyword.get(pagination, :offset, @default_offset)
    order = Keyword.get(pagination, :order, :desc)
    blocks_read = length(block_ids)

    # Parallel block decompression + filtering across cores
    all_matching =
      if storage == :disk and blocks_read > 1 do
        block_ids
        |> Task.async_stream(
          fn {_block_id, file_path, format} ->
            format_atom = to_format_atom(format)

            case TimelessLogs.Writer.read_block(file_path, format_atom) do
              {:ok, entries} ->
                entries
                |> TimelessLogs.Filter.filter(search_filters)
                |> Enum.map(&TimelessLogs.Entry.from_map/1)

              {:error, reason} ->
                TimelessLogs.Telemetry.event(
                  [:timeless_logs, :block, :error],
                  %{},
                  %{file_path: file_path, reason: reason}
                )

                []
            end
          end,
          max_concurrency: System.schedulers_online(),
          ordered: false
        )
        |> Enum.flat_map(fn {:ok, entries} -> entries end)
      else
        # Memory storage or single block — sequential (memory blocks need GenServer)
        Enum.flat_map(block_ids, fn {block_id, file_path, format} ->
          format_atom = to_format_atom(format)

          read_result =
            case storage do
              :disk -> TimelessLogs.Writer.read_block(file_path, format_atom)
              :memory -> read_block_data(block_id)
            end

          case read_result do
            {:ok, entries} ->
              entries
              |> TimelessLogs.Filter.filter(search_filters)
              |> Enum.map(&TimelessLogs.Entry.from_map/1)

            {:error, reason} ->
              TimelessLogs.Telemetry.event(
                [:timeless_logs, :block, :error],
                %{},
                %{file_path: file_path, reason: reason}
              )

              []
          end
        end)
      end

    sorted =
      case order do
        :asc -> Enum.sort_by(all_matching, & &1.timestamp, :asc)
        :desc -> Enum.sort_by(all_matching, & &1.timestamp, :desc)
      end

    total = length(sorted)
    page = sorted |> Enum.drop(offset) |> Enum.take(limit)
    duration = System.monotonic_time() - start_time

    TimelessLogs.Telemetry.event(
      [:timeless_logs, :query, :stop],
      %{duration: duration, total: total, blocks_read: blocks_read},
      %{filters: search_filters}
    )

    {:ok,
     %TimelessLogs.Result{
       entries: page,
       total: total,
       limit: limit,
       offset: offset
     }}
  end

  defp read_block_from_db(db, block_id) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "SELECT data, format FROM blocks WHERE block_id = ?1")

    Exqlite.Sqlite3.bind(stmt, [block_id])

    result =
      case Exqlite.Sqlite3.step(db, stmt) do
        {:row, [data, format]} when is_binary(data) ->
          TimelessLogs.Writer.decompress_block(data, to_format_atom(format))

        {:row, [nil, _format]} ->
          {:error, :no_data}

        :done ->
          {:error, :not_found}
      end

    Exqlite.Sqlite3.release(db, stmt)
    result
  end

  defp do_raw_block_stats(db) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT
        COALESCE(SUM(entry_count), 0),
        COUNT(*),
        MIN(created_at)
      FROM blocks WHERE format = 'raw'
      """)

    {:row, [entry_count, block_count, oldest]} = Exqlite.Sqlite3.step(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    %{entry_count: entry_count, block_count: block_count, oldest_created_at: oldest}
  end

  defp do_raw_block_ids(db) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT block_id, file_path FROM blocks WHERE format = 'raw' ORDER BY ts_min ASC
      """)

    rows = collect_rows_2(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
    rows
  end

  defp do_compact_blocks(db, old_ids, new_meta, new_entries, storage) do
    # Collect file paths of old blocks before deleting (for disk cleanup)
    old_file_paths =
      if storage == :disk do
        Enum.flat_map(old_ids, fn id ->
          {:ok, stmt} =
            Exqlite.Sqlite3.prepare(db, "SELECT file_path FROM blocks WHERE block_id = ?1")

          Exqlite.Sqlite3.bind(stmt, [id])

          result =
            case Exqlite.Sqlite3.step(db, stmt) do
              {:row, [path]} when is_binary(path) -> [path]
              _ -> []
            end

          Exqlite.Sqlite3.release(db, stmt)
          result
        end)
      else
        []
      end

    # Atomic swap: delete old blocks, insert new compressed block
    format_str = Atom.to_string(Map.get(new_meta, :format, :zstd))

    Exqlite.Sqlite3.execute(db, "BEGIN")

    # Delete old block terms and blocks
    for id <- old_ids do
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM block_terms WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM blocks WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)
    end

    # Insert new compressed block
    {:ok, block_stmt} =
      Exqlite.Sqlite3.prepare(db, """
      INSERT INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, data, format)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
      """)

    Exqlite.Sqlite3.bind(block_stmt, [
      new_meta.block_id,
      new_meta[:file_path],
      new_meta.byte_size,
      new_meta.entry_count,
      new_meta.ts_min,
      new_meta.ts_max,
      new_meta[:data],
      format_str
    ])

    Exqlite.Sqlite3.step(db, block_stmt)
    Exqlite.Sqlite3.release(db, block_stmt)

    # Insert terms for new block
    terms = extract_terms(new_entries)
    insert_terms_batch(db, terms, new_meta.block_id)

    Exqlite.Sqlite3.execute(db, "COMMIT")

    # Clean up old files after successful commit
    for path <- old_file_paths, do: File.rm(path)

    :ok
  end

  defp split_pagination(filters) do
    {pagination, search} =
      Enum.split_with(filters, fn {k, _v} -> k in [:limit, :offset, :order] end)

    {search, pagination}
  end

  defp split_filters(filters) do
    term_filters =
      filters
      |> Enum.filter(fn {k, _v} -> k in [:level, :metadata] end)

    time_filters =
      filters
      |> Enum.filter(fn {k, _v} -> k in [:since, :until] end)

    {term_filters, time_filters}
  end

  defp find_matching_blocks(db, term_filters, time_filters, order) do
    terms = build_query_terms(term_filters)

    {where_clauses, params} = build_where(terms, time_filters)

    order_dir = if order == :asc, do: "ASC", else: "DESC"

    sql =
      if where_clauses == [] do
        "SELECT block_id, file_path, format FROM blocks ORDER BY ts_min #{order_dir}"
      else
        """
        SELECT DISTINCT b.block_id, b.file_path, b.format FROM blocks b
        #{if terms != [], do: "JOIN block_terms bt ON b.block_id = bt.block_id", else: ""}
        WHERE #{Enum.join(where_clauses, " AND ")}
        ORDER BY b.ts_min #{order_dir}
        """
      end

    {:ok, stmt} = Exqlite.Sqlite3.prepare(db, sql)

    if params != [] do
      Exqlite.Sqlite3.bind(stmt, params)
    end

    rows = collect_rows_3(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
    rows
  end

  defp build_query_terms(term_filters) do
    Enum.flat_map(term_filters, fn
      {:level, level} -> ["level:#{level}"]
      {:metadata, map} -> Enum.map(map, fn {k, v} -> "#{k}:#{v}" end)
      _ -> []
    end)
  end

  defp build_where(terms, time_filters) do
    {term_clauses, term_params} =
      case terms do
        [] ->
          {[], []}

        terms ->
          placeholders = Enum.map_join(1..length(terms), ", ", &"?#{&1}")
          {["bt.term IN (#{placeholders})"], terms}
      end

    {time_clauses, time_params} =
      time_filters
      |> Enum.reduce({[], []}, fn
        {:since, ts}, {clauses, params} ->
          idx = length(term_params) + length(params) + 1
          {["b.ts_max >= ?#{idx}" | clauses], params ++ [to_unix(ts)]}

        {:until, ts}, {clauses, params} ->
          idx = length(term_params) + length(params) + 1
          {["b.ts_min <= ?#{idx}" | clauses], params ++ [to_unix(ts)]}
      end)

    {term_clauses ++ time_clauses, term_params ++ time_params}
  end

  defp to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp to_unix(ts) when is_integer(ts), do: ts

  defp to_format_atom("raw"), do: :raw
  defp to_format_atom("zstd"), do: :zstd
  defp to_format_atom(:raw), do: :raw
  defp to_format_atom(:zstd), do: :zstd
  defp to_format_atom(_), do: :zstd

  # 2-element tuple rows (used by raw_block_ids, delete helpers)
  defp collect_rows_2(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path]} -> [{block_id, file_path} | collect_rows_2(db, stmt)]
      :done -> []
    end
  end

  # 3-element tuple rows (used by find_matching_blocks)
  defp collect_rows_3(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path, format]} ->
        [{block_id, file_path, format} | collect_rows_3(db, stmt)]

      :done ->
        []
    end
  end

  defp do_delete_before(db, cutoff_timestamp, storage) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "SELECT block_id, file_path FROM blocks WHERE ts_max < ?1")

    Exqlite.Sqlite3.bind(stmt, [cutoff_timestamp])
    blocks = collect_rows_2(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    delete_blocks(db, blocks, storage)
  end

  defp do_delete_over_size(db, max_bytes, storage) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "SELECT COALESCE(SUM(byte_size), 0) FROM blocks")
    {:row, [total]} = Exqlite.Sqlite3.step(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    if total <= max_bytes do
      0
    else
      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(
          db,
          "SELECT block_id, file_path, byte_size FROM blocks ORDER BY ts_min ASC"
        )

      rows = collect_rows_with_size(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {to_delete, _} =
        Enum.reduce_while(rows, {[], total}, fn {block_id, file_path, size}, {acc, remaining} ->
          if remaining > max_bytes do
            {:cont, {[{block_id, file_path} | acc], remaining - size}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      delete_blocks(db, to_delete, storage)
    end
  end

  defp delete_blocks(_db, [], _storage), do: 0

  defp delete_blocks(db, blocks, storage) do
    Exqlite.Sqlite3.execute(db, "BEGIN")

    for {block_id, file_path} <- blocks do
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM block_terms WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [block_id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM blocks WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [block_id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      if storage == :disk do
        File.rm(file_path)
      end
    end

    Exqlite.Sqlite3.execute(db, "COMMIT")
    length(blocks)
  end

  defp collect_rows_with_size(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path, size]} ->
        [{block_id, file_path, size} | collect_rows_with_size(db, stmt)]

      :done ->
        []
    end
  end
end
