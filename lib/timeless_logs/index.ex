defmodule TimelessLogs.Index do
  @moduledoc false

  use GenServer

  @default_limit 100
  @default_offset 0

  # Flush pending index operations after this interval
  @index_flush_interval 100

  # Batch INSERT up to 400 terms per statement (800 params, under SQLite's 999 limit)
  @terms_batch_size 400

  @blocks_table :timeless_logs_blocks
  @overflow_table :timeless_logs_index_overflow

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec index_block(TimelessLogs.Writer.block_meta(), [map()]) :: :ok
  def index_block(block_meta, entries) do
    terms = extract_terms(entries)
    GenServer.call(__MODULE__, {:index_block, block_meta, entries, terms})
  end

  @spec index_block_async(TimelessLogs.Writer.block_meta(), [map()]) :: :ok
  def index_block_async(block_meta, entries) do
    GenServer.cast(__MODULE__, {:index_block_raw, block_meta, entries})
  end

  # --- Lock-free read functions (run in caller's process) ---

  @spec query(keyword()) :: {:ok, TimelessLogs.Result.t()}
  def query(filters) do
    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :desc)
    block_ids = find_matching_blocks_ets(term_filters, time_filters, order)
    storage = :persistent_term.get({__MODULE__, :storage})

    do_query_parallel(block_ids, storage, pagination, search_filters)
  end

  @spec stats() :: {:ok, TimelessLogs.Stats.t()}
  def stats do
    db_path = :persistent_term.get({__MODULE__, :db_path})

    rows = :ets.tab2list(@blocks_table)

    {total_blocks, total_entries, total_bytes, oldest, newest, format_stats} =
      Enum.reduce(
        rows,
        {0, 0, 0, nil, nil, %{}},
        fn {_bid, _fp, byte_size, entry_count, ts_min, ts_max, format, _created_at},
           {blocks, entries, bytes, old, new, fstats} ->
          new_old = if old == nil or ts_min < old, do: ts_min, else: old
          new_new = if new == nil or ts_max > new, do: ts_max, else: new

          fmt_key = Atom.to_string(format)
          cur = Map.get(fstats, fmt_key, %{blocks: 0, bytes: 0, entries: 0})

          updated = %{
            blocks: cur.blocks + 1,
            bytes: cur.bytes + byte_size,
            entries: cur.entries + entry_count
          }

          {blocks + 1, entries + entry_count, bytes + byte_size, new_old, new_new,
           Map.put(fstats, fmt_key, updated)}
        end
      )

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
       zstd_entries: format_stats["zstd"][:entries] || 0,
       openzl_blocks: format_stats["openzl"][:blocks] || 0,
       openzl_bytes: format_stats["openzl"][:bytes] || 0,
       openzl_entries: format_stats["openzl"][:entries] || 0
     }}
  end

  @spec matching_block_ids(keyword()) :: [{integer(), String.t() | nil, :raw | :zstd}]
  def matching_block_ids(filters) do
    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :asc)
    find_matching_blocks_ets(term_filters, time_filters, order)
  end

  @spec raw_block_stats() :: %{
          entry_count: integer(),
          block_count: integer(),
          oldest_created_at: integer() | nil
        }
  def raw_block_stats do
    rows = :ets.tab2list(@blocks_table)

    Enum.reduce(
      rows,
      %{entry_count: 0, block_count: 0, oldest_created_at: nil},
      fn {_bid, _fp, _bs, entry_count, _tsmin, _tsmax, format, created_at}, acc ->
        if format == :raw do
          oldest =
            if acc.oldest_created_at == nil or created_at < acc.oldest_created_at,
              do: created_at,
              else: acc.oldest_created_at

          %{
            entry_count: acc.entry_count + entry_count,
            block_count: acc.block_count + 1,
            oldest_created_at: oldest
          }
        else
          acc
        end
      end
    )
  end

  @spec raw_block_ids() :: [{integer(), String.t() | nil}]
  def raw_block_ids do
    @blocks_table
    |> :ets.tab2list()
    |> Enum.filter(fn {_bid, _fp, _bs, _ec, _tsmin, _tsmax, format, _ca} -> format == :raw end)
    |> Enum.sort_by(fn {_bid, _fp, _bs, _ec, ts_min, _tsmax, _fmt, _ca} -> ts_min end)
    |> Enum.map(fn {bid, fp, _bs, _ec, _tsmin, _tsmax, _fmt, _ca} -> {bid, fp} end)
  end

  # read_block_data stays as GenServer.call — reads BLOB from SQLite (memory mode only)
  @spec read_block_data(integer()) :: {:ok, [map()]} | {:error, term()}
  def read_block_data(block_id) do
    GenServer.call(
      __MODULE__,
      {:read_block_data, block_id},
      TimelessLogs.Config.query_timeout()
    )
  end

  @spec delete_blocks_before(integer()) :: non_neg_integer()
  def delete_blocks_before(cutoff_timestamp) do
    GenServer.call(__MODULE__, {:delete_before, cutoff_timestamp}, 60_000)
  end

  @spec delete_blocks_over_size(non_neg_integer()) :: non_neg_integer()
  def delete_blocks_over_size(max_bytes) do
    GenServer.call(__MODULE__, {:delete_over_size, max_bytes}, 60_000)
  end

  @spec compact_blocks([integer()], [{TimelessLogs.Writer.block_meta(), [map()]}]) :: :ok
  def compact_blocks(old_block_ids, meta_chunk_pairs) do
    new_terms_list =
      Enum.map(meta_chunk_pairs, fn {meta, entries} ->
        {meta, entries, extract_terms(entries)}
      end)

    GenServer.call(
      __MODULE__,
      {:compact_blocks, old_block_ids, new_terms_list},
      60_000
    )
  end

  @spec backup(String.t()) :: :ok | {:error, term()}
  def backup(target_path) do
    GenServer.call(__MODULE__, {:backup, target_path}, :infinity)
  end

  @spec sync() :: :ok
  def sync, do: GenServer.call(__MODULE__, :sync, TimelessLogs.Config.query_timeout())

  # --- GenServer callbacks ---

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

    # Cache prepared statements for hot-path inserts
    {:ok, block_insert_stmt} =
      Exqlite.Sqlite3.prepare(db, """
      INSERT INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, data, format)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
      """)

    {:ok, terms_insert_stmt} = Exqlite.Sqlite3.prepare(db, build_terms_sql(@terms_batch_size))

    # Initialize ETS tables
    init_ets_tables()

    # Store storage mode + db_path in persistent_term
    :persistent_term.put({__MODULE__, :storage}, storage)
    :persistent_term.put({__MODULE__, :db_path}, db_path)

    # Bulk-load from SQLite into ETS/persistent_term
    bulk_load_blocks(db)
    bulk_load_term_index(db)

    # Schedule publish timer
    publish_interval = TimelessLogs.Config.index_publish_interval()
    publish_ref = Process.send_after(self(), :publish, publish_interval)

    {:ok,
     %{
       db: db,
       db_path: db_path,
       storage: storage,
       pending: [],
       flush_timer: nil,
       block_insert_stmt: block_insert_stmt,
       terms_insert_stmt: terms_insert_stmt,
       terms_stmt_cache: %{@terms_batch_size => terms_insert_stmt},
       publish_timer: publish_ref,
       publish_interval: publish_interval,
       dirty: false
     }}
  end

  @impl true
  def terminate(_reason, state) do
    state = flush_pending(state)

    Exqlite.Sqlite3.release(state.db, state.block_insert_stmt)

    for {_n, stmt} <- state.terms_stmt_cache do
      Exqlite.Sqlite3.release(state.db, stmt)
    end

    Exqlite.Sqlite3.close(state.db)

    # Clean up persistent_term keys
    :persistent_term.erase({__MODULE__, :storage})
    :persistent_term.erase({__MODULE__, :db_path})
    :persistent_term.erase({__MODULE__, :term_index})

    # Clean up ETS tables
    safe_delete_ets(@blocks_table)
    safe_delete_ets(@overflow_table)
  end

  # --- handle_call (grouped) ---

  @impl true
  def handle_call({:index_block, meta, entries, terms}, _from, state) do
    state = flush_pending(state)
    result = do_index_block(state, meta, entries, terms)
    state = collect_stmt_cache(state)
    # Update cache after SQLite commit
    insert_block_ets(meta)
    insert_overflow(terms, meta.block_id)
    {:reply, result, %{state | dirty: true}}
  end

  def handle_call({:delete_before, cutoff}, _from, state) do
    state = flush_pending(state)

    {count, deleted_ids, old_terms} =
      do_delete_before(state.db, cutoff, state.storage)

    remove_blocks_from_cache(deleted_ids, old_terms)
    {:reply, count, %{state | dirty: false}}
  end

  def handle_call({:delete_over_size, max_bytes}, _from, state) do
    state = flush_pending(state)

    {count, deleted_ids, old_terms} =
      do_delete_over_size(state.db, max_bytes, state.storage)

    remove_blocks_from_cache(deleted_ids, old_terms)
    {:reply, count, %{state | dirty: false}}
  end

  def handle_call({:read_block_data, block_id}, _from, state) do
    state = flush_pending(state)
    result = read_block_from_db(state.db, block_id)
    {:reply, result, state}
  end

  def handle_call({:compact_blocks, old_ids, new_terms_list}, _from, state) do
    state = flush_pending(state)

    # Collect old terms for cache cleanup BEFORE SQLite delete
    old_terms = collect_terms_for_blocks(state.db, old_ids)

    result = do_compact_blocks(state, old_ids, new_terms_list)
    state = collect_stmt_cache(state)

    # Update cache: remove old, add new
    for id <- old_ids, do: :ets.delete(@blocks_table, id)
    remove_block_ids_from_persistent_term(old_ids, old_terms)
    clear_overflow_for_blocks(old_ids)

    for {meta, _entries, terms} <- new_terms_list do
      insert_block_ets(meta)
      insert_overflow(terms, meta.block_id)
    end

    # Force immediate publish since compaction changes are significant
    publish_overflow()

    {:reply, result, %{state | dirty: false}}
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

  def handle_call(:sync, _from, state) do
    state = flush_pending(state)
    publish_overflow()
    {:reply, :ok, %{state | dirty: false}}
  end

  # --- handle_cast ---

  @impl true
  def handle_cast({:index_block, meta, entries, terms}, state) do
    pending = [{meta, entries, terms} | state.pending]
    state = schedule_index_flush(%{state | pending: pending})
    {:noreply, state}
  end

  def handle_cast({:index_block_raw, meta, entries}, state) do
    pending = [{:raw, meta, entries} | state.pending]
    state = schedule_index_flush(%{state | pending: pending})
    {:noreply, state}
  end

  # --- handle_info ---

  @impl true
  def handle_info(:flush_index, state) do
    state = %{state | flush_timer: nil}
    state = flush_pending(state)
    {:noreply, state}
  end

  def handle_info(:publish, state) do
    if state.dirty do
      publish_overflow()
    end

    ref = Process.send_after(self(), :publish, state.publish_interval)
    {:noreply, %{state | publish_timer: ref, dirty: false}}
  end

  # --- ETS/persistent_term initialization ---

  defp init_ets_tables do
    safe_delete_ets(@blocks_table)
    safe_delete_ets(@overflow_table)

    :ets.new(@blocks_table, [
      :named_table,
      :ordered_set,
      :public,
      read_concurrency: true
    ])

    :ets.new(@overflow_table, [
      :named_table,
      :duplicate_bag,
      :public,
      read_concurrency: true
    ])

    :persistent_term.put({__MODULE__, :term_index}, %{})
  end

  defp safe_delete_ets(table) do
    try do
      :ets.delete(table)
    rescue
      ArgumentError -> :ok
    end
  end

  # --- Bulk loading from SQLite ---

  defp bulk_load_blocks(db) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT block_id, file_path, byte_size, entry_count, ts_min, ts_max, format, created_at
      FROM blocks
      """)

    bulk_load_blocks_rows(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
  end

  defp bulk_load_blocks_rows(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path, byte_size, entry_count, ts_min, ts_max, format, created_at]} ->
        :ets.insert(@blocks_table, {
          block_id,
          file_path,
          byte_size,
          entry_count,
          ts_min,
          ts_max,
          to_format_atom(format),
          created_at
        })

        bulk_load_blocks_rows(db, stmt)

      :done ->
        :ok
    end
  end

  defp bulk_load_term_index(db) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "SELECT term, block_id FROM block_terms")

    term_map = bulk_load_term_rows(db, stmt, %{})
    Exqlite.Sqlite3.release(db, stmt)
    :persistent_term.put({__MODULE__, :term_index}, term_map)
  end

  defp bulk_load_term_rows(db, stmt, acc) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [term, block_id]} ->
        updated =
          Map.update(acc, term, MapSet.new([block_id]), &MapSet.put(&1, block_id))

        bulk_load_term_rows(db, stmt, updated)

      :done ->
        acc
    end
  end

  # --- Cache update helpers ---

  defp insert_block_ets(meta) do
    format = Map.get(meta, :format, :zstd)
    created_at = System.system_time(:second)

    :ets.insert(@blocks_table, {
      meta.block_id,
      meta[:file_path],
      meta.byte_size,
      meta.entry_count,
      meta.ts_min,
      meta.ts_max,
      format,
      created_at
    })
  end

  defp insert_overflow(terms, block_id) do
    if terms != [] do
      term_objects = Enum.map(terms, fn term -> {:term, term, block_id} end)
      :ets.insert(@overflow_table, term_objects)
    end
  end

  # --- Lock-free read helpers ---

  defp term_block_ids(term) do
    pt_set = Map.get(:persistent_term.get({__MODULE__, :term_index}), term, MapSet.new())

    overflow_ids =
      @overflow_table
      |> :ets.match({:term, term, :"$1"})
      |> List.flatten()
      |> MapSet.new()

    MapSet.union(pt_set, overflow_ids)
  end

  defp find_matching_blocks_ets(term_filters, time_filters, order) do
    terms = build_query_terms(term_filters)

    candidate_bids =
      case terms do
        [] ->
          nil

        _ ->
          terms
          |> Enum.map(&term_block_ids/1)
          |> Enum.reduce(&MapSet.intersection/2)
      end

    # Collect all blocks from ETS, apply filters
    all_blocks = :ets.tab2list(@blocks_table)

    all_blocks
    |> Enum.filter(fn {bid, _fp, _bs, _ec, ts_min, ts_max, _fmt, _ca} ->
      term_match =
        case candidate_bids do
          nil -> true
          bids -> MapSet.member?(bids, bid)
        end

      time_match =
        Enum.all?(time_filters, fn
          {:since, ts} -> ts_max >= to_unix(ts)
          {:until, ts} -> ts_min <= to_unix(ts)
        end)

      term_match and time_match
    end)
    |> Enum.sort_by(
      fn {_bid, _fp, _bs, _ec, ts_min, _tsmax, _fmt, _ca} -> ts_min end,
      if(order == :asc, do: :asc, else: :desc)
    )
    |> Enum.map(fn {bid, fp, _bs, _ec, _tsmin, _tsmax, fmt, _ca} -> {bid, fp, fmt} end)
  end

  # --- Publish: merge overflow into persistent_term ---

  defp publish_overflow do
    overflow_entries = :ets.tab2list(@overflow_table)

    if overflow_entries != [] do
      term_index = :persistent_term.get({__MODULE__, :term_index})

      new_term_index =
        Enum.reduce(overflow_entries, term_index, fn
          {:term, term, block_id}, ti ->
            Map.update(ti, term, MapSet.new([block_id]), &MapSet.put(&1, block_id))
        end)

      :persistent_term.put({__MODULE__, :term_index}, new_term_index)
      :ets.delete_all_objects(@overflow_table)
    end
  end

  # --- Cache cleanup helpers ---

  defp collect_terms_for_blocks(db, block_ids) do
    Enum.flat_map(block_ids, fn id ->
      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(db, "SELECT term FROM block_terms WHERE block_id = ?1")

      Exqlite.Sqlite3.bind(stmt, [id])
      terms = collect_single_column(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)
      Enum.map(terms, fn term -> {term, id} end)
    end)
  end

  defp remove_block_ids_from_persistent_term(old_ids, old_terms) do
    old_id_set = MapSet.new(old_ids)

    term_index = :persistent_term.get({__MODULE__, :term_index})

    new_term_index =
      old_terms
      |> Enum.map(fn {term, _id} -> term end)
      |> Enum.uniq()
      |> Enum.reduce(term_index, fn term, acc ->
        case Map.get(acc, term) do
          nil ->
            acc

          bids ->
            cleaned = MapSet.difference(bids, old_id_set)

            if MapSet.size(cleaned) == 0 do
              Map.delete(acc, term)
            else
              Map.put(acc, term, cleaned)
            end
        end
      end)

    :persistent_term.put({__MODULE__, :term_index}, new_term_index)
  end

  defp clear_overflow_for_blocks(old_ids) do
    old_id_set = MapSet.new(old_ids)

    @overflow_table
    |> :ets.tab2list()
    |> Enum.each(fn {_type, _key, bid} = entry ->
      if MapSet.member?(old_id_set, bid) do
        :ets.delete_object(@overflow_table, entry)
      end
    end)
  end

  defp remove_blocks_from_cache(deleted_ids, old_terms) do
    if deleted_ids != [] do
      for id <- deleted_ids, do: :ets.delete(@blocks_table, id)
      remove_block_ids_from_persistent_term(deleted_ids, old_terms)
      clear_overflow_for_blocks(deleted_ids)
    end
  end

  # --- Pending flush helpers ---

  defp flush_pending(%{pending: []} = state), do: state

  defp flush_pending(%{pending: pending} = state) do
    # Precompute any raw entries
    resolved =
      pending
      |> Enum.reverse()
      |> Enum.map(fn
        {:raw, meta, entries} ->
          terms = extract_terms(entries)
          {meta, entries, terms}

        {meta, entries, terms} ->
          {meta, entries, terms}
      end)

    Exqlite.Sqlite3.execute(state.db, "BEGIN")

    for {meta, entries, terms} <- resolved do
      insert_block_data(state, meta, entries, terms)
    end

    Exqlite.Sqlite3.execute(state.db, "COMMIT")

    # Update ETS + overflow for all flushed entries
    for {meta, _entries, terms} <- resolved do
      insert_block_ets(meta)
      insert_overflow(terms, meta.block_id)
    end

    if state.flush_timer do
      Process.cancel_timer(state.flush_timer)
    end

    state = collect_stmt_cache(state)
    %{state | pending: [], flush_timer: nil, dirty: true}
  end

  defp collect_stmt_cache(state) do
    {new_terms} =
      Process.get_keys()
      |> Enum.reduce({state.terms_stmt_cache}, fn
        {:terms_stmt_cache, n}, {tc} ->
          stmt = Process.delete({:terms_stmt_cache, n})
          {Map.put_new(tc, n, stmt)}

        _, acc ->
          acc
      end)

    %{state | terms_stmt_cache: new_terms}
  end

  defp schedule_index_flush(%{flush_timer: nil} = state) do
    ref = Process.send_after(self(), :flush_index, @index_flush_interval)
    %{state | flush_timer: ref}
  end

  defp schedule_index_flush(state), do: state

  # --- Table creation ---

  defp create_tables(db) do
    Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode=WAL")
    Exqlite.Sqlite3.execute(db, "PRAGMA synchronous=NORMAL")
    Exqlite.Sqlite3.execute(db, "PRAGMA cache_size = -16000")
    Exqlite.Sqlite3.execute(db, "PRAGMA mmap_size = 134217728")
    Exqlite.Sqlite3.execute(db, "PRAGMA temp_store = memory")

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

  # --- Indexing ---

  defp do_index_block(state, meta, entries, terms) do
    Exqlite.Sqlite3.execute(state.db, "BEGIN")
    insert_block_data(state, meta, entries, terms)
    Exqlite.Sqlite3.execute(state.db, "COMMIT")
    :ok
  end

  # Insert block data (caller manages transaction)
  defp insert_block_data(state, meta, _entries, terms) do
    format = Map.get(meta, :format, :zstd)
    format_str = Atom.to_string(format)
    db = state.db

    Exqlite.Sqlite3.bind(state.block_insert_stmt, [
      meta.block_id,
      meta[:file_path],
      meta.byte_size,
      meta.entry_count,
      meta.ts_min,
      meta.ts_max,
      meta[:data],
      format_str
    ])

    Exqlite.Sqlite3.step(db, state.block_insert_stmt)
    Exqlite.Sqlite3.reset(state.block_insert_stmt)

    insert_terms_batch(state, terms, meta.block_id)
  end

  defp insert_terms_batch(_state, [], _block_id), do: :ok

  defp insert_terms_batch(state, terms, block_id) do
    db = state.db

    terms
    |> Enum.chunk_every(@terms_batch_size)
    |> Enum.each(fn batch ->
      n = length(batch)
      params = Enum.flat_map(batch, fn term -> [term, block_id] end)
      stmt = get_or_cache_terms_stmt(state, n)
      Exqlite.Sqlite3.bind(stmt, params)
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.reset(stmt)
    end)
  end

  defp get_or_cache_terms_stmt(state, n) do
    case Map.get(state.terms_stmt_cache, n) do
      nil ->
        {:ok, stmt} = Exqlite.Sqlite3.prepare(state.db, build_terms_sql(n))
        Process.put({:terms_stmt_cache, n}, stmt)
        stmt

      stmt ->
        stmt
    end
  end

  defp extract_terms(entries) do
    entries
    |> Enum.reduce(MapSet.new(), fn entry, acc ->
      acc = MapSet.put(acc, "level:#{entry.level}")

      Enum.reduce(entry.metadata, acc, fn {k, v}, inner_acc ->
        MapSet.put(inner_acc, "#{k}:#{v}")
      end)
    end)
    |> MapSet.to_list()
  end

  defp build_terms_sql(n) do
    placeholders =
      Enum.map_join(1..n, ", ", fn i ->
        "(?#{i * 2 - 1}, ?#{i * 2})"
      end)

    "INSERT OR IGNORE INTO block_terms (term, block_id) VALUES #{placeholders}"
  end

  # --- Querying (parallel, runs in caller's process) ---

  defp do_query_parallel(block_ids, storage, pagination, search_filters) do
    start_time = System.monotonic_time()

    limit = Keyword.get(pagination, :limit, @default_limit)
    offset = Keyword.get(pagination, :offset, @default_offset)
    order = Keyword.get(pagination, :order, :desc)
    blocks_read = length(block_ids)

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

  # --- Block reading ---

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

  # --- Compaction ---

  defp do_compact_blocks(state, old_ids, new_terms_list) do
    db = state.db
    storage = state.storage

    old_file_paths =
      if storage == :disk do
        {:ok, fp_stmt} =
          Exqlite.Sqlite3.prepare(db, "SELECT file_path FROM blocks WHERE block_id = ?1")

        paths =
          Enum.flat_map(old_ids, fn id ->
            Exqlite.Sqlite3.bind(fp_stmt, [id])

            result =
              case Exqlite.Sqlite3.step(db, fp_stmt) do
                {:row, [path]} when is_binary(path) -> [path]
                _ -> []
              end

            Exqlite.Sqlite3.reset(fp_stmt)
            result
          end)

        Exqlite.Sqlite3.release(db, fp_stmt)
        paths
      else
        []
      end

    Exqlite.Sqlite3.execute(db, "BEGIN")

    {:ok, del_terms} = Exqlite.Sqlite3.prepare(db, "DELETE FROM block_terms WHERE block_id = ?1")
    {:ok, del_blocks} = Exqlite.Sqlite3.prepare(db, "DELETE FROM blocks WHERE block_id = ?1")

    for id <- old_ids do
      Exqlite.Sqlite3.bind(del_terms, [id])
      Exqlite.Sqlite3.step(db, del_terms)
      Exqlite.Sqlite3.reset(del_terms)

      Exqlite.Sqlite3.bind(del_blocks, [id])
      Exqlite.Sqlite3.step(db, del_blocks)
      Exqlite.Sqlite3.reset(del_blocks)
    end

    Exqlite.Sqlite3.release(db, del_terms)
    Exqlite.Sqlite3.release(db, del_blocks)

    for {meta, entries, terms} <- new_terms_list do
      insert_block_data(state, meta, entries, terms)
    end

    Exqlite.Sqlite3.execute(db, "COMMIT")

    for path <- old_file_paths, do: File.rm(path)

    :ok
  end

  # --- Deletion ---

  defp do_delete_before(db, cutoff_timestamp, storage) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "SELECT block_id, file_path FROM blocks WHERE ts_max < ?1")

    Exqlite.Sqlite3.bind(stmt, [cutoff_timestamp])
    blocks = collect_rows_2(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    block_ids = Enum.map(blocks, fn {id, _fp} -> id end)

    # Collect terms BEFORE deleting from SQLite
    old_terms = collect_terms_for_blocks(db, block_ids)

    count = delete_blocks(db, blocks, storage)
    {count, block_ids, old_terms}
  end

  defp do_delete_over_size(db, max_bytes, storage) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "SELECT COALESCE(SUM(byte_size), 0) FROM blocks")
    {:row, [total]} = Exqlite.Sqlite3.step(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    if total <= max_bytes do
      {0, [], []}
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

      block_ids = Enum.map(to_delete, fn {id, _fp} -> id end)

      # Collect terms BEFORE deleting from SQLite
      old_terms = collect_terms_for_blocks(db, block_ids)

      count = delete_blocks(db, to_delete, storage)
      {count, block_ids, old_terms}
    end
  end

  defp delete_blocks(_db, [], _storage), do: 0

  defp delete_blocks(db, blocks, storage) do
    Exqlite.Sqlite3.execute(db, "BEGIN")

    {:ok, del_terms} = Exqlite.Sqlite3.prepare(db, "DELETE FROM block_terms WHERE block_id = ?1")
    {:ok, del_blocks} = Exqlite.Sqlite3.prepare(db, "DELETE FROM blocks WHERE block_id = ?1")

    for {block_id, file_path} <- blocks do
      Exqlite.Sqlite3.bind(del_terms, [block_id])
      Exqlite.Sqlite3.step(db, del_terms)
      Exqlite.Sqlite3.reset(del_terms)

      Exqlite.Sqlite3.bind(del_blocks, [block_id])
      Exqlite.Sqlite3.step(db, del_blocks)
      Exqlite.Sqlite3.reset(del_blocks)

      if storage == :disk do
        File.rm(file_path)
      end
    end

    Exqlite.Sqlite3.release(db, del_terms)
    Exqlite.Sqlite3.release(db, del_blocks)

    Exqlite.Sqlite3.execute(db, "COMMIT")
    length(blocks)
  end

  # --- Query building ---

  defp split_pagination(filters) do
    {pagination, search} =
      Enum.split_with(filters, fn {k, _v} -> k in [:limit, :offset, :order] end)

    {search, pagination}
  end

  defp split_filters(filters) do
    term_filters =
      Enum.filter(filters, fn {k, _v} -> k in [:level, :metadata] end)

    time_filters =
      Enum.filter(filters, fn {k, _v} -> k in [:since, :until] end)

    {term_filters, time_filters}
  end

  defp build_query_terms(term_filters) do
    Enum.flat_map(term_filters, fn
      {:level, level} -> ["level:#{level}"]
      {:metadata, map} -> Enum.map(map, fn {k, v} -> "#{k}:#{v}" end)
      _ -> []
    end)
  end

  defp to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp to_unix(ts) when is_integer(ts), do: ts

  defp to_format_atom("raw"), do: :raw
  defp to_format_atom("zstd"), do: :zstd
  defp to_format_atom("openzl"), do: :openzl
  defp to_format_atom(:raw), do: :raw
  defp to_format_atom(:zstd), do: :zstd
  defp to_format_atom(:openzl), do: :openzl
  defp to_format_atom(_), do: :zstd

  # --- Row collectors ---

  defp collect_single_column(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [value]} -> [value | collect_single_column(db, stmt)]
      :done -> []
    end
  end

  defp collect_rows_2(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path]} -> [{block_id, file_path} | collect_rows_2(db, stmt)]
      :done -> []
    end
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
