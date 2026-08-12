defmodule TimelessLogs.Index do
  @moduledoc false

  use GenServer

  require Logger

  @default_limit 100
  @default_offset 0

  # Flush pending index operations after this interval
  @index_flush_interval 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec index_block(TimelessLogs.Writer.block_meta(), [map()], [String.t()]) :: :ok
  def index_block(block_meta, entries, terms) do
    GenServer.call(__MODULE__, {:index_block, block_meta, entries, terms})
  end

  # `shard`, when given, is credited back to the ingest-pressure gauge
  # once the block row lands in SQLite — the gauge covers an entry's whole
  # journey (shard mailbox, flush task, index mailbox), not just the part
  # before the disk write.
  @spec index_block_async(TimelessLogs.Writer.block_meta(), [map()], term(), integer() | nil) ::
          :ok
  def index_block_async(block_meta, entries, terms, shard \\ nil) do
    GenServer.cast(__MODULE__, {:index_block, block_meta, entries, terms, shard})
  end

  # --- Read functions (use DB reader pool, run in caller's process via DB GenServer) ---

  @spec query(keyword()) :: {:ok, TimelessLogs.Result.t()}
  def query(filters) do
    db = :persistent_term.get({__MODULE__, :db})
    storage = :persistent_term.get({__MODULE__, :storage})

    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)

    # Partition by the hot-tail boundary: memory serves ts >= boundary,
    # disk serves ts < boundary — exact union, no dedup.
    boundary = TimelessLogs.HotTail.boundary()

    {tail_entries, tail_total, disk_time_filters} =
      tail_partition(search_filters, time_filters, boundary, pagination)

    do_query_parallel(db, storage, term_filters, disk_time_filters, pagination, search_filters,
      tail_entries: tail_entries,
      tail_total: tail_total
    )
  end

  # Page entries always come from a bounded walk (never a full-tail
  # materialization); an exact total, when requested, is a chunked count.
  defp tail_partition(search_filters, time_filters, boundary, pagination) do
    since_us = time_filter_us(time_filters, :since)
    until_us = time_filter_us(time_filters, :until)
    tail_since = max(since_us || boundary, boundary)
    out_of_range = until_us != nil and until_us < boundary

    tail_entries =
      if out_of_range do
        []
      else
        limit = Keyword.get(pagination, :limit, @default_limit)
        offset = Keyword.get(pagination, :offset, @default_offset)
        order = Keyword.get(pagination, :order, :desc)
        TimelessLogs.HotTail.take(search_filters, tail_since, until_us, order, offset + limit + 1)
      end

    tail_total =
      cond do
        out_of_range -> 0
        not Keyword.get(pagination, :count_total, true) -> length(tail_entries)
        true -> TimelessLogs.HotTail.count_matching(search_filters, tail_since, until_us)
      end

    disk_time_filters =
      time_filters
      |> Keyword.delete(:until)
      |> Keyword.put(:until, min(until_us || boundary - 1, boundary - 1))

    {tail_entries, tail_total, disk_time_filters}
  end

  defp time_filter_us(time_filters, key) do
    case Keyword.get(time_filters, key) do
      nil -> nil
      ts -> to_unix(ts)
    end
  end

  @spec stats() :: {:ok, TimelessLogs.Stats.t()}
  def stats do
    db = :persistent_term.get({__MODULE__, :db})

    {:ok, format_rows} =
      TimelessLogs.DB.read(db, """
      SELECT format, COUNT(*), COALESCE(SUM(entry_count), 0),
             COALESCE(SUM(byte_size), 0), MIN(ts_min), MAX(ts_max)
      FROM blocks GROUP BY format
      """)

    {:ok, comp_rows} =
      TimelessLogs.DB.read(
        db,
        "SELECT raw_in, compressed_out, count FROM compression_stats WHERE key = 'lifetime'"
      )

    db_path = TimelessLogs.DB.db_path(db)
    index_size = file_size(db_path) + file_size(db_path <> "-wal") + file_size(db_path <> "-shm")

    {total_blocks, total_entries, total_bytes, oldest, newest, format_stats} =
      Enum.reduce(format_rows, {0, 0, 0, nil, nil, %{}}, fn
        [fmt, count, entries, bytes, ts_min, ts_max], {tb, te, tby, old, new, fs} ->
          new_old = if old == nil or ts_min < old, do: ts_min, else: old
          new_new = if new == nil or ts_max > new, do: ts_max, else: new
          updated = %{blocks: count, bytes: bytes, entries: entries}
          {tb + count, te + entries, tby + bytes, new_old, new_new, Map.put(fs, fmt, updated)}
      end)

    {raw_in, compressed_out, compaction_count} =
      case comp_rows do
        [[r, c, n]] -> {r, c, n}
        _ -> {0, 0, 0}
      end

    {:ok,
     %TimelessLogs.Stats{
       storage_mode: TimelessLogs.Config.storage(),
       total_blocks: total_blocks,
       total_entries: total_entries,
       total_bytes: total_bytes,
       oldest_timestamp: oldest,
       newest_timestamp: newest,
       disk_size: total_bytes,
       index_size: index_size,
       raw_blocks: (format_stats["raw"] || %{})[:blocks] || 0,
       raw_bytes: (format_stats["raw"] || %{})[:bytes] || 0,
       raw_entries: (format_stats["raw"] || %{})[:entries] || 0,
       compressed_blocks:
         ((format_stats["zstd"] || %{})[:blocks] || 0) +
           ((format_stats["openzl"] || %{})[:blocks] || 0),
       compressed_bytes:
         ((format_stats["zstd"] || %{})[:bytes] || 0) +
           ((format_stats["openzl"] || %{})[:bytes] || 0),
       zstd_blocks: (format_stats["zstd"] || %{})[:blocks] || 0,
       zstd_bytes: (format_stats["zstd"] || %{})[:bytes] || 0,
       zstd_entries: (format_stats["zstd"] || %{})[:entries] || 0,
       openzl_blocks: (format_stats["openzl"] || %{})[:blocks] || 0,
       openzl_bytes: (format_stats["openzl"] || %{})[:bytes] || 0,
       openzl_entries: (format_stats["openzl"] || %{})[:entries] || 0,
       compression_raw_bytes_in: raw_in,
       compression_compressed_bytes_out: compressed_out,
       compaction_count: compaction_count
     }}
  end

  @doc """
  Count entries matching the given filters without materializing them.

  When every filter is fully representable in the term index (level,
  indexed metadata, time range) and at most one term is involved, the
  count is answered from per-term index counts plus a scan of only the
  time-boundary blocks. Otherwise falls back to the scanning query path.
  """
  @spec count(keyword()) :: {:ok, non_neg_integer()}
  def count(filters) do
    db = :persistent_term.get({__MODULE__, :db})
    storage = :persistent_term.get({__MODULE__, :storage})

    {search_filters, _pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    terms = build_query_terms(term_filters)

    boundary = TimelessLogs.HotTail.boundary()

    {_tail_entries, tail_total, disk_time_filters} =
      tail_partition(search_filters, time_filters, boundary, count_total: true, limit: 0)

    disk_until = Keyword.fetch!(disk_time_filters, :until)
    disk_search_filters = [{:until, disk_until} | search_filters]

    {:ok, disk_total} =
      if index_countable?(search_filters, terms) do
        count_via_index(db, storage, terms, disk_time_filters, disk_search_filters)
      else
        # query/1 sees until < boundary, so its own tail partition is
        # empty — no double count.
        {:ok, %{total: total}} =
          search_filters
          |> Keyword.merge(until: disk_until, limit: 1, count_total: true)
          |> query()

        {:ok, total}
      end

    {:ok, disk_total + tail_total}
  end

  defp index_countable?(search_filters, terms) do
    length(terms) <= 1 and
      Enum.all?(search_filters, fn
        {:level, _} -> true
        {:metadata, map} -> Enum.all?(map, fn {k, v} -> indexed_metadata_term(k, v) != nil end)
        {:since, _} -> true
        {:until, _} -> true
        _ -> false
      end)
  end

  defp count_via_index(db, storage, terms, time_filters, search_filters) do
    since_us =
      case Keyword.get(time_filters, :since) do
        nil -> nil
        ts -> to_unix(ts)
      end

    until_us =
      case Keyword.get(time_filters, :until) do
        nil -> nil
        ts -> to_unix(ts)
      end

    # A block is "covered" when the time range fully contains it: its
    # counts apply verbatim. Blocks merely overlapping the range (or with
    # unknown legacy term counts) are scanned with the full filter set.
    covered = time_cond(since_us, "b.ts_min >=") ++ time_cond(until_us, "b.ts_max <=")
    in_range = time_cond(since_us, "b.ts_max >=") ++ time_cond(until_us, "b.ts_min <=")
    covered_sql = and_clause(covered)
    not_covered_sql = if covered == [], do: "0", else: "NOT (#{covered_sql})"
    in_range_sql = and_clause(in_range)

    {sum_sql, sum_params, scan_sql, scan_params} =
      case terms do
        [] ->
          {"SELECT COALESCE(SUM(b.entry_count), 0) FROM blocks b WHERE #{covered_sql}", [],
           "SELECT b.block_id, b.file_path, b.format FROM blocks b " <>
             "WHERE #{in_range_sql} AND #{not_covered_sql}", []}

        [term] ->
          {"SELECT COALESCE(SUM(ti.entry_count), 0) FROM term_index ti " <>
             "JOIN blocks b ON b.block_id = ti.block_id " <>
             "WHERE ti.term = ?1 AND ti.entry_count > 0 AND #{covered_sql}", [term],
           "SELECT b.block_id, b.file_path, b.format FROM term_index ti " <>
             "JOIN blocks b ON b.block_id = ti.block_id " <>
             "WHERE ti.term = ?1 AND #{in_range_sql} " <>
             "AND (ti.entry_count = 0 OR #{not_covered_sql})", [term]}
      end

    {:ok, [[covered_total]]} = TimelessLogs.DB.read(db, sum_sql, sum_params)
    {:ok, scan_rows} = TimelessLogs.DB.read(db, scan_sql, scan_params)

    scan_blocks = Enum.map(scan_rows, fn [bid, fp, fmt] -> {bid, fp, to_format_atom(fmt)} end)

    {:ok, covered_total + scan_count(scan_blocks, db, storage, search_filters)}
  end

  defp time_cond(nil, _op), do: []
  defp time_cond(ts, op), do: ["#{op} #{ts}"]

  defp and_clause([]), do: "1 = 1"
  defp and_clause(conds), do: Enum.join(conds, " AND ")

  defp scan_count([], _db, _storage, _search_filters), do: 0

  defp scan_count(blocks, db, storage, search_filters) do
    blocks
    |> Task.async_stream(
      fn {block_id, file_path, format} ->
        read_result =
          case storage do
            :disk -> TimelessLogs.Writer.read_block(file_path, format)
            :memory -> read_block_from_db(db, block_id)
          end

        case read_result do
          {:ok, entries} ->
            entries |> TimelessLogs.Filter.filter(search_filters) |> length()

          {:error, reason} ->
            TimelessLogs.Telemetry.event(
              [:timeless_logs, :block, :error],
              %{},
              %{file_path: file_path, reason: reason}
            )

            0
        end
      end,
      max_concurrency: TimelessLogs.Config.query_concurrency(),
      ordered: false
    )
    |> Enum.reduce(0, fn {:ok, n}, acc -> acc + n end)
  end

  @spec matching_block_ids(keyword()) :: [{integer(), String.t() | nil, :raw | :zstd}]
  def matching_block_ids(filters) do
    db = :persistent_term.get({__MODULE__, :db})
    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :asc)
    find_matching_blocks(db, term_filters, time_filters, order)
  end

  @spec raw_block_stats() :: %{
          entry_count: integer(),
          block_count: integer(),
          oldest_created_at: integer() | nil
        }
  def raw_block_stats do
    db = :persistent_term.get({__MODULE__, :db})

    {:ok, rows} =
      TimelessLogs.DB.read(db, """
      SELECT COUNT(*), COALESCE(SUM(entry_count), 0), COALESCE(SUM(byte_size), 0), MIN(created_at)
      FROM blocks WHERE format = 'raw'
      """)

    case rows do
      [[count, entries, bytes, oldest]] ->
        %{entry_count: entries, block_count: count, total_bytes: bytes, oldest_created_at: oldest}

      _ ->
        %{entry_count: 0, block_count: 0, total_bytes: 0, oldest_created_at: nil}
    end
  end

  @spec small_compressed_block_ids(pos_integer()) ::
          [{integer(), String.t() | nil, non_neg_integer(), non_neg_integer()}]
  def small_compressed_block_ids(max_entry_count) do
    db = :persistent_term.get({__MODULE__, :db})

    {:ok, rows} =
      TimelessLogs.DB.read(
        db,
        """
        SELECT block_id, file_path, byte_size, entry_count
        FROM blocks WHERE format != 'raw' AND entry_count < ?1
        ORDER BY ts_min ASC
        """,
        [max_entry_count]
      )

    Enum.map(rows, fn [bid, fp, bs, ec] -> {bid, fp, bs, ec} end)
  end

  @spec raw_block_ids() :: [{integer(), String.t() | nil, non_neg_integer()}]
  def raw_block_ids do
    db = :persistent_term.get({__MODULE__, :db})

    {:ok, rows} =
      TimelessLogs.DB.read(db, """
      SELECT block_id, file_path, byte_size, entry_count
      FROM blocks WHERE format = 'raw'
      ORDER BY ts_min ASC
      """)

    Enum.map(rows, fn [bid, fp, bs, ec] -> {bid, fp, bs, ec} end)
  end

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

  @spec delete_oldest_blocks_until_term_limit(pos_integer()) :: non_neg_integer()
  def delete_oldest_blocks_until_term_limit(max_entries) do
    GenServer.call(__MODULE__, {:delete_by_term_limit, max_entries}, 60_000)
  end

  @spec compact_blocks(
          [integer()],
          [{TimelessLogs.Writer.block_meta(), [map()], [String.t()]}],
          {non_neg_integer(), non_neg_integer()}
        ) :: :ok
  def compact_blocks(old_block_ids, new_terms_list, compression_sizes \\ {0, 0}) do
    GenServer.call(
      __MODULE__,
      {:compact_blocks, old_block_ids, new_terms_list, compression_sizes},
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
    Process.flag(:trap_exit, true)
    storage = Keyword.get(opts, :storage, :disk)
    db = Keyword.get(opts, :db, TimelessLogs.DB)
    data_dir = Keyword.get(opts, :data_dir)

    with :ok <- rebase_block_paths(db, storage, data_dir) do
      :persistent_term.put({__MODULE__, :storage}, storage)
      :persistent_term.put({__MODULE__, :db}, db)

      # One-time migration from old ETS snapshot
      if data_dir, do: maybe_migrate_from_ets(db, data_dir)

      {:ok, %{storage: storage, db: db, data_dir: data_dir, pending: [], flush_timer: nil}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, state) do
    flush_pending(state)
    :persistent_term.erase({__MODULE__, :storage})
    :persistent_term.erase({__MODULE__, :db})
  end

  # --- handle_call (grouped) ---

  @impl true
  def handle_call({:index_block, meta, _entries, terms}, _from, state) do
    state = flush_pending(state)
    do_index_block(state.db, state.storage, meta, terms)
    {:reply, :ok, state}
  end

  def handle_call({:delete_before, cutoff}, _from, state) do
    state = flush_pending(state)
    count = do_delete_before(state.db, cutoff, state.storage)
    {:reply, count, state}
  end

  def handle_call({:delete_over_size, max_bytes}, _from, state) do
    state = flush_pending(state)
    count = do_delete_over_size(state.db, max_bytes, state.storage)
    {:reply, count, state}
  end

  def handle_call({:delete_by_term_limit, max_entries}, _from, state) do
    state = flush_pending(state)
    count = do_delete_by_term_limit(state.db, max_entries, state.storage)
    {:reply, count, state}
  end

  def handle_call({:read_block_data, block_id}, _from, state) do
    state = flush_pending(state)
    result = read_block_from_db(state.db, block_id)
    {:reply, result, state}
  end

  def handle_call(
        {:compact_blocks, old_block_ids, new_terms_list, compression_sizes},
        _from,
        state
      ) do
    state = flush_pending(state)

    # Get file paths for old blocks before deleting
    old_file_paths =
      if old_block_ids != [] do
        ph = placeholders(old_block_ids)

        {:ok, rows} =
          TimelessLogs.DB.read(
            state.db,
            "SELECT file_path FROM blocks WHERE block_id IN (#{ph})",
            old_block_ids
          )

        for [fp] <- rows, is_binary(fp), do: fp
      else
        []
      end

    {:ok, _} =
      TimelessLogs.DB.write_transaction(state.db, fn conn ->
        # Delete old blocks
        if old_block_ids != [] do
          ph = placeholders(old_block_ids)

          TimelessLogs.DB.execute(
            conn,
            "DELETE FROM term_index WHERE block_id IN (#{ph})",
            old_block_ids
          )

          TimelessLogs.DB.execute(
            conn,
            "DELETE FROM block_data WHERE block_id IN (#{ph})",
            old_block_ids
          )

          TimelessLogs.DB.execute(
            conn,
            "DELETE FROM blocks WHERE block_id IN (#{ph})",
            old_block_ids
          )
        end

        # Insert new blocks
        for {meta, _entries, terms} <- new_terms_list do
          insert_block_sql(conn, meta)
          insert_terms_sql(conn, terms, meta.block_id)

          if state.storage == :memory and meta[:data] do
            TimelessLogs.DB.execute(
              conn,
              "INSERT OR REPLACE INTO block_data (block_id, data) VALUES (?1, ?2)",
              [meta.block_id, meta[:data]]
            )
          end
        end

        # Update compression stats
        {raw_in, compressed_out} = compression_sizes
        update_compression_stats_sql(conn, raw_in, compressed_out)
      end)

    # Delete old disk files outside the transaction
    if state.storage == :disk do
      Enum.each(old_file_paths, &File.rm/1)
    end

    {:reply, :ok, state}
  end

  def handle_call({:backup, target_path}, _from, state) do
    state = flush_pending(state)

    case TimelessLogs.DB.backup(state.db, target_path) do
      {:ok, _} -> {:reply, :ok, state}
      error -> {:reply, error, state}
    end
  end

  def handle_call(:sync, _from, state) do
    state = flush_pending(state)
    TimelessLogs.DB.write(state.db, "PRAGMA wal_checkpoint(TRUNCATE)")
    {:reply, :ok, state}
  end

  # --- handle_cast ---

  @impl true
  def handle_cast({:index_block, meta, entries, terms, shard}, state) do
    pending = [{meta, entries, terms, shard} | state.pending]
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

  # --- SQL write helpers ---

  defp rebase_block_paths(_db, storage, _data_dir) when storage != :disk, do: :ok
  defp rebase_block_paths(_db, :disk, nil), do: {:error, :missing_block_data_dir}

  defp rebase_block_paths(db, :disk, data_dir) do
    blocks_dir = data_dir |> Path.join("blocks") |> Path.expand()

    with {:ok, rows} <- TimelessLogs.DB.read(db, "SELECT block_id, file_path FROM blocks"),
         {:ok, updates} <- relocation_updates(rows, blocks_dir) do
      case updates do
        [] ->
          :ok

        updates ->
          case TimelessLogs.DB.write_transaction(db, fn conn ->
                 Enum.each(updates, fn {block_id, target} ->
                   case TimelessLogs.DB.execute(
                          conn,
                          "UPDATE blocks SET file_path = ?1 WHERE block_id = ?2",
                          [target, block_id]
                        ) do
                     {:ok, _} -> :ok
                     {:error, reason} -> raise "rebase block #{block_id}: #{inspect(reason)}"
                   end
                 end)
               end) do
            {:ok, _} -> :ok
            {:error, reason} -> {:error, {:block_path_rebase_failed, reason}}
          end
      end
    else
      {:error, reason} -> {:error, {:block_path_preflight_failed, reason}}
    end
  end

  defp relocation_updates(rows, blocks_dir) do
    Enum.reduce_while(rows, {:ok, []}, fn
      [_block_id, nil], {:ok, updates} ->
        {:cont, {:ok, updates}}

      [block_id, file_path], {:ok, updates} when is_binary(file_path) ->
        current = Path.expand(file_path)
        target = Path.join(blocks_dir, Path.basename(file_path))

        case File.lstat(target) do
          {:ok, %File.Stat{type: :regular}} when current == target ->
            {:cont, {:ok, updates}}

          {:ok, %File.Stat{type: :regular}} ->
            {:cont, {:ok, [{block_id, target} | updates]}}

          {:error, :enoent} when current == target ->
            # Preserve the existing missing-block reconciliation behavior for
            # an in-place store; never follow a path outside this data root.
            {:cont, {:ok, updates}}

          other ->
            {:halt,
             {:error, {:unsafe_block_path, block_id, file_path, target, block_path_reason(other)}}}
        end

      row, _acc ->
        {:halt, {:error, {:invalid_block_path_row, row}}}
    end)
  end

  defp block_path_reason({:ok, %File.Stat{type: type}}), do: {:unsupported_type, type}
  defp block_path_reason({:error, reason}), do: reason

  defp do_index_block(db, storage, meta, terms) do
    {:ok, _} =
      TimelessLogs.DB.write_transaction(db, fn conn ->
        insert_block_sql(conn, meta)
        insert_terms_sql(conn, terms, meta.block_id)

        if storage == :memory and meta[:data] do
          TimelessLogs.DB.execute(
            conn,
            "INSERT OR REPLACE INTO block_data (block_id, data) VALUES (?1, ?2)",
            [meta.block_id, meta[:data]]
          )
        end
      end)
  end

  defp insert_block_sql(conn, meta) do
    format = Map.get(meta, :format, :zstd) |> to_string()
    created_at = System.system_time(:second)

    TimelessLogs.DB.execute(
      conn,
      "INSERT OR REPLACE INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, format, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
      [
        meta.block_id,
        meta[:file_path],
        meta.byte_size,
        meta.entry_count,
        meta.ts_min,
        meta.ts_max,
        format,
        created_at
      ]
    )
  end

  defp insert_terms_sql(conn, terms, block_id) do
    case term_rows(terms, block_id) do
      [] ->
        :ok

      rows ->
        TimelessLogs.DB.execute_batch(
          conn,
          "INSERT OR REPLACE INTO term_index (term, block_id, entry_count) VALUES (?1, ?2, ?3)",
          rows
        )
    end
  end

  # Accepts the %{term => count} map from extract_terms/1; plain term lists
  # (legacy callers/tests) get entry_count 0, meaning "unknown — scan".
  defp term_rows(terms, block_id) do
    Enum.map(terms, fn
      {term, count} -> [term, block_id, count]
      term when is_binary(term) -> [term, block_id, 0]
    end)
  end

  defp update_compression_stats_sql(conn, raw_in, compressed_out) do
    if raw_in > 0 or compressed_out > 0 do
      TimelessLogs.DB.execute(
        conn,
        """
        INSERT INTO compression_stats (key, raw_in, compressed_out, count)
        VALUES ('lifetime', ?1, ?2, 1)
        ON CONFLICT(key) DO UPDATE SET
          raw_in = raw_in + excluded.raw_in,
          compressed_out = compressed_out + excluded.compressed_out,
          count = count + 1
        """,
        [raw_in, compressed_out]
      )
    end
  end

  # --- SQL delete helpers ---

  defp do_delete_before(db, cutoff, storage) do
    {:ok, rows} =
      TimelessLogs.DB.read(db, "SELECT block_id, file_path FROM blocks WHERE ts_max < ?1", [
        cutoff
      ])

    TimelessLogs.HotTail.prune_before(cutoff)

    if rows == [] do
      0
    else
      block_ids = Enum.map(rows, fn [bid, _fp] -> bid end)
      file_paths = for [_bid, fp] <- rows, is_binary(fp), do: fp
      delete_block_set(db, block_ids)

      if storage == :disk do
        Enum.each(file_paths, &File.rm/1)
      end

      length(block_ids)
    end
  end

  defp do_delete_over_size(db, max_bytes, storage) do
    {:ok, [[total]]} =
      TimelessLogs.DB.read(db, "SELECT COALESCE(SUM(byte_size), 0) FROM blocks")

    if total <= max_bytes do
      0
    else
      {:ok, rows} =
        TimelessLogs.DB.read(
          db,
          "SELECT block_id, file_path, byte_size, ts_max FROM blocks ORDER BY ts_min ASC"
        )

      {to_delete, _} =
        Enum.reduce_while(rows, {[], total}, fn [bid, fp, bs, ts_max], {acc, remaining} ->
          if remaining > max_bytes do
            {:cont, {[{bid, fp, ts_max} | acc], remaining - bs}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      if to_delete == [] do
        0
      else
        block_ids = Enum.map(to_delete, fn {bid, _fp, _ts} -> bid end)
        file_paths = for {_bid, fp, _ts} <- to_delete, is_binary(fp), do: fp
        max_ts = to_delete |> Enum.map(fn {_bid, _fp, ts} -> ts end) |> Enum.max()
        delete_block_set(db, block_ids)
        TimelessLogs.HotTail.prune_before(max_ts + 1)

        if storage == :disk do
          Enum.each(file_paths, &File.rm/1)
        end

        length(to_delete)
      end
    end
  end

  defp do_delete_by_term_limit(db, max_entries, storage) do
    {:ok, [[current_size]]} = TimelessLogs.DB.read(db, "SELECT COUNT(*) FROM term_index")

    if current_size <= max_entries do
      0
    else
      {:ok, rows} =
        TimelessLogs.DB.read(db, """
        SELECT b.block_id, b.file_path, COALESCE(t.tc, 0) as term_count, b.ts_max
        FROM blocks b
        LEFT JOIN (SELECT block_id, COUNT(*) as tc FROM term_index GROUP BY block_id) t
          ON b.block_id = t.block_id
        ORDER BY b.ts_min ASC
        """)

      {to_delete, _} =
        Enum.reduce_while(rows, {[], current_size}, fn [bid, fp, tc, ts_max], {acc, remaining} ->
          if remaining > max_entries do
            {:cont, {[{bid, fp, ts_max} | acc], remaining - tc}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      if to_delete == [] do
        0
      else
        block_ids = Enum.map(to_delete, fn {bid, _fp, _ts} -> bid end)
        file_paths = for {_bid, fp, _ts} <- to_delete, is_binary(fp), do: fp
        max_ts = to_delete |> Enum.map(fn {_bid, _fp, ts} -> ts end) |> Enum.max()
        delete_block_set(db, block_ids)
        TimelessLogs.HotTail.prune_before(max_ts + 1)

        if storage == :disk do
          Enum.each(file_paths, &File.rm/1)
        end

        length(to_delete)
      end
    end
  end

  defp delete_block_set(db, block_ids) do
    ph = placeholders(block_ids)

    {:ok, _} =
      TimelessLogs.DB.write_transaction(db, fn conn ->
        TimelessLogs.DB.execute(
          conn,
          "DELETE FROM term_index WHERE block_id IN (#{ph})",
          block_ids
        )

        TimelessLogs.DB.execute(
          conn,
          "DELETE FROM block_data WHERE block_id IN (#{ph})",
          block_ids
        )

        TimelessLogs.DB.execute(conn, "DELETE FROM blocks WHERE block_id IN (#{ph})", block_ids)
      end)
  end

  # --- SQL read helpers ---

  defp find_matching_blocks(db, term_filters, time_filters, order) do
    terms = build_query_terms(term_filters)
    order_dir = if order == :asc, do: "ASC", else: "DESC"

    {conditions, params} = build_block_conditions(terms, time_filters)
    where = if conditions == [], do: "", else: " WHERE " <> Enum.join(conditions, " AND ")
    sql = "SELECT block_id, file_path, format FROM blocks#{where} ORDER BY ts_min #{order_dir}"

    {:ok, rows} = TimelessLogs.DB.read(db, sql, params)
    Enum.map(rows, fn [bid, fp, fmt] -> {bid, fp, to_format_atom(fmt)} end)
  end

  defp build_block_conditions(terms, time_filters) do
    {conditions, params, idx} =
      case terms do
        [] ->
          {[], [], 1}

        _ ->
          n = length(terms)
          ph = Enum.map_join(1..n, ", ", &"?#{&1}")

          clause =
            "block_id IN (SELECT block_id FROM term_index WHERE term IN (#{ph}) GROUP BY block_id HAVING COUNT(DISTINCT term) = ?#{n + 1})"

          {[clause], terms ++ [n], n + 2}
      end

    {time_conds, time_params, _} =
      Enum.reduce(time_filters, {[], [], idx}, fn
        {:since, ts}, {c, p, i} -> {c ++ ["ts_max >= ?#{i}"], p ++ [to_unix(ts)], i + 1}
        {:until, ts}, {c, p, i} -> {c ++ ["ts_min <= ?#{i}"], p ++ [to_unix(ts)], i + 1}
      end)

    {conditions ++ time_conds, params ++ time_params}
  end

  defp read_block_from_db(db, block_id) do
    {:ok, rows} =
      TimelessLogs.DB.read(
        db,
        """
        SELECT bd.data, b.format FROM block_data bd
        JOIN blocks b ON bd.block_id = b.block_id
        WHERE bd.block_id = ?1
        """,
        [block_id]
      )

    case rows do
      [[data, format]] when is_binary(data) ->
        TimelessLogs.Writer.decompress_block(data, to_format_atom(format))

      _ ->
        {:error, :not_found}
    end
  end

  # --- Pending flush helpers ---

  defp flush_pending(%{pending: []} = state), do: state

  defp flush_pending(%{pending: pending} = state) do
    resolved = Enum.reverse(pending)
    created_at = System.system_time(:second)

    # Collect all block params and term params across all pending blocks
    {block_params_list, term_params_list, data_params_list} =
      Enum.reduce(resolved, {[], [], []}, fn {meta, _entries, terms, _shard}, {bp, tp, dp} ->
        format = Map.get(meta, :format, :zstd) |> to_string()

        block_row = [
          meta.block_id,
          meta[:file_path],
          meta.byte_size,
          meta.entry_count,
          meta.ts_min,
          meta.ts_max,
          format,
          created_at
        ]

        term_rows = term_rows(terms, meta.block_id)

        data_rows =
          if state.storage == :memory and meta[:data] do
            [[meta.block_id, meta[:data]]]
          else
            []
          end

        {[block_row | bp], term_rows ++ tp, data_rows ++ dp}
      end)

    {:ok, _} =
      TimelessLogs.DB.write_transaction(state.db, fn conn ->
        TimelessLogs.DB.execute_batch(
          conn,
          "INSERT OR REPLACE INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, format, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
          Enum.reverse(block_params_list)
        )

        if term_params_list != [] do
          TimelessLogs.DB.execute_batch(
            conn,
            "INSERT OR REPLACE INTO term_index (term, block_id, entry_count) VALUES (?1, ?2, ?3)",
            term_params_list
          )
        end

        if data_params_list != [] do
          TimelessLogs.DB.execute_batch(
            conn,
            "INSERT OR REPLACE INTO block_data (block_id, data) VALUES (?1, ?2)",
            data_params_list
          )
        end
      end)

    # Entries are durable and visible now — credit the ingest gauge.
    Enum.each(resolved, fn
      {meta, _entries, _terms, shard} when is_integer(shard) ->
        TimelessLogs.IngestPressure.sub(shard, meta.entry_count)

      _ ->
        :ok
    end)

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

  # --- Indexing ---

  @spec extract_terms([map()]) :: %{String.t() => pos_integer()}
  def extract_terms(entries) do
    # Log batches are highly repetitive: the same {key, value} pairs recur
    # thousands of times per block, and the indexability verdict for a
    # pair is deterministic. Run the (comparatively expensive) heuristics
    # once per distinct pair and only count occurrences per entry.
    {terms, _memo} =
      Enum.reduce(entries, {%{}, %{}}, fn entry, {terms, memo} ->
        terms = Map.update(terms, level_term(entry.level), 1, &(&1 + 1))

        Enum.reduce(entry.metadata, {terms, memo}, fn {k, v}, {t, m} ->
          # Non-whitelisted keys (request_id and friends, often unique
          # per entry) are rejected by a set lookup — memoizing them
          # would just grow the memo by one entry per entry.
          if indexed_key?(k) do
            case Map.fetch(m, {k, v}) do
              {:ok, nil} ->
                {t, m}

              {:ok, term} ->
                {Map.update(t, term, 1, &(&1 + 1)), m}

              :error ->
                term = indexed_metadata_term(k, v)
                t = if term, do: Map.update(t, term, 1, &(&1 + 1)), else: t
                {t, Map.put(m, {k, v}, term)}
            end
          else
            {t, m}
          end
        end)
      end)

    terms
  end

  defp level_term(:debug), do: "level:debug"
  defp level_term(:info), do: "level:info"
  defp level_term(:notice), do: "level:notice"
  defp level_term(:warning), do: "level:warning"
  defp level_term(:error), do: "level:error"
  defp level_term(:critical), do: "level:critical"
  defp level_term(level), do: "level:#{level}"

  @indexed_metadata_keys MapSet.new([
                           "application",
                           "cache",
                           "host",
                           "key",
                           "job",
                           "method",
                           "path",
                           "reason",
                           "service",
                           "status",
                           "table"
                         ])

  defp indexed_key?(key) when is_atom(key), do: indexed_key?(Atom.to_string(key))
  defp indexed_key?(key) when is_binary(key), do: MapSet.member?(@indexed_metadata_keys, key)
  defp indexed_key?(_key), do: false

  defp indexed_metadata_term(key, value)
       when (is_binary(value) or is_atom(value)) and is_atom(key) do
    indexed_metadata_term(Atom.to_string(key), value)
  end

  defp indexed_metadata_term(key, value)
       when (is_binary(value) or is_atom(value)) and is_binary(key) do
    if MapSet.member?(@indexed_metadata_keys, key) and
         low_cardinality_value?(key, value) do
      "#{key}:#{value}"
    end
  end

  defp indexed_metadata_term(_key, _value), do: nil

  defp low_cardinality_value?("host", value) when is_atom(value),
    do: low_cardinality_value?("host", Atom.to_string(value))

  defp low_cardinality_value?("host", value) when is_binary(value) do
    byte_size(value) <= 255
  end

  defp low_cardinality_value?(_key, value), do: low_cardinality_value?(value)

  defp low_cardinality_value?(value) when is_atom(value),
    do: low_cardinality_value?(Atom.to_string(value))

  defp low_cardinality_value?(value) when is_binary(value) do
    byte_size(value) <= 64 and not high_cardinality_shape?(value)
  end

  defp low_cardinality_value?(_value), do: false

  # Identifier-like (hyphen/underscore-separated of length >= 12, or a
  # 12+ char hex string) or mostly-numeric values. A single byte walk
  # replaces the original regexes — this runs on the flush hot path.
  # Non-ASCII values fall back to the regex originals for exact
  # equivalence (the same verdict guards query-side block narrowing).
  defp high_cardinality_shape?(value) do
    case ascii_scan(value, 0, 0, 0, true) do
      {:ascii, len, digits, seps, all_hex} ->
        (seps > 0 and len >= 12) or (all_hex and len >= 12) or
          (digits > 0 and digits >= max(div(len * 3, 4), 6))

      :non_ascii ->
        (String.contains?(value, ["-", "_"]) and String.length(value) >= 12) or
          String.match?(value, ~r/\A[0-9a-f]{12,}\z/i) or
          mostly_numeric_regex?(value)
    end
  end

  defp mostly_numeric_regex?(value) do
    digits = String.replace(value, ~r/\D/, "")
    digits != "" and String.length(digits) >= max(div(String.length(value) * 3, 4), 6)
  end

  defp ascii_scan(<<>>, len, digits, seps, all_hex),
    do: {:ascii, len, digits, seps, all_hex and len > 0}

  defp ascii_scan(<<b, rest::binary>>, len, digits, seps, all_hex) when b < 128 do
    digit? = b >= ?0 and b <= ?9

    ascii_scan(
      rest,
      len + 1,
      if(digit?, do: digits + 1, else: digits),
      if(b == ?- or b == ?_, do: seps + 1, else: seps),
      all_hex and
        (digit? or (b >= ?a and b <= ?f) or (b >= ?A and b <= ?F))
    )
  end

  defp ascii_scan(_value, _len, _digits, _seps, _all_hex), do: :non_ascii

  # --- Querying (with early-exit limit) ---
  #
  # Blocks arrive pre-sorted in the requested timestamp order from
  # find_matching_blocks (newest-first for :desc, oldest-first for :asc).
  # We read blocks sequentially (or in parallel batches for disk) and stop
  # as soon as we've accumulated enough filtered entries (offset + limit).
  # This turns an O(all_entries) scan into O(limit) for the common case.

  defp do_query_parallel(
         db,
         storage,
         term_filters,
         disk_time_filters,
         pagination,
         search_filters,
         opts
       ) do
    start_time = System.monotonic_time()

    limit = Keyword.get(pagination, :limit, @default_limit)
    offset = Keyword.get(pagination, :offset, @default_offset)
    order = Keyword.get(pagination, :order, :desc)
    count_total = Keyword.get(pagination, :count_total, true)
    need = offset + limit
    collect_need = if count_total, do: need, else: need + 1

    tail_sorted =
      opts
      |> Keyword.get(:tail_entries, [])
      |> Enum.map(&TimelessLogs.Entry.from_map/1)
      |> sort_entries(order)

    tail_total = Keyword.get(opts, :tail_total, length(tail_sorted))

    # For :desc the tail (all newer than any disk entry) fills the page
    # first; only the remainder needs disk reads. When the tail alone
    # satisfies the page, skip block selection entirely.
    disk_need =
      if count_total or order == :asc do
        collect_need
      else
        max(collect_need - length(tail_sorted), 0)
      end

    boundary_until = Keyword.fetch!(disk_time_filters, :until)
    disk_search_filters = [{:until, boundary_until} | search_filters]

    {collected, disk_total, blocks_read} =
      if disk_need == 0 and not count_total do
        {[], 0, 0}
      else
        block_ids = find_matching_blocks(db, term_filters, disk_time_filters, order)

        collect_with_early_exit(
          block_ids,
          db,
          storage,
          disk_search_filters,
          disk_need,
          count_total,
          order
        )
      end

    sorted =
      case order do
        :asc -> Enum.sort_by(collected ++ tail_sorted, & &1.timestamp, :asc)
        :desc -> Enum.sort_by(tail_sorted ++ collected, & &1.timestamp, :desc)
      end

    total = disk_total + tail_total
    has_more = length(sorted) > need
    page = sorted |> Enum.take(need) |> Enum.drop(offset) |> Enum.take(limit)

    reported_total =
      if count_total, do: total, else: offset + length(page) + if(has_more, do: 1, else: 0)

    duration = System.monotonic_time() - start_time

    TimelessLogs.Telemetry.event(
      [:timeless_logs, :query, :stop],
      %{duration: duration, total: reported_total, blocks_read: blocks_read},
      %{filters: search_filters, count_total: count_total}
    )

    {:ok,
     %TimelessLogs.Result{
       entries: page,
       total: reported_total,
       limit: limit,
       offset: offset,
       has_more: has_more
     }}
  end

  defp collect_with_early_exit(block_ids, db, storage, search_filters, need, count_total, order) do
    if storage == :disk and length(block_ids) > 1 do
      collect_parallel_early_exit(block_ids, search_filters, need, count_total, order)
    else
      collect_sequential_early_exit(
        block_ids,
        db,
        storage,
        search_filters,
        need,
        count_total,
        order
      )
    end
  end

  defp collect_sequential_early_exit(
         block_ids,
         db,
         storage,
         search_filters,
         need,
         count_total,
         order
       ) do
    Enum.reduce_while(block_ids, {[], 0, 0}, fn {block_id, file_path, format},
                                                {acc, total, count} ->
      format_atom = to_format_atom(format)

      read_result =
        case storage do
          :disk -> TimelessLogs.Writer.read_block(file_path, format_atom)
          :memory -> read_block_from_db(db, block_id)
        end

      case read_result do
        {:ok, entries} ->
          filtered =
            entries
            |> TimelessLogs.Filter.filter(search_filters)
            |> Enum.map(&TimelessLogs.Entry.from_map/1)
            |> sort_entries(order)

          new_total = total + length(filtered)
          new_count = count + 1
          remaining = max(need - length(acc), 0)
          new_acc = if remaining > 0, do: acc ++ Enum.take(filtered, remaining), else: acc

          result = {new_acc, new_total, new_count}

          if count_total or length(new_acc) < need do
            {:cont, result}
          else
            {:halt, result}
          end

        {:error, reason} ->
          TimelessLogs.Telemetry.event(
            [:timeless_logs, :block, :error],
            %{},
            %{file_path: file_path, reason: reason}
          )

          {:cont, {acc, total, count + 1}}
      end
    end)
  end

  defp collect_parallel_early_exit(block_ids, search_filters, need, count_total, order) do
    batch_size = TimelessLogs.Config.query_concurrency()

    block_ids
    |> Enum.chunk_every(batch_size)
    |> Enum.reduce_while({[], 0, 0}, fn batch, {acc, total, count} ->
      batch_results =
        batch
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
          max_concurrency: batch_size,
          ordered: false
        )
        |> Enum.flat_map(fn {:ok, entries} -> entries end)
        |> sort_entries(order)

      new_total = total + length(batch_results)
      new_count = count + length(batch)
      remaining = max(need - length(acc), 0)
      new_acc = if remaining > 0, do: acc ++ Enum.take(batch_results, remaining), else: acc

      result = {new_acc, new_total, new_count}

      if count_total or length(new_acc) < need do
        {:cont, result}
      else
        {:halt, result}
      end
    end)
  end

  defp sort_entries(entries, :asc), do: Enum.sort(entries, &entry_before?(&1, &2, :asc))
  defp sort_entries(entries, :desc), do: Enum.sort(entries, &entry_before?(&1, &2, :desc))

  defp entry_before?(left, right, :asc) do
    left.timestamp < right.timestamp or
      (left.timestamp == right.timestamp and entry_tie_key(left) <= entry_tie_key(right))
  end

  defp entry_before?(left, right, :desc) do
    left.timestamp > right.timestamp or
      (left.timestamp == right.timestamp and entry_tie_key(left) <= entry_tie_key(right))
  end

  defp entry_tie_key(entry), do: {entry.message, entry.level, entry.metadata}

  # --- Query building ---

  defp split_pagination(filters) do
    {pagination, search} =
      Enum.split_with(filters, fn {k, _v} -> k in [:limit, :offset, :order, :count_total] end)

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
      {:level, level} ->
        ["level:#{level}"]

      {:metadata, map} ->
        # Only terms the write side would actually have indexed may narrow
        # block selection. A filter on a non-indexed key or high-cardinality
        # value has no term rows — using it here would silently return
        # nothing; the per-entry filter handles it instead.
        Enum.flat_map(map, fn {k, v} ->
          case indexed_metadata_term(k, v) do
            nil -> []
            term -> [term]
          end
        end)

      _ ->
        []
    end)
  end

  # --- Migration from old ETS snapshot ---

  defp maybe_migrate_from_ets(db, data_dir) do
    snapshot_path = Path.join(data_dir, "index.snapshot")
    log_path = Path.join(data_dir, "index.log")

    case File.read(snapshot_path) do
      {:ok, binary} ->
        try do
          snapshot = :erlang.binary_to_term(binary)

          {:ok, _} =
            TimelessLogs.DB.write_transaction(db, fn conn ->
              for {block_id, file_path, byte_size, entry_count, ts_min, ts_max, format,
                   created_at} <-
                    snapshot.blocks do
                TimelessLogs.DB.execute(
                  conn,
                  "INSERT OR IGNORE INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, format, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                  [
                    block_id,
                    file_path,
                    byte_size,
                    entry_count,
                    ts_min,
                    ts_max,
                    to_string(format),
                    created_at
                  ]
                )
              end

              for {term, block_id} <- snapshot.term_index do
                TimelessLogs.DB.execute(
                  conn,
                  "INSERT OR IGNORE INTO term_index (term, block_id) VALUES (?1, ?2)",
                  [term, block_id]
                )
              end

              for {:lifetime, raw_in, compressed_out, count} <- snapshot.compression_stats do
                TimelessLogs.DB.execute(
                  conn,
                  "INSERT OR REPLACE INTO compression_stats (key, raw_in, compressed_out, count) VALUES ('lifetime', ?1, ?2, ?3)",
                  [raw_in, compressed_out, count]
                )
              end

              for {block_id, data} <- Map.get(snapshot, :block_data, []) do
                TimelessLogs.DB.execute(
                  conn,
                  "INSERT OR IGNORE INTO block_data (block_id, data) VALUES (?1, ?2)",
                  [block_id, data]
                )
              end
            end)

          File.rm(snapshot_path)
          File.rm(log_path)
          File.rm(log_path <> ".idx")

          Logger.info(
            "TimelessLogs: migrated #{length(snapshot.blocks)} blocks from ETS snapshot to SQLite"
          )
        rescue
          e ->
            Logger.warning("TimelessLogs: failed to migrate from ETS snapshot: #{inspect(e)}")
        end

      {:error, _} ->
        :ok
    end
  end

  # --- Utilities ---

  defp placeholders(list) do
    list |> Enum.with_index(1) |> Enum.map_join(", ", fn {_, i} -> "?#{i}" end)
  end

  defp to_unix(ts), do: TimelessLogs.Timestamp.to_microseconds(ts)

  defp to_format_atom("raw"), do: :raw
  defp to_format_atom("zstd"), do: :zstd
  defp to_format_atom("openzl"), do: :openzl
  defp to_format_atom(:raw), do: :raw
  defp to_format_atom(:zstd), do: :zstd
  defp to_format_atom(:openzl), do: :openzl
  defp to_format_atom(_), do: :zstd

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end
end
