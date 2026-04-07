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

  @spec index_block_async(TimelessLogs.Writer.block_meta(), [map()], [String.t()]) :: :ok
  def index_block_async(block_meta, entries, terms) do
    GenServer.cast(__MODULE__, {:index_block, block_meta, entries, terms})
  end

  # --- Read functions (use DB reader pool, run in caller's process via DB GenServer) ---

  @spec query(keyword()) :: {:ok, TimelessLogs.Result.t()}
  def query(filters) do
    db = :persistent_term.get({__MODULE__, :db})
    storage = :persistent_term.get({__MODULE__, :storage})

    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :desc)
    block_ids = find_matching_blocks(db, term_filters, time_filters, order)

    do_query_parallel(block_ids, db, storage, pagination, search_filters)
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
      SELECT COUNT(*), COALESCE(SUM(entry_count), 0), MIN(created_at)
      FROM blocks WHERE format = 'raw'
      """)

    case rows do
      [[count, entries, oldest]] ->
        %{entry_count: entries, block_count: count, oldest_created_at: oldest}

      _ ->
        %{entry_count: 0, block_count: 0, oldest_created_at: nil}
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
      SELECT block_id, file_path, byte_size
      FROM blocks WHERE format = 'raw'
      ORDER BY ts_min ASC
      """)

    Enum.map(rows, fn [bid, fp, bs] -> {bid, fp, bs} end)
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

    :persistent_term.put({__MODULE__, :storage}, storage)
    :persistent_term.put({__MODULE__, :db}, db)

    # One-time migration from old ETS snapshot
    if data_dir, do: maybe_migrate_from_ets(db, data_dir)

    {:ok, %{storage: storage, db: db, data_dir: data_dir, pending: [], flush_timer: nil}}
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
  def handle_cast({:index_block, meta, entries, terms}, state) do
    pending = [{meta, entries, terms} | state.pending]
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

  defp insert_terms_sql(_conn, [], _block_id), do: :ok

  defp insert_terms_sql(conn, terms, block_id) do
    TimelessLogs.DB.execute_batch(
      conn,
      "INSERT OR IGNORE INTO term_index (term, block_id) VALUES (?1, ?2)",
      Enum.map(terms, &[&1, block_id])
    )
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
          "SELECT block_id, file_path, byte_size FROM blocks ORDER BY ts_min ASC"
        )

      {to_delete, _} =
        Enum.reduce_while(rows, {[], total}, fn [bid, fp, bs], {acc, remaining} ->
          if remaining > max_bytes do
            {:cont, {[{bid, fp} | acc], remaining - bs}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      if to_delete == [] do
        0
      else
        block_ids = Enum.map(to_delete, fn {bid, _fp} -> bid end)
        file_paths = for {_bid, fp} <- to_delete, is_binary(fp), do: fp
        delete_block_set(db, block_ids)

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
        SELECT b.block_id, b.file_path, COALESCE(t.tc, 0) as term_count
        FROM blocks b
        LEFT JOIN (SELECT block_id, COUNT(*) as tc FROM term_index GROUP BY block_id) t
          ON b.block_id = t.block_id
        ORDER BY b.ts_min ASC
        """)

      {to_delete, _} =
        Enum.reduce_while(rows, {[], current_size}, fn [bid, fp, tc], {acc, remaining} ->
          if remaining > max_entries do
            {:cont, {[{bid, fp} | acc], remaining - tc}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      if to_delete == [] do
        0
      else
        block_ids = Enum.map(to_delete, fn {bid, _fp} -> bid end)
        file_paths = for {_bid, fp} <- to_delete, is_binary(fp), do: fp
        delete_block_set(db, block_ids)

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
      Enum.reduce(resolved, {[], [], []}, fn {meta, _entries, terms}, {bp, tp, dp} ->
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

        term_rows = Enum.map(terms, &[&1, meta.block_id])

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
            "INSERT OR IGNORE INTO term_index (term, block_id) VALUES (?1, ?2)",
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

  @spec extract_terms([map()]) :: [String.t()]
  def extract_terms(entries) do
    entries
    |> Enum.reduce(MapSet.new(), fn entry, acc ->
      acc = MapSet.put(acc, "level:#{entry.level}")

      Enum.reduce(entry.metadata, acc, fn {k, v}, inner_acc ->
        case indexed_metadata_term(k, v) do
          nil -> inner_acc
          term -> MapSet.put(inner_acc, term)
        end
      end)
    end)
    |> MapSet.to_list()
  end

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
    byte_size(value) <= 64 and
      not likely_identifier?(value) and
      not mostly_numeric?(value)
  end

  defp low_cardinality_value?(_value), do: false

  defp likely_identifier?(value) do
    (String.contains?(value, ["-", "_"]) and String.length(value) >= 12) or
      String.match?(value, ~r/\A[0-9a-f]{12,}\z/i)
  end

  defp mostly_numeric?(value) do
    digits = String.replace(value, ~r/\D/, "")
    digits != "" and String.length(digits) >= max(div(String.length(value) * 3, 4), 6)
  end

  # --- Querying (with early-exit limit) ---
  #
  # Blocks arrive pre-sorted in the requested timestamp order from
  # find_matching_blocks (newest-first for :desc, oldest-first for :asc).
  # We read blocks sequentially (or in parallel batches for disk) and stop
  # as soon as we've accumulated enough filtered entries (offset + limit).
  # This turns an O(all_entries) scan into O(limit) for the common case.

  defp do_query_parallel(block_ids, db, storage, pagination, search_filters) do
    start_time = System.monotonic_time()

    limit = Keyword.get(pagination, :limit, @default_limit)
    offset = Keyword.get(pagination, :offset, @default_offset)
    order = Keyword.get(pagination, :order, :desc)
    count_total = Keyword.get(pagination, :count_total, true)
    need = offset + limit
    collect_need = if count_total, do: need, else: need + 1

    {collected, total, blocks_read} =
      collect_with_early_exit(block_ids, db, storage, search_filters, collect_need, count_total)

    sorted =
      case order do
        :asc -> Enum.sort_by(collected, & &1.timestamp, :asc)
        :desc -> Enum.sort_by(collected, & &1.timestamp, :desc)
      end

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

  defp collect_with_early_exit(block_ids, db, storage, search_filters, need, count_total) do
    if storage == :disk and length(block_ids) > 1 do
      collect_parallel_early_exit(block_ids, search_filters, need, count_total)
    else
      collect_sequential_early_exit(block_ids, db, storage, search_filters, need, count_total)
    end
  end

  defp collect_sequential_early_exit(block_ids, db, storage, search_filters, need, count_total) do
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

  defp collect_parallel_early_exit(block_ids, search_filters, need, count_total) do
    batch_size = System.schedulers_online()

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
      {:level, level} -> ["level:#{level}"]
      {:metadata, map} -> Enum.map(map, fn {k, v} -> "#{k}:#{v}" end)
      _ -> []
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

  defp to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp to_unix(ts) when is_integer(ts), do: ts

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
