defmodule TimelessLogs.Index do
  @moduledoc false

  use GenServer

  @default_limit 100
  @default_offset 0

  # Flush pending index operations after this interval
  @index_flush_interval 100

  @blocks_table :timeless_logs_blocks
  @term_index_table :timeless_logs_term_index
  @compression_stats_table :timeless_logs_compression_stats
  @block_data_table :timeless_logs_block_data

  @snapshot_threshold 1000

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
      try do
        snapshot_size =
          case :persistent_term.get({__MODULE__, :snapshot_path}, nil) do
            nil -> 0
            path -> file_size(path)
          end

        log_size =
          case :persistent_term.get({__MODULE__, :log_path}, nil) do
            nil -> 0
            path -> file_size(path)
          end

        snapshot_size + log_size
      rescue
        _ -> 0
      end

    {raw_in, compressed_out, compaction_count} =
      case :ets.lookup(@compression_stats_table, :lifetime) do
        [{:lifetime, r, c, n}] -> {r, c, n}
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
       raw_blocks: format_stats["raw"][:blocks] || 0,
       raw_bytes: format_stats["raw"][:bytes] || 0,
       raw_entries: format_stats["raw"][:entries] || 0,
       zstd_blocks: format_stats["zstd"][:blocks] || 0,
       zstd_bytes: format_stats["zstd"][:bytes] || 0,
       zstd_entries: format_stats["zstd"][:entries] || 0,
       openzl_blocks: format_stats["openzl"][:blocks] || 0,
       openzl_bytes: format_stats["openzl"][:bytes] || 0,
       openzl_entries: format_stats["openzl"][:entries] || 0,
       compression_raw_bytes_in: raw_in,
       compression_compressed_bytes_out: compressed_out,
       compaction_count: compaction_count
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
    :ets.foldl(
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
      end,
      %{entry_count: 0, block_count: 0, oldest_created_at: nil},
      @blocks_table
    )
  end

  @spec small_compressed_block_ids(pos_integer()) ::
          [{integer(), String.t() | nil, non_neg_integer(), non_neg_integer()}]
  def small_compressed_block_ids(max_entry_count) do
    :ets.foldl(
      fn {bid, fp, bs, ec, ts_min, _tsmax, format, _ca}, acc ->
        if format != :raw and ec < max_entry_count do
          [{ts_min, bid, fp, bs, ec} | acc]
        else
          acc
        end
      end,
      [],
      @blocks_table
    )
    |> Enum.sort_by(fn {ts_min, _bid, _fp, _bs, _ec} -> ts_min end)
    |> Enum.map(fn {_ts_min, bid, fp, bs, ec} -> {bid, fp, bs, ec} end)
  end

  @spec raw_block_ids() :: [{integer(), String.t() | nil, non_neg_integer()}]
  def raw_block_ids do
    @blocks_table
    |> :ets.tab2list()
    |> Enum.filter(fn {_bid, _fp, _bs, _ec, _tsmin, _tsmax, format, _ca} -> format == :raw end)
    |> Enum.sort_by(fn {_bid, _fp, _bs, _ec, ts_min, _tsmax, _fmt, _ca} -> ts_min end)
    |> Enum.map(fn {bid, fp, bs, _ec, _tsmin, _tsmax, _fmt, _ca} -> {bid, fp, bs} end)
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
    storage = Keyword.get(opts, :storage, :disk)

    # Initialize ETS tables
    init_ets_tables()

    # Store storage mode in persistent_term
    :persistent_term.put({__MODULE__, :storage}, storage)

    case storage do
      :memory ->
        :persistent_term.put({__MODULE__, :snapshot_path}, nil)
        :persistent_term.put({__MODULE__, :log_path}, nil)

        {:ok,
         %{
           storage: storage,
           log_ref: nil,
           log_path: nil,
           snapshot_path: nil,
           log_entry_count: 0,
           snapshot_threshold: @snapshot_threshold,
           pending: [],
           flush_timer: nil
         }}

      :disk ->
        data_dir = Keyword.fetch!(opts, :data_dir)
        log_path = Path.join(data_dir, "index.log")
        snapshot_path = Path.join(data_dir, "index.snapshot")
        db_path = Path.join(data_dir, "index.db")

        :persistent_term.put({__MODULE__, :snapshot_path}, snapshot_path)
        :persistent_term.put({__MODULE__, :log_path}, log_path)

        # SQLite migration: if index.db exists but no snapshot, migrate
        if File.exists?(db_path) and not File.exists?(snapshot_path) do
          migrate_from_sqlite(db_path, data_dir)
        end

        # Load snapshot into ETS
        load_snapshot(snapshot_path)

        # Open disk_log and replay entries after snapshot
        log_ref = open_disk_log(log_path)
        log_entry_count = replay_log(log_ref, snapshot_timestamp())

        {:ok,
         %{
           storage: storage,
           log_ref: log_ref,
           log_path: log_path,
           snapshot_path: snapshot_path,
           log_entry_count: log_entry_count,
           snapshot_threshold: @snapshot_threshold,
           pending: [],
           flush_timer: nil
         }}
    end
  end

  @impl true
  def terminate(_reason, state) do
    state = flush_pending(state)

    # Write final snapshot and close log
    if state.storage == :disk and state.log_ref != nil do
      write_snapshot(state.snapshot_path)
      :disk_log.sync(state.log_ref)
      :disk_log.close(state.log_ref)
    end

    # Clean up persistent_term keys
    :persistent_term.erase({__MODULE__, :storage})
    :persistent_term.erase({__MODULE__, :snapshot_path})
    :persistent_term.erase({__MODULE__, :log_path})

    # Clean up ETS tables
    safe_delete_ets(@blocks_table)
    safe_delete_ets(@term_index_table)
    safe_delete_ets(@compression_stats_table)
    safe_delete_ets(@block_data_table)
  end

  # --- handle_call (grouped) ---

  @impl true
  def handle_call({:index_block, meta, _entries, terms}, _from, state) do
    state = flush_pending(state)

    # Insert into ETS
    insert_block_ets(meta)
    insert_index_entries(terms, meta.block_id)

    # Memory mode: store block data in ETS
    if state.storage == :memory and meta[:data] do
      :ets.insert(@block_data_table, {meta.block_id, meta[:data]})
    end

    # Append to log
    state =
      append_log(state, {:index_block, System.monotonic_time(), block_meta_to_map(meta), terms})

    {:reply, :ok, state}
  end

  def handle_call({:delete_before, cutoff}, _from, state) do
    state = flush_pending(state)

    {count, deleted_ids} = do_delete_before_ets(cutoff, state.storage)

    state =
      if deleted_ids != [] do
        append_log(state, {:delete_blocks, System.monotonic_time(), deleted_ids})
      else
        state
      end

    {:reply, count, state}
  end

  def handle_call({:delete_over_size, max_bytes}, _from, state) do
    state = flush_pending(state)

    {count, deleted_ids} = do_delete_over_size_ets(max_bytes, state.storage)

    state =
      if deleted_ids != [] do
        append_log(state, {:delete_blocks, System.monotonic_time(), deleted_ids})
      else
        state
      end

    {:reply, count, state}
  end

  def handle_call({:delete_by_term_limit, max_entries}, _from, state) do
    current_size = :ets.info(@term_index_table, :size)

    if current_size <= max_entries do
      {:reply, 0, state}
    else
      state = flush_pending(state)

      {count, deleted_ids} = do_delete_by_term_limit_ets(max_entries, state.storage)

      state =
        if deleted_ids != [] do
          append_log(state, {:delete_blocks, System.monotonic_time(), deleted_ids})
        else
          state
        end

      {:reply, count, state}
    end
  end

  def handle_call({:read_block_data, block_id}, _from, state) do
    state = flush_pending(state)
    result = read_block_from_ets(block_id)
    {:reply, result, state}
  end

  def handle_call({:compact_blocks, old_ids, new_terms_list, compression_sizes}, _from, state) do
    state = flush_pending(state)

    # Collect old terms for cache cleanup
    old_terms = collect_terms_for_blocks_ets(old_ids)

    # Delete old blocks from ETS + disk files
    delete_blocks_ets(old_ids, state.storage)
    remove_block_ids_from_index(old_ids, old_terms)

    # Insert new blocks
    for {meta, _entries, terms} <- new_terms_list do
      insert_block_ets(meta)
      insert_index_entries(terms, meta.block_id)

      if state.storage == :memory and meta[:data] do
        :ets.insert(@block_data_table, {meta.block_id, meta[:data]})
      end
    end

    # Update compression stats
    {raw_in, compressed_out} = compression_sizes
    update_compression_stats_ets(raw_in, compressed_out)

    # Append to log
    state =
      append_log(state, {
        :compact_blocks,
        System.monotonic_time(),
        old_ids,
        Enum.map(new_terms_list, fn {meta, _entries, terms} ->
          {block_meta_to_map(meta), terms}
        end),
        compression_sizes
      })

    {:reply, :ok, state}
  end

  def handle_call({:backup, target_path}, _from, state) do
    state = flush_pending(state)
    result = write_snapshot(target_path)
    {:reply, result, state}
  end

  def handle_call(:sync, _from, state) do
    state = flush_pending(state)

    if state.log_ref do
      :disk_log.sync(state.log_ref)
    end

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

  # --- ETS initialization ---

  defp init_ets_tables do
    safe_delete_ets(@blocks_table)
    safe_delete_ets(@term_index_table)
    safe_delete_ets(@compression_stats_table)
    safe_delete_ets(@block_data_table)

    :ets.new(@blocks_table, [
      :named_table,
      :ordered_set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@term_index_table, [
      :named_table,
      :bag,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@compression_stats_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    :ets.new(@block_data_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp safe_delete_ets(table) do
    try do
      :ets.delete(table)
    rescue
      ArgumentError -> :ok
    end
  end

  # --- Snapshot persistence ---

  defp write_snapshot(path) do
    snapshot = %{
      version: 1,
      timestamp: System.monotonic_time(),
      blocks: :ets.tab2list(@blocks_table),
      term_index: :ets.tab2list(@term_index_table),
      compression_stats: :ets.tab2list(@compression_stats_table),
      block_data: :ets.tab2list(@block_data_table)
    }

    binary = :erlang.term_to_binary(snapshot, [:compressed])
    tmp_path = path <> ".tmp"

    case File.write(tmp_path, binary) do
      :ok ->
        File.rename!(tmp_path, path)
        :ok

      {:error, reason} ->
        File.rm(tmp_path)
        {:error, reason}
    end
  end

  defp load_snapshot(path) do
    case File.read(path) do
      {:ok, binary} ->
        snapshot = :erlang.binary_to_term(binary)
        :ets.insert(@blocks_table, snapshot.blocks)
        :ets.insert(@term_index_table, snapshot.term_index)
        :ets.insert(@compression_stats_table, snapshot.compression_stats)
        :ets.insert(@block_data_table, Map.get(snapshot, :block_data, []))
        :persistent_term.put({__MODULE__, :snapshot_ts}, snapshot.timestamp)
        :ok

      {:error, :enoent} ->
        :persistent_term.put({__MODULE__, :snapshot_ts}, 0)
        :ok

      {:error, reason} ->
        require Logger
        Logger.warning("TimelessLogs: failed to load snapshot: #{inspect(reason)}")
        :persistent_term.put({__MODULE__, :snapshot_ts}, 0)
        :ok
    end
  end

  defp snapshot_timestamp do
    :persistent_term.get({__MODULE__, :snapshot_ts}, 0)
  end

  # --- Disk log ---

  defp open_disk_log(log_path) do
    log_name = :timeless_logs_index_log

    # Close any existing log with this name (e.g. from previous run)
    :disk_log.close(log_name)

    {:ok, ref} =
      :disk_log.open(
        name: log_name,
        file: String.to_charlist(log_path),
        type: :halt,
        format: :internal
      )

    ref
  end

  defp replay_log(log_ref, snapshot_ts) do
    replay_log_chunks(log_ref, :start, snapshot_ts, 0)
  end

  defp replay_log_chunks(log_ref, cont, snapshot_ts, count) do
    case :disk_log.chunk(log_ref, cont) do
      :eof ->
        count

      {next_cont, terms} ->
        new_count =
          Enum.reduce(terms, count, fn term, acc ->
            ts = elem(term, 1)

            if ts > snapshot_ts do
              apply_log_entry(term)
              acc + 1
            else
              acc
            end
          end)

        replay_log_chunks(log_ref, next_cont, snapshot_ts, new_count)

      {next_cont, terms, _bad_bytes} ->
        # Skip bad entries, replay good ones
        new_count =
          Enum.reduce(terms, count, fn term, acc ->
            ts = elem(term, 1)

            if ts > snapshot_ts do
              apply_log_entry(term)
              acc + 1
            else
              acc
            end
          end)

        replay_log_chunks(log_ref, next_cont, snapshot_ts, new_count)
    end
  end

  defp apply_log_entry({:index_block, _ts, meta_map, terms}) do
    insert_block_ets_from_map(meta_map)
    insert_index_entries(terms, meta_map.block_id)

    storage = :persistent_term.get({__MODULE__, :storage})

    if storage == :memory and meta_map[:data] do
      :ets.insert(@block_data_table, {meta_map.block_id, meta_map[:data]})
    end
  end

  defp apply_log_entry({:delete_blocks, _ts, block_ids}) do
    storage = :persistent_term.get({__MODULE__, :storage})

    for id <- block_ids do
      # Collect terms before deleting block
      terms = :ets.match_object(@term_index_table, {:_, id})

      # Delete block file if disk mode
      if storage == :disk do
        case :ets.lookup(@blocks_table, id) do
          [{_bid, file_path, _bs, _ec, _tsmin, _tsmax, _fmt, _ca}] when is_binary(file_path) ->
            File.rm(file_path)

          _ ->
            :ok
        end
      end

      :ets.delete(@blocks_table, id)
      :ets.delete(@block_data_table, id)

      for {term, block_id} <- terms do
        :ets.delete_object(@term_index_table, {term, block_id})
      end
    end
  end

  defp apply_log_entry({:compact_blocks, _ts, old_ids, new_meta_terms, compression_sizes}) do
    storage = :persistent_term.get({__MODULE__, :storage})

    # Delete old blocks
    for id <- old_ids do
      terms = :ets.match_object(@term_index_table, {:_, id})

      if storage == :disk do
        case :ets.lookup(@blocks_table, id) do
          [{_bid, file_path, _bs, _ec, _tsmin, _tsmax, _fmt, _ca}] when is_binary(file_path) ->
            File.rm(file_path)

          _ ->
            :ok
        end
      end

      :ets.delete(@blocks_table, id)
      :ets.delete(@block_data_table, id)

      for {term, block_id} <- terms do
        :ets.delete_object(@term_index_table, {term, block_id})
      end
    end

    # Insert new blocks
    for {meta_map, terms} <- new_meta_terms do
      insert_block_ets_from_map(meta_map)
      insert_index_entries(terms, meta_map.block_id)
    end

    # Update compression stats
    {raw_in, compressed_out} = compression_sizes
    update_compression_stats_ets(raw_in, compressed_out)
  end

  defp apply_log_entry({:update_compression_stats, _ts, raw_in, compressed_out}) do
    update_compression_stats_ets(raw_in, compressed_out)
  end

  defp append_log(%{log_ref: nil} = state, _entry), do: state

  defp append_log(state, entry) do
    :disk_log.log(state.log_ref, entry)
    new_count = state.log_entry_count + 1
    state = %{state | log_entry_count: new_count}
    maybe_snapshot(state)
  end

  defp maybe_snapshot(%{log_entry_count: count, snapshot_threshold: threshold} = state)
       when count >= threshold do
    write_snapshot(state.snapshot_path)
    :disk_log.truncate(state.log_ref)
    %{state | log_entry_count: 0}
  end

  defp maybe_snapshot(state), do: state

  # --- Cache update helpers ---

  defp block_meta_to_map(meta) do
    %{
      block_id: meta.block_id,
      file_path: meta[:file_path],
      byte_size: meta.byte_size,
      entry_count: meta.entry_count,
      ts_min: meta.ts_min,
      ts_max: meta.ts_max,
      format: Map.get(meta, :format, :zstd),
      data: meta[:data]
    }
  end

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

  defp insert_block_ets_from_map(meta_map) do
    format = Map.get(meta_map, :format, :zstd)
    created_at = System.system_time(:second)

    :ets.insert(@blocks_table, {
      meta_map.block_id,
      meta_map[:file_path],
      meta_map.byte_size,
      meta_map.entry_count,
      meta_map.ts_min,
      meta_map.ts_max,
      format,
      created_at
    })
  end

  defp insert_index_entries(terms, block_id) do
    if terms != [] do
      term_objects = Enum.map(terms, fn term -> {term, block_id} end)
      :ets.insert(@term_index_table, term_objects)
    end
  end

  # --- Lock-free read helpers ---

  defp term_block_ids(term) do
    @term_index_table
    |> :ets.lookup(term)
    |> Enum.map(fn {_term, bid} -> bid end)
    |> MapSet.new()
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

  # --- ETS-based deletion helpers ---

  defp collect_terms_for_blocks_ets(block_ids) do
    Enum.flat_map(block_ids, fn id ->
      :ets.match_object(@term_index_table, {:_, id})
    end)
  end

  defp remove_block_ids_from_index(_old_ids, old_terms) do
    for {term, block_id} <- old_terms do
      :ets.delete_object(@term_index_table, {term, block_id})
    end
  end

  defp delete_blocks_ets(block_ids, storage) do
    for id <- block_ids do
      if storage == :disk do
        case :ets.lookup(@blocks_table, id) do
          [{_bid, file_path, _bs, _ec, _tsmin, _tsmax, _fmt, _ca}] when is_binary(file_path) ->
            File.rm(file_path)

          _ ->
            :ok
        end
      end

      :ets.delete(@blocks_table, id)
      :ets.delete(@block_data_table, id)
    end
  end

  defp do_delete_before_ets(cutoff_timestamp, storage) do
    # Find blocks where ts_max < cutoff
    to_delete =
      :ets.foldl(
        fn {bid, _fp, _bs, _ec, _tsmin, ts_max, _fmt, _ca}, acc ->
          if ts_max < cutoff_timestamp, do: [bid | acc], else: acc
        end,
        [],
        @blocks_table
      )

    if to_delete == [] do
      {0, []}
    else
      old_terms = collect_terms_for_blocks_ets(to_delete)
      delete_blocks_ets(to_delete, storage)
      remove_block_ids_from_index(to_delete, old_terms)
      {length(to_delete), to_delete}
    end
  end

  defp do_delete_over_size_ets(max_bytes, storage) do
    total =
      :ets.foldl(
        fn {_bid, _fp, byte_size, _ec, _tsmin, _tsmax, _fmt, _ca}, acc -> acc + byte_size end,
        0,
        @blocks_table
      )

    if total <= max_bytes do
      {0, []}
    else
      # Get all blocks sorted by ts_min ascending
      rows =
        @blocks_table
        |> :ets.tab2list()
        |> Enum.sort_by(fn {_bid, _fp, _bs, _ec, ts_min, _tsmax, _fmt, _ca} -> ts_min end)

      {to_delete, _} =
        Enum.reduce_while(rows, {[], total}, fn {bid, _fp, byte_size, _ec, _tsmin, _tsmax, _fmt,
                                                 _ca},
                                                {acc, remaining} ->
          if remaining > max_bytes do
            {:cont, {[bid | acc], remaining - byte_size}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      if to_delete == [] do
        {0, []}
      else
        old_terms = collect_terms_for_blocks_ets(to_delete)
        delete_blocks_ets(to_delete, storage)
        remove_block_ids_from_index(to_delete, old_terms)
        {length(to_delete), to_delete}
      end
    end
  end

  defp do_delete_by_term_limit_ets(max_entries, storage) do
    current_size = :ets.info(@term_index_table, :size)

    if current_size <= max_entries do
      {0, []}
    else
      # Get all blocks with their term counts, sorted by ts_min
      rows =
        @blocks_table
        |> :ets.tab2list()
        |> Enum.map(fn {bid, _fp, _bs, _ec, ts_min, _tsmax, _fmt, _ca} ->
          term_count = length(:ets.match_object(@term_index_table, {:_, bid}))
          {ts_min, bid, term_count}
        end)
        |> Enum.sort_by(fn {ts_min, _bid, _tc} -> ts_min end)

      {to_delete, _} =
        Enum.reduce_while(rows, {[], current_size}, fn {_ts_min, bid, term_count},
                                                       {acc, remaining} ->
          if remaining > max_entries do
            {:cont, {[bid | acc], remaining - term_count}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      if to_delete == [] do
        {0, []}
      else
        old_terms = collect_terms_for_blocks_ets(to_delete)
        delete_blocks_ets(to_delete, storage)
        remove_block_ids_from_index(to_delete, old_terms)
        {length(to_delete), to_delete}
      end
    end
  end

  defp update_compression_stats_ets(raw_in, compressed_out) do
    if raw_in > 0 or compressed_out > 0 do
      case :ets.lookup(@compression_stats_table, :lifetime) do
        [{:lifetime, old_raw, old_comp, old_count}] ->
          :ets.insert(@compression_stats_table, {
            :lifetime,
            old_raw + raw_in,
            old_comp + compressed_out,
            old_count + 1
          })

        _ ->
          :ets.insert(@compression_stats_table, {:lifetime, raw_in, compressed_out, 1})
      end
    end
  end

  defp read_block_from_ets(block_id) do
    case :ets.lookup(@block_data_table, block_id) do
      [{^block_id, data}] when is_binary(data) ->
        # Get format from blocks table
        format =
          case :ets.lookup(@blocks_table, block_id) do
            [{_bid, _fp, _bs, _ec, _tsmin, _tsmax, fmt, _ca}] -> fmt
            _ -> :zstd
          end

        TimelessLogs.Writer.decompress_block(data, format)

      _ ->
        {:error, :not_found}
    end
  end

  # --- Pending flush helpers ---

  defp flush_pending(%{pending: []} = state), do: state

  defp flush_pending(%{pending: pending} = state) do
    resolved = Enum.reverse(pending)

    # Update ETS + log for each pending item
    state =
      Enum.reduce(resolved, state, fn {meta, _entries, terms}, acc ->
        insert_block_ets(meta)
        insert_index_entries(terms, meta.block_id)

        if acc.storage == :memory and meta[:data] do
          :ets.insert(@block_data_table, {meta.block_id, meta[:data]})
        end

        append_log(acc, {:index_block, System.monotonic_time(), block_meta_to_map(meta), terms})
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

  # --- SQLite migration ---

  defp migrate_from_sqlite(db_path, _data_dir) do
    if Code.ensure_loaded?(Exqlite.Sqlite3) do
      require Logger
      Logger.info("TimelessLogs: migrating from SQLite to ETS snapshots...")

      {:ok, db} = Exqlite.Sqlite3.open(db_path)

      # Bulk load blocks
      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(db, """
        SELECT block_id, file_path, byte_size, entry_count, ts_min, ts_max, format, created_at
        FROM blocks
        """)

      migrate_load_blocks(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      # Bulk load term index
      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(db, "SELECT term, block_id FROM block_terms")

      migrate_load_terms(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      # Bulk load compression stats
      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(
          db,
          "SELECT raw_bytes_in, compressed_bytes_out, compaction_count FROM compression_stats WHERE id = 1"
        )

      case Exqlite.Sqlite3.step(db, stmt) do
        {:row, [r, c, n]} ->
          :ets.insert(@compression_stats_table, {:lifetime, r, c, n})

        :done ->
          :ets.insert(@compression_stats_table, {:lifetime, 0, 0, 0})
      end

      Exqlite.Sqlite3.release(db, stmt)
      Exqlite.Sqlite3.close(db)

      # Write snapshot from migrated ETS state
      snapshot_path = :persistent_term.get({__MODULE__, :snapshot_path})
      write_snapshot(snapshot_path)

      # Rename old db
      File.rename!(db_path, db_path <> ".migrated")
      Logger.info("TimelessLogs: migration complete, renamed index.db to index.db.migrated")
    else
      require Logger
      Logger.warning("TimelessLogs: index.db found but Exqlite not available for migration")
    end
  end

  defp migrate_load_blocks(db, stmt) do
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

        migrate_load_blocks(db, stmt)

      :done ->
        :ok
    end
  end

  defp migrate_load_terms(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [term, block_id]} ->
        :ets.insert(@term_index_table, {term, block_id})
        migrate_load_terms(db, stmt)

      :done ->
        :ok
    end
  end

  # --- Indexing ---

  @spec extract_terms([map()]) :: [String.t()]
  def extract_terms(entries) do
    entries
    |> Enum.reduce(MapSet.new(), fn entry, acc ->
      acc = MapSet.put(acc, "level:#{entry.level}")

      Enum.reduce(entry.metadata, acc, fn {k, v}, inner_acc ->
        if is_binary(v) or is_atom(v) or is_number(v) do
          MapSet.put(inner_acc, "#{k}:#{v}")
        else
          inner_acc
        end
      end)
    end)
    |> MapSet.to_list()
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

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end
end
