defmodule TimelessLogs.Compactor do
  @moduledoc false

  use GenServer

  require Logger

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec compact_now() :: :ok | :noop
  def compact_now do
    GenServer.call(__MODULE__, :compact_now, 60_000)
  end

  @spec merge_now() :: :ok | :noop
  def merge_now do
    GenServer.call(__MODULE__, :merge_now, 60_000)
  end

  @impl true
  def init(opts) do
    storage = Keyword.get(opts, :storage, :disk)
    data_dir = Keyword.get(opts, :data_dir, TimelessLogs.Config.data_dir())
    interval = TimelessLogs.Config.compaction_interval()
    schedule(interval)

    {:ok, %{storage: storage, data_dir: data_dir, interval: interval}}
  end

  @impl true
  def handle_call(:compact_now, _from, state) do
    result = maybe_compact(state)
    {:reply, result, state}
  end

  def handle_call(:merge_now, _from, state) do
    result = maybe_merge_compact(state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:compaction_check, state) do
    maybe_compact(state)
    maybe_merge_compact(state)
    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval) do
    Process.send_after(self(), :compaction_check, interval)
  end

  defp maybe_compact(state) do
    stats = TimelessLogs.Index.raw_block_stats()
    threshold = TimelessLogs.Config.compaction_threshold()
    max_age = TimelessLogs.Config.compaction_max_raw_age()
    now = System.system_time(:second)

    age_exceeded =
      stats.oldest_created_at != nil and
        now - stats.oldest_created_at >= max_age and
        stats.block_count > 0

    if stats.entry_count >= threshold or age_exceeded do
      do_compact(state)
    else
      :noop
    end
  end

  defp do_compact(state) do
    start_time = System.monotonic_time()
    raw_blocks = TimelessLogs.Index.raw_block_ids()

    # Read all entries from raw blocks
    all_entries =
      Enum.flat_map(raw_blocks, fn {block_id, file_path, _bs} ->
        read_result =
          case state.storage do
            :disk -> TimelessLogs.Writer.read_block(file_path, :raw)
            :memory -> TimelessLogs.Index.read_block_data(block_id)
          end

        case read_result do
          {:ok, entries} -> entries
          {:error, _} -> []
        end
      end)

    if all_entries == [] do
      :noop
    else
      # Sum actual raw block file sizes (true "before compression" size)
      raw_bytes = Enum.reduce(raw_blocks, 0, fn {_bid, _fp, bs}, acc -> acc + bs end)

      write_target = if state.storage == :memory, do: :memory, else: state.data_dir
      concurrency = System.schedulers_online()

      chunks = Enum.chunk_every(all_entries, max(div(length(all_entries), concurrency), 1))

      results =
        chunks
        |> Task.async_stream(
          fn chunk ->
            case TimelessLogs.Writer.write_block(
                   chunk,
                   write_target,
                   TimelessLogs.Config.compaction_format()
                 ) do
              {:ok, meta} -> {:ok, meta, chunk}
              error -> error
            end
          end,
          max_concurrency: concurrency,
          ordered: false
        )
        |> Enum.reduce({[], 0}, fn
          {:ok, {:ok, meta, chunk}}, {pairs, bytes} ->
            {[{meta, chunk} | pairs], bytes + meta.byte_size}

          _, acc ->
            acc
        end)

      case results do
        {[], _} ->
          Logger.warning("TimelessLogs: compaction failed: all chunks errored")
          :noop

        {meta_chunk_pairs, total_bytes} ->
          old_ids = Enum.map(raw_blocks, &elem(&1, 0))

          new_terms_list =
            Enum.map(meta_chunk_pairs, fn {meta, entries} ->
              {meta, entries, TimelessLogs.Index.extract_terms(entries)}
            end)

          TimelessLogs.Index.compact_blocks(old_ids, new_terms_list, {raw_bytes, total_bytes})

          duration = System.monotonic_time() - start_time

          TimelessLogs.Telemetry.event(
            [:timeless_logs, :compaction, :stop],
            %{
              duration: duration,
              raw_blocks: length(raw_blocks),
              entry_count: length(all_entries),
              byte_size: total_bytes
            },
            %{}
          )

          :ok
      end
    end
  end

  # --- Merge compaction ---

  defp maybe_merge_compact(state) do
    target = TimelessLogs.Config.merge_compaction_target_size()
    min_blocks = TimelessLogs.Config.merge_compaction_min_blocks()
    small_blocks = TimelessLogs.Index.small_compressed_block_ids(target)

    if length(small_blocks) >= min_blocks do
      do_merge_compact(state, small_blocks, target)
    else
      :noop
    end
  end

  defp do_merge_compact(state, small_blocks, target_size) do
    start_time = System.monotonic_time()
    batches = group_into_batches(small_blocks, target_size)

    merged_count =
      Enum.reduce(batches, 0, fn batch, acc ->
        case merge_batch(state, batch) do
          :ok -> acc + 1
          :noop -> acc
        end
      end)

    if merged_count > 0 do
      duration = System.monotonic_time() - start_time

      TimelessLogs.Telemetry.event(
        [:timeless_logs, :merge_compaction, :stop],
        %{
          duration: duration,
          batches_merged: merged_count,
          blocks_consumed: length(small_blocks)
        },
        %{}
      )

      :ok
    else
      :noop
    end
  end

  defp group_into_batches(blocks, target_size) do
    {batches, current} =
      Enum.reduce(blocks, {[], []}, fn {_bid, _fp, _bs, ec} = block, {batches, current} ->
        current_count = Enum.reduce(current, 0, fn {_, _, _, e}, a -> a + e end)

        if current_count + ec > target_size and current != [] do
          {[current | batches], [block]}
        else
          {batches, current ++ [block]}
        end
      end)

    # Only include the last batch if it has >= 2 blocks
    all = if length(current) >= 2, do: [current | batches], else: batches
    Enum.reverse(all)
  end

  defp merge_batch(state, batch) do
    all_entries =
      Enum.flat_map(batch, fn {block_id, file_path, _bs, _ec} ->
        read_result =
          case state.storage do
            :disk -> TimelessLogs.Writer.read_block(file_path, format_from_path(file_path))
            :memory -> TimelessLogs.Index.read_block_data(block_id)
          end

        case read_result do
          {:ok, entries} -> entries
          {:error, _} -> []
        end
      end)

    if all_entries == [] do
      :noop
    else
      old_bytes = Enum.reduce(batch, 0, fn {_, _, bs, _}, a -> a + bs end)
      write_target = if state.storage == :memory, do: :memory, else: state.data_dir

      case TimelessLogs.Writer.write_block(
             all_entries,
             write_target,
             TimelessLogs.Config.compaction_format()
           ) do
        {:ok, new_meta} ->
          old_ids = Enum.map(batch, &elem(&1, 0))
          terms = TimelessLogs.Index.extract_terms(all_entries)

          TimelessLogs.Index.compact_blocks(
            old_ids,
            [{new_meta, all_entries, terms}],
            {old_bytes, new_meta.byte_size}
          )

          :ok

        {:error, _} ->
          :noop
      end
    end
  end

  defp format_from_path(nil), do: :raw

  defp format_from_path(path) do
    case Path.extname(path) do
      ".ozl" -> :openzl
      ".zst" -> :zstd
      ".raw" -> :raw
      _ -> :raw
    end
  end
end
