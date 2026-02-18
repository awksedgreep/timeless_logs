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

  @impl true
  def handle_info(:compaction_check, state) do
    maybe_compact(state)
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
      Enum.flat_map(raw_blocks, fn {block_id, file_path} ->
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
          TimelessLogs.Index.compact_blocks(old_ids, meta_chunk_pairs)

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
end
