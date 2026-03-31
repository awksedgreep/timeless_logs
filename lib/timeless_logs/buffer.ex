defmodule TimelessLogs.Buffer do
  @moduledoc false

  use GenServer

  require Logger

  @max_in_flight System.schedulers_online()
  @type buffer_state :: %{
          buffer: [map()],
          buffer_size: non_neg_integer(),
          data_dir: String.t(),
          flush_interval: pos_integer(),
          in_flight: non_neg_integer(),
          pending_batches: :queue.queue([map()]),
          flush_waiters: [GenServer.from()]
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec log(map()) :: :ok
  def log(entry) do
    GenServer.cast(__MODULE__, {:log, entry})
  end

  @spec log_many([map()]) :: :ok
  def log_many(entries) when is_list(entries) do
    case entries do
      [] -> :ok
      _ -> GenServer.cast(__MODULE__, {:log_many, entries})
    end
  end

  @spec flush() :: :ok
  def flush do
    GenServer.call(__MODULE__, :flush, TimelessLogs.Config.query_timeout())
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    interval = TimelessLogs.Config.flush_interval()
    schedule_flush(interval)

    :logger.add_handler(TimelessLogs.Handler.handler_id(), TimelessLogs.Handler, %{level: :all})

    {:ok,
     %{
       buffer: [],
       buffer_size: 0,
       data_dir: data_dir,
       flush_interval: interval,
       in_flight: 0,
       pending_batches: :queue.new(),
       flush_waiters: []
     }}
  end

  @impl true
  def terminate(_reason, state) do
    :logger.remove_handler(TimelessLogs.Handler.handler_id())
    do_flush(state.buffer, state.data_dir, sync: true)
    :ok
  end

  @impl true
  def handle_cast({:log, entry}, state) do
    broadcast_to_subscribers(entry)
    buffer = [entry | state.buffer]
    size = state.buffer_size + 1

    if size >= TimelessLogs.Config.max_buffer_size() do
      state = dispatch_or_queue_batch(Enum.reverse(buffer), state)
      {:noreply, %{state | buffer: [], buffer_size: 0}}
    else
      {:noreply, %{state | buffer: buffer, buffer_size: size}}
    end
  end

  def handle_cast({:log_many, entries}, state) do
    Enum.each(entries, &broadcast_to_subscribers/1)
    buffer = Enum.reverse(entries, state.buffer)
    size = state.buffer_size + length(entries)

    if size >= TimelessLogs.Config.max_buffer_size() do
      state = dispatch_or_queue_batch(Enum.reverse(buffer), state)
      {:noreply, %{state | buffer: [], buffer_size: 0}}
    else
      {:noreply, %{state | buffer: buffer, buffer_size: size}}
    end
  end

  @impl true
  def handle_call(:flush, from, state) do
    state =
      if state.buffer != [] do
        do_flush(state.buffer, state.data_dir, sync: true)
        %{state | buffer: [], buffer_size: 0}
      else
        state
      end

    state = dispatch_queued_batches(state)

    if idle?(state) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | flush_waiters: [from | state.flush_waiters]}}
    end
  end

  @impl true
  def handle_info(:flush_timer, state) do
    state =
      if state.buffer != [] do
        dispatch_or_queue_batch(Enum.reverse(state.buffer), state)
      else
        state
      end

    schedule_flush(state.flush_interval)
    {:noreply, %{state | buffer: [], buffer_size: 0}}
  end

  def handle_info({:flush_done, _ref}, state) do
    state = %{state | in_flight: max(state.in_flight - 1, 0)}
    {:noreply, state |> dispatch_queued_batches() |> maybe_reply_flush_waiters()}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    state = %{state | in_flight: max(state.in_flight - 1, 0)}
    {:noreply, state |> dispatch_queued_batches() |> maybe_reply_flush_waiters()}
  end

  defp dispatch_or_queue_batch([], state), do: state

  defp dispatch_or_queue_batch(entries, state) do
    if state.in_flight < @max_in_flight do
      start_flush_task(state, entries)
    else
      %{state | pending_batches: :queue.in(entries, state.pending_batches)}
    end
  end

  defp dispatch_queued_batches(state) do
    if state.in_flight >= @max_in_flight do
      state
    else
      case :queue.out(state.pending_batches) do
        {{:value, entries}, rest} ->
          state
          |> Map.put(:pending_batches, rest)
          |> start_flush_task(entries)
          |> dispatch_queued_batches()

        {:empty, _queue} ->
          state
      end
    end
  end

  defp start_flush_task(state, entries) do
    data_dir = state.data_dir
    caller = self()

    Task.Supervisor.start_child(TimelessLogs.FlushSupervisor, fn ->
      do_flush_work(entries, data_dir)
      send(caller, {:flush_done, make_ref()})
    end)

    %{state | in_flight: state.in_flight + 1}
  end

  defp maybe_reply_flush_waiters(state) do
    if idle?(state) and state.flush_waiters != [] do
      Enum.each(state.flush_waiters, &GenServer.reply(&1, :ok))
      %{state | flush_waiters: []}
    else
      state
    end
  end

  defp idle?(state) do
    state.buffer == [] and state.in_flight == 0 and :queue.is_empty(state.pending_batches)
  end

  defp do_flush(buffer, data_dir, opts)
  defp do_flush([], _data_dir, _opts), do: :ok

  defp do_flush(buffer, data_dir, opts) do
    entries = Enum.reverse(buffer)
    do_flush_work(entries, data_dir, opts)
  end

  defp do_flush_work(entries, data_dir, opts \\ []) do
    start_time = System.monotonic_time()

    write_target = if TimelessLogs.Config.storage() == :memory, do: :memory, else: data_dir

    case TimelessLogs.Writer.write_block(entries, write_target, :raw) do
      {:ok, block_meta} ->
        terms = TimelessLogs.Index.extract_terms(entries)

        if Keyword.get(opts, :sync, false) do
          TimelessLogs.Index.index_block(block_meta, entries, terms)
        else
          TimelessLogs.Index.index_block_async(block_meta, entries, terms)
        end

        duration = System.monotonic_time() - start_time

        TimelessLogs.Telemetry.event(
          [:timeless_logs, :flush, :stop],
          %{
            duration: duration,
            entry_count: block_meta.entry_count,
            byte_size: block_meta.byte_size
          },
          %{block_id: block_meta.block_id}
        )

      {:error, reason} ->
        Logger.error("TimelessLogs: failed to write block: #{inspect(reason)}")

        TimelessLogs.Telemetry.event(
          [:timeless_logs, :flush, :error],
          %{entry_count: length(entries)},
          %{reason: reason}
        )
    end
  rescue
    e ->
      Logger.error("TimelessLogs: flush crashed: #{Exception.format(:error, e, __STACKTRACE__)}")

      TimelessLogs.Telemetry.event(
        [:timeless_logs, :flush, :error],
        %{entry_count: length(entries)},
        %{reason: e}
      )
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_timer, interval)
  end

  defp broadcast_to_subscribers(entry) do
    case Registry.count_match(TimelessLogs.Registry, :log_entries, :_) do
      0 ->
        :ok

      _n ->
        entry_struct = TimelessLogs.Entry.from_map(entry)

        Registry.dispatch(TimelessLogs.Registry, :log_entries, fn subscribers ->
          for {pid, opts} <- subscribers do
            if matches_subscription?(entry, opts) do
              send(pid, {:timeless_logs, :entry, entry_struct})
            end
          end
        end)
    end
  end

  defp matches_subscription?(_entry, []), do: true

  defp matches_subscription?(entry, opts) do
    Enum.all?(opts, fn
      {:level, level} ->
        entry.level == level

      {:metadata, map} ->
        Enum.all?(map, fn {k, v} ->
          Map.get(entry.metadata, to_string(k)) == to_string(v)
        end)

      _ ->
        true
    end)
  end
end
