defmodule LogStream.Buffer do
  @moduledoc false

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec log(map()) :: :ok
  def log(entry) do
    GenServer.cast(__MODULE__, {:log, entry})
  end

  @spec flush() :: :ok
  def flush do
    GenServer.call(__MODULE__, :flush, LogStream.Config.query_timeout())
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    interval = LogStream.Config.flush_interval()
    schedule_flush(interval)

    :logger.add_handler(LogStream.Handler.handler_id(), LogStream.Handler, %{level: :all})

    {:ok, %{buffer: [], data_dir: data_dir, flush_interval: interval}}
  end

  @impl true
  def terminate(_reason, state) do
    :logger.remove_handler(LogStream.Handler.handler_id())
    do_flush(state.buffer, state.data_dir)
    :ok
  end

  @impl true
  def handle_cast({:log, entry}, state) do
    broadcast_to_subscribers(entry)
    buffer = [entry | state.buffer]

    if length(buffer) >= LogStream.Config.max_buffer_size() do
      do_flush(buffer, state.data_dir)
      {:noreply, %{state | buffer: []}}
    else
      {:noreply, %{state | buffer: buffer}}
    end
  end

  @impl true
  def handle_call(:flush, _from, state) do
    do_flush(state.buffer, state.data_dir)
    {:reply, :ok, %{state | buffer: []}}
  end

  @impl true
  def handle_info(:flush_timer, state) do
    if state.buffer != [] do
      do_flush(state.buffer, state.data_dir)
    end

    schedule_flush(state.flush_interval)
    {:noreply, %{state | buffer: []}}
  end

  defp do_flush([], _data_dir), do: :ok

  defp do_flush(buffer, data_dir) do
    entries = Enum.reverse(buffer)
    start_time = System.monotonic_time()

    write_target = if LogStream.Config.storage() == :memory, do: :memory, else: data_dir

    case LogStream.Writer.write_block(entries, write_target, :raw) do
      {:ok, block_meta} ->
        LogStream.Index.index_block(block_meta, entries)
        duration = System.monotonic_time() - start_time

        LogStream.Telemetry.event(
          [:log_stream, :flush, :stop],
          %{
            duration: duration,
            entry_count: block_meta.entry_count,
            byte_size: block_meta.byte_size
          },
          %{block_id: block_meta.block_id}
        )

      {:error, reason} ->
        IO.warn("LogStream: failed to write block: #{inspect(reason)}")
    end
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_timer, interval)
  end

  defp broadcast_to_subscribers(entry) do
    entry_struct = LogStream.Entry.from_map(entry)

    Registry.dispatch(LogStream.Registry, :log_entries, fn subscribers ->
      for {pid, opts} <- subscribers do
        if matches_subscription?(entry, opts) do
          send(pid, {:log_stream, :entry, entry_struct})
        end
      end
    end)
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
