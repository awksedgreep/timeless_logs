# Real-Time Subscriptions

TimelessLogs supports real-time log subscriptions. Subscriber processes receive log entries as they arrive, before they're written to disk.

## Subscribing

```elixir
TimelessLogs.subscribe()
```

The calling process will receive messages of the form:

```elixir
{:timeless_logs, :entry, %TimelessLogs.Entry{}}
```

### Filtered subscriptions

Filter by log level and/or metadata:

```elixir
# Only receive errors
TimelessLogs.subscribe(level: :error)

# Only receive entries matching specific metadata
TimelessLogs.subscribe(metadata: %{service: "payments"})

# Both
TimelessLogs.subscribe(level: :error, metadata: %{service: "payments"})
```

## Receiving entries

```elixir
TimelessLogs.subscribe(level: :error)

receive do
  {:timeless_logs, :entry, %TimelessLogs.Entry{} = entry} ->
    IO.puts("[#{entry.level}] #{entry.message}")
end
```

### In a GenServer

```elixir
defmodule MyApp.LogWatcher do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    TimelessLogs.subscribe(level: :error)
    {:ok, %{}}
  end

  @impl true
  def handle_info({:timeless_logs, :entry, entry}, state) do
    # React to errors: send alert, increment counter, etc.
    IO.puts("ERROR: #{entry.message}")
    {:noreply, state}
  end
end
```

### In a LiveView

```elixir
defmodule MyAppWeb.LogLive do
  use Phoenix.LiveView

  def mount(_params, _session, socket) do
    if connected?(socket) do
      TimelessLogs.subscribe(level: :error)
    end

    {:ok, assign(socket, entries: [])}
  end

  def handle_info({:timeless_logs, :entry, entry}, socket) do
    entries = [entry | socket.assigns.entries] |> Enum.take(100)
    {:noreply, assign(socket, entries: entries)}
  end
end
```

## Unsubscribing

```elixir
TimelessLogs.unsubscribe()
```

Subscriptions are automatically cleaned up when the subscriber process exits.

## How it works

Subscriptions use an Elixir `Registry` with `:duplicate` keys. When the Buffer receives a new log entry, it broadcasts to all registered subscribers before buffering. This means subscribers see entries immediately, even before they're flushed to disk.

Filter matching happens at broadcast time -- subscribers only receive entries that match their filter criteria.

## Performance considerations

- Subscription delivery is synchronous with the Buffer's log path. A slow subscriber can affect ingest throughput.
- For high-volume log streams, consider filtering at the subscription level (`:level` and `:metadata`) rather than in the subscriber's `handle_info`.
- If you need to do expensive work in response to log entries, send yourself a message and process it asynchronously.
