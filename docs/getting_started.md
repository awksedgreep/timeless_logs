# Getting Started

This guide walks you through installing TimelessLogs, writing your first log, and querying it back.

## Installation

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:timeless_logs, "~> 0.10"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

## Minimal configuration

TimelessLogs only requires a data directory:

```elixir
# config/config.exs
config :timeless_logs,
  data_dir: "priv/timeless_logs"
```

That's it. TimelessLogs installs itself as an Elixir `:logger` handler on application start. All `Logger` calls are automatically captured, compressed, and indexed.

## Writing your first log

Since TimelessLogs integrates with Elixir's Logger, just log normally:

```elixir
require Logger

Logger.info("Application started")
Logger.error("Connection timeout", service: "api", request_id: "abc123")
Logger.warning("Memory usage high", host: "web-1", usage_pct: 92)
```

Metadata key/value pairs are automatically captured and indexed for fast querying.

### Via HTTP

If you enable the HTTP API, you can also ingest logs via NDJSON:

```elixir
# config/config.exs
config :timeless_logs,
  data_dir: "priv/timeless_logs",
  http: true  # port 9428, no auth
```

```bash
curl -X POST http://localhost:9428/insert/jsonline -d \
  '{"_msg": "Request completed", "_time": "2024-01-15T10:30:00Z", "level": "info", "request_id": "abc123"}'
```

## Querying logs

### Elixir API

```elixir
# Recent errors
{:ok, result} = TimelessLogs.query(level: :error)
# => {:ok, %TimelessLogs.Result{entries: [...], total: 42, limit: 100, offset: 0}}

# Errors from the last hour
{:ok, result} = TimelessLogs.query(
  level: :error,
  since: DateTime.add(DateTime.utc_now(), -3600))

# Search by metadata
{:ok, result} = TimelessLogs.query(metadata: %{request_id: "abc123"})

# Substring search on messages
{:ok, result} = TimelessLogs.query(message: "timeout")

# Combined filters with pagination
{:ok, result} = TimelessLogs.query(
  level: :warning,
  message: "memory",
  limit: 50,
  offset: 0,
  order: :asc)
```

Each entry in the result is a `TimelessLogs.Entry` struct:

```elixir
%TimelessLogs.Entry{
  timestamp: 1700000000000000,  # microseconds
  level: :error,
  message: "Connection timeout",
  metadata: %{"service" => "api", "request_id" => "abc123"}
}
```

### Via HTTP

```bash
# Recent errors
curl 'http://localhost:9428/select/logsql/query?level=error&limit=50'

# Time range query
curl 'http://localhost:9428/select/logsql/query?level=error&start=2024-01-15T00:00:00Z&end=2024-01-16T00:00:00Z'

# Message search
curl 'http://localhost:9428/select/logsql/query?message=timeout'
```

## Streaming large result sets

For memory-efficient access, use `stream/1`. Blocks are decompressed on demand:

```elixir
TimelessLogs.stream(level: :error)
|> Enum.take(10)

TimelessLogs.stream(since: DateTime.add(DateTime.utc_now(), -86400))
|> Stream.filter(fn entry -> String.contains?(entry.message, "timeout") end)
|> Enum.to_list()
```

## Checking storage stats

```elixir
{:ok, stats} = TimelessLogs.stats()
# => %TimelessLogs.Stats{total_blocks: 48, total_entries: 125_000, disk_size: 24_000_000, ...}
```

## Next steps

- [Configuration Reference](configuration.md) -- all config options and tuning guidance
- [Architecture](architecture.md) -- how the storage engine works
- [Querying](querying.md) -- full query API with filters, streaming, and pagination
- [HTTP API](http_api.md) -- VictoriaLogs-compatible HTTP endpoints
- [Real-Time Subscriptions](subscriptions.md) -- subscribe to live log entries
- [Storage & Compression](storage.md) -- block formats, compaction, and compression
- [Operations](operations.md) -- backup, retention, monitoring, and troubleshooting
- Interactive exploration: run the User's Guide livebook at `livebook/users_guide.livemd`
