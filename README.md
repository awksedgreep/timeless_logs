# TimelessLogs

Embedded log compression and indexing for Elixir applications. Add one dependency, configure a data directory, and your app gets compressed, searchable logs with zero external infrastructure.

Logs are written to raw blocks, automatically compacted with zstd (~11x compression ratio), and indexed in SQLite for fast querying. Includes optional real-time subscriptions and a VictoriaLogs-compatible HTTP API.

## Installation

```elixir
def deps do
  [
    {:timeless_logs, "~> 0.6"}
  ]
end
```

## Setup

```elixir
# config/config.exs
config :timeless_logs,
  data_dir: "priv/timeless_logs"
```

That's it. TimelessLogs installs itself as a `:logger` handler on application start. All `Logger` calls are automatically captured, compressed, and indexed.

## Querying

```elixir
# Recent errors
TimelessLogs.query(level: :error, since: DateTime.add(DateTime.utc_now(), -3600))

# Search by metadata
TimelessLogs.query(level: :info, metadata: %{request_id: "abc123"})

# Substring match on message
TimelessLogs.query(message: "timeout")

# Pagination
TimelessLogs.query(level: :warning, limit: 50, offset: 100, order: :asc)
```

Returns a `TimelessLogs.Result` struct:

```elixir
{:ok, %TimelessLogs.Result{
  entries: [%TimelessLogs.Entry{timestamp: ..., level: :error, message: "...", metadata: %{}}],
  total: 42,
  limit: 100,
  offset: 0
}}
```

### Query Filters

| Filter | Type | Description |
|--------|------|-------------|
| `:level` | atom | `:debug`, `:info`, `:warning`, or `:error` |
| `:message` | string | Case-insensitive substring match on message and metadata values |
| `:since` | DateTime or integer | Lower time bound (integers are unix timestamps) |
| `:until` | DateTime or integer | Upper time bound |
| `:metadata` | map | Exact match on key/value pairs |
| `:limit` | integer | Max entries to return (default 100) |
| `:offset` | integer | Skip N entries (default 0) |
| `:order` | atom | `:asc` (oldest first) or `:desc` (newest first, default) |

## Streaming

For memory-efficient access to large result sets, use `stream/1`. Blocks are decompressed on demand as the stream is consumed:

```elixir
TimelessLogs.stream(level: :error)
|> Enum.take(10)

TimelessLogs.stream(since: DateTime.add(DateTime.utc_now(), -86400))
|> Stream.filter(fn entry -> String.contains?(entry.message, "timeout") end)
|> Enum.to_list()
```

## Real-Time Subscriptions

Subscribe to log entries as they arrive:

```elixir
TimelessLogs.subscribe(level: :error)

# Entries arrive as messages
receive do
  {:timeless_logs, :entry, %TimelessLogs.Entry{} = entry} ->
    IO.puts("Got error: #{entry.message}")
end

# Stop subscribing
TimelessLogs.unsubscribe()
```

You can filter subscriptions by level and metadata:

```elixir
TimelessLogs.subscribe(level: :warning, metadata: %{service: "payments"})
```

## Statistics

Get aggregate storage statistics without reading blocks:

```elixir
{:ok, stats} = TimelessLogs.stats()

# %TimelessLogs.Stats{
#   total_blocks: 48,
#   total_entries: 125_000,
#   total_bytes: 24_000_000,
#   disk_size: 24_000_000,
#   index_size: 3_200_000,
#   oldest_timestamp: 1700000000000000,
#   newest_timestamp: 1700086400000000,
#   raw_blocks: 2,
#   raw_bytes: 50_000,
#   raw_entries: 500,
#   zstd_blocks: 46,
#   zstd_bytes: 23_950_000,
#   zstd_entries: 124_500
# }
```

## Backup

Create a consistent online backup without stopping the application:

```elixir
{:ok, result} = TimelessLogs.backup("/tmp/logs_backup")

# %{path: "/tmp/logs_backup", files: [...], total_bytes: 24_000_000}
```

Uses SQLite `VACUUM INTO` for an atomic index snapshot and copies block files in parallel.

## Retention

Configure automatic cleanup to prevent unbounded disk growth:

```elixir
config :timeless_logs,
  data_dir: "priv/timeless_logs",
  retention_max_age: 7 * 24 * 3600,       # Delete logs older than 7 days
  retention_max_size: 512 * 1024 * 1024,   # Keep total blocks under 512 MB
  retention_check_interval: 300_000         # Check every 5 minutes (default)
```

You can also trigger cleanup manually:

```elixir
TimelessLogs.Retention.run_now()
```

## Compaction

New log entries are first written as uncompressed raw blocks for low-latency ingestion. A background compactor periodically merges raw blocks into zstd-compressed blocks:

```elixir
config :timeless_logs,
  compaction_threshold: 500,       # Min raw entries to trigger compaction
  compaction_interval: 30_000,     # Check every 30 seconds
  compaction_max_raw_age: 60,      # Force compact raw blocks older than 60s
  compression_level: 5             # zstd level 1-22 (default 5)
```

Trigger manually:

```elixir
TimelessLogs.Compactor.compact_now()
```

## HTTP API

TimelessLogs includes an optional HTTP API compatible with VictoriaLogs. Enable it in config:

```elixir
config :timeless_logs,
  http: [port: 9428, bearer_token: "secret"]
```

Or simply `http: true` to use defaults (port 9428, no auth).

### Endpoints

**Health check** (always accessible, no auth required):

```
GET /health
→ {"status": "ok", "blocks": 48, "entries": 125000, "disk_size": 24000000}
```

**Ingest** (NDJSON, one JSON object per line):

```
POST /insert/jsonline?_msg_field=_msg&_time_field=_time

{"_msg": "Request completed", "_time": "2024-01-15T10:30:00Z", "level": "info", "request_id": "abc123"}
{"_msg": "Connection timeout", "level": "error", "service": "api"}
```

**Query**:

```
GET /select/logsql/query?level=error&start=2024-01-15T00:00:00Z&limit=50
→ NDJSON response, one entry per line
```

**Stats**:

```
GET /select/logsql/stats
→ {"total_blocks": 48, "total_entries": 125000, ...}
```

**Flush buffer**:

```
GET /api/v1/flush
→ {"status": "ok"}
```

**Backup**:

```
POST /api/v1/backup
Content-Type: application/json
{"path": "/tmp/backup"}

→ {"status": "ok", "path": "/tmp/backup", "files": [...], "total_bytes": 24000000}
```

### Authentication

When `bearer_token` is configured, all endpoints except `/health` require either:

- Header: `Authorization: Bearer <token>`
- Query param: `?token=<token>`

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `"priv/log_stream"` | Root directory for blocks and index |
| `storage` | `:disk` | Storage backend (`:disk` or `:memory`) |
| `flush_interval` | `1_000` | Buffer flush interval in ms |
| `max_buffer_size` | `1_000` | Max entries before auto-flush |
| `query_timeout` | `30_000` | Query timeout in ms |
| `compression_level` | `5` | zstd compression level (1-22) |
| `compaction_threshold` | `500` | Min raw entries to trigger compaction |
| `compaction_interval` | `30_000` | Compaction check interval in ms |
| `compaction_max_raw_age` | `60` | Force compact raw blocks older than this (seconds) |
| `retention_max_age` | `7 * 86_400` | Max log age in seconds (`nil` = keep forever) |
| `retention_max_size` | `512 * 1_048_576` | Max block storage in bytes (`nil` = unlimited) |
| `retention_check_interval` | `300_000` | Retention check interval in ms |
| `http` | `false` | Enable HTTP API (`true`, or keyword list with `:port` and `:bearer_token`) |

## Telemetry

TimelessLogs emits telemetry events for monitoring:

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:timeless_logs, :flush, :stop]` | `duration`, `entry_count`, `byte_size` | `block_id` |
| `[:timeless_logs, :query, :stop]` | `duration`, `total`, `blocks_read` | `filters` |
| `[:timeless_logs, :retention, :stop]` | `duration`, `blocks_deleted` | |
| `[:timeless_logs, :compaction, :stop]` | `duration`, `raw_blocks`, `entry_count`, `byte_size` | |
| `[:timeless_logs, :block, :error]` | | `file_path`, `reason` |

## How It Works

1. Your app logs normally via `Logger`
2. TimelessLogs captures log events via an OTP `:logger` handler
3. Events buffer in a GenServer, flushing every 1s or 1000 entries
4. Each flush writes a raw (uncompressed) block file
5. A background compactor merges raw blocks into zstd-compressed blocks (~11x ratio)
6. Block metadata and an inverted index of terms are stored in SQLite (WAL mode)
7. Queries hit the SQLite index to find relevant blocks, decompress only those in parallel, and filter entries
8. Real-time subscribers receive matching entries as they're buffered

## Benchmarks

**Ingestion throughput** (500K entries, 1000 entries/block):

| Phase | Throughput |
|---|---|
| Writer only (serialization + disk I/O) | ~159K entries/sec |
| Writer + Index (sync SQLite indexing) | ~45K entries/sec |
| Full pipeline (Buffer → Writer → async Index) | ~88K entries/sec |

On a simulated week of Phoenix logs (~1.1M entries, ~30 req/min):

**Compression** (zstd level 5, 1.1M simulated Phoenix log entries):

| Metric | Value |
|--------|-------|
| Compression ratio | 11.2x |
| Raw size | 246 MB |
| Compressed size | 22 MB |
| Compression throughput | 500K entries/sec |

**Columnar+OpenZL** (per-column compression with reusable contexts):

| Level | Ratio | Throughput |
|-------|-------|-----------|
| 1 | 10.9x | 1.7M entries/sec |
| 5 | 11.4x | 1.2M entries/sec |
| 9 | 12.5x | 763K entries/sec |
| 19 | 14.0x | 22.6K entries/sec |

**Query latency** (1.1M entries indexed, 5 iterations, median):

| Query | Median |
|-------|--------|
| Specific request_id | 0.6ms |
| Last 1h + level=error | 2.4ms |
| Last 1 hour (all levels) | 4.4ms |
| level=error (all time) | 226ms |
| Message substring search | 420ms |
| Last 24 hours | 244ms |
| All logs, no filters | 1.4s |

## License

MIT - see [LICENSE](LICENSE) for details.
