<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/logo-light.svg">
    <img src="docs/logo-light.svg" width="300" alt="Timeless">
  </picture>
</p>

<h3 align="center">Embedded Log Compression & Indexing for Elixir</h3>

<p align="center">
  <a href="https://hex.pm/packages/timeless_logs"><img src="https://img.shields.io/hexpm/v/timeless_logs.svg" alt="Hex.pm"></a>
  <a href="https://hexdocs.pm/timeless_logs"><img src="https://img.shields.io/badge/docs-hexdocs-blue.svg" alt="Docs"></a>
  <a href="LICENSE"><img src="https://img.shields.io/hexpm/l/timeless_logs.svg" alt="License"></a>
</p>

---

> "I found it ironic that the first thing you do to time series data is squash the timestamp. That's how the name Timeless was born." --Mark Cotner

Embedded log compression and indexing for Elixir applications. Add one dependency, configure a data directory, and your app gets compressed, searchable logs with zero external infrastructure.

Logs are written to raw blocks, automatically compacted with OpenZL (~12.8x compression ratio), and indexed in SQLite for crash-safe persistence. The index keeps level terms plus a curated set of low-cardinality metadata terms, while message substring search still scans message text and metadata values inside matching blocks. Includes optional real-time subscriptions and a VictoriaLogs-compatible HTTP API.

## Documentation

- [Getting Started](docs/getting_started.md)
- [Configuration Reference](docs/configuration.md)
- [Architecture](docs/architecture.md)
- [Querying](docs/querying.md)
- [HTTP API](docs/http_api.md)
- [Real-Time Subscriptions](docs/subscriptions.md)
- [Storage & Compression](docs/storage.md)
- [Operations](docs/operations.md)
- [Telemetry](docs/telemetry.md)

## Installation

```elixir
def deps do
  [
    {:timeless_logs, "~> 1.0"}
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

# Search by indexed metadata
TimelessLogs.query(level: :info, metadata: %{service: "payments"})

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

### Semantic Host and Service Filtering

`TimelessLogs` supports semantic host/service filtering so callers do not need
to hardcode one raw metadata key for every producer.

When you query with:

```elixir
TimelessLogs.query(metadata: %{host: "web-01"})
TimelessLogs.query(metadata: %{service: "payments"})
```

TimelessLogs expands those filters across common aliases:

- `host` matches `host.name`, `host`, `hostname`, and `node`
- `service` matches `service.name`, `service`, and `application`

The same semantic expansion applies to real-time subscriptions:

```elixir
TimelessLogs.subscribe(metadata: %{host: "web-01"})
TimelessLogs.subscribe(metadata: %{service: "payments"})
```

### Query Filters

| Filter | Type | Description |
|--------|------|-------------|
| `:level` | atom | `:debug`, `:info`, `:warning`, or `:error` |
| `:message` | string | Case-insensitive substring match on message and metadata values |
| `:since` | DateTime or integer | Lower time bound (integers are unix timestamps) |
| `:until` | DateTime or integer | Upper time bound |
| `:metadata` | map | Exact match on metadata key/value pairs, with semantic alias expansion for `host` and `service` |
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
#   openzl_blocks: 46,
#   openzl_bytes: 23_950_000,
#   openzl_entries: 124_500
# }
```

## Backup

Create a consistent online backup without stopping the application:

```elixir
{:ok, result} = TimelessLogs.backup("/tmp/logs_backup")

# %{path: "/tmp/logs_backup", files: [...], total_bytes: 24_000_000}
```

Creates a consistent SQLite backup (VACUUM INTO) and copies block files.

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

New log entries are first written as uncompressed raw blocks for low-latency ingestion. A background compactor periodically merges raw blocks into compressed blocks:

```elixir
config :timeless_logs,
  compaction_threshold: 500,       # Min raw entries to trigger compaction
  compaction_interval: 30_000,     # Check every 30 seconds
  compaction_max_raw_age: 60,      # Force compact raw blocks older than 60s
  compaction_format: :openzl,      # :openzl (default) or :zstd
  openzl_compression_level: 9      # OpenZL level 1-22 (default 9)
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

## Reducing Overhead

The biggest source of logging overhead in most Elixir apps is stdout/console
output, not the log capture itself. For production or embedded use, disable
the default console handler and let TimelessLogs be the sole destination:

```elixir
# config/prod.exs (or config/config.exs for all environments)
config :logger,
  backends: [],
  handle_otp_reports: true,
  handle_sasl_reports: false

# Remove the default handler on boot
config :logger, :default_handler, false
```

This eliminates the cost of formatting and writing every log line to stdout
while TimelessLogs captures everything at the level you choose:

```elixir
# Only capture :info and above (skip :debug in production)
config :logger, level: :info
```

If you still want console output during development:

```elixir
# config/dev.exs
config :logger, :default_handler, %{level: :debug}
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `"priv/log_stream"` | Root directory for blocks and index |
| `storage` | `:disk` | Storage backend (`:disk` or `:memory`) |
| `flush_interval` | `1_000` | Buffer flush interval in ms |
| `max_buffer_size` | `1_000` | Max entries before auto-flush |
| `query_timeout` | `30_000` | Query timeout in ms |
| `compaction_format` | `:openzl` | Compression format (`:openzl` or `:zstd`) |
| `openzl_compression_level` | `9` | OpenZL compression level (1-22) |
| `zstd_compression_level` | `3` | Zstd compression level (1-22) |
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
5. A background compactor merges raw blocks into OpenZL-compressed blocks (~12.8x ratio)
6. Block metadata and an inverted term index are stored in SQLite (WAL mode, single writer + reader pool) for crash-safe persistence
7. Queries use the SQLite reader pool to find relevant blocks, decompress only those in parallel, and filter entries
8. Real-time subscribers receive matching entries as they're buffered

## Benchmarks

Run on M5 Pro (18 cores). Reproduce with `mix timeless_logs.ingest_benchmark`, `mix timeless_logs.benchmark`, and `mix timeless_logs.search_benchmark`.

**Ingestion** (1.1M simulated Phoenix log entries, 1 week, 1000-entry blocks):

| Path | Throughput |
|------|------------|
| Raw to disk | **1.1M entries/sec** |
| Raw to memory | **4.0M entries/sec** |

**Compression** (1.1M entries, 1000-entry blocks):

| Engine | Level | Size | Ratio | Throughput |
|--------|-------|------|-------|------------|
| zstd | 1 | 23.9 MB | 10.3x | 3.6M entries/sec |
| zstd | 3 (default) | 24.7 MB | 10.0x | 6.3M entries/sec |
| zstd | 9 | 21.7 MB | 11.4x | 941K entries/sec |
| OpenZL | 1 | 22.0 MB | 11.2x | 978K entries/sec |
| OpenZL | 3 | 21.8 MB | 11.3x | 2.0M entries/sec |
| OpenZL | 9 (default) | 19.2 MB | **12.8x** | 793K entries/sec |
| OpenZL | 19 | 17.1 MB | **14.4x** | 21.1K entries/sec |

**Head-to-head** (default levels: zstd=3, OpenZL=9):

| Metric | zstd | OpenZL | Delta |
|--------|------|--------|-------|
| Compressed size | 24.7 MB | **19.2 MB** | 22.2% smaller |
| Compression time | **178 ms** | 1392 ms | 681.7% slower |
| Decompression | 3.1M entries/sec | **3.6M entries/sec** | 12.4% faster |
| Filtered query | 2864 ms | **379 ms** | 86.8% faster |
| Compaction | **3.4M entries/sec** | 2.6M entries/sec | 31.0% slower |

OpenZL columnar wins on filtered queries (86.8% faster) because it can skip irrelevant columns during decompression. Decompression (the read hot path) is 12.4% faster than zstd.

## License

MIT - see [LICENSE](LICENSE) for details.
