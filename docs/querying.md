# Querying

TimelessLogs provides three ways to access stored logs: `query/1` for paginated results, `stream/1` for memory-efficient iteration, and the HTTP query endpoint.

## Query API

```elixir
{:ok, result} = TimelessLogs.query(filters)
```

Returns `{:ok, %TimelessLogs.Result{}}` with:

| Field | Type | Description |
|-------|------|-------------|
| `entries` | list | List of `%TimelessLogs.Entry{}` structs |
| `total` | integer | Total matching entries (before pagination) |
| `limit` | integer | Max entries per page |
| `offset` | integer | Entries skipped |

### Entry struct

```elixir
%TimelessLogs.Entry{
  timestamp: 1700000000000000,   # microseconds since epoch
  level: :error,                 # :debug | :info | :warning | :error
  message: "Connection timeout",
  metadata: %{"service" => "api", "request_id" => "abc123"}
}
```

## Query filters

| Filter | Type | Description |
|--------|------|-------------|
| `:level` | atom | Exact match: `:debug`, `:info`, `:warning`, or `:error` |
| `:message` | string | Case-insensitive substring match on message text and metadata values |
| `:since` | DateTime or integer | Lower time bound (integers are unix timestamps in microseconds) |
| `:until` | DateTime or integer | Upper time bound |
| `:metadata` | map | Exact match on key/value pairs (atom or string keys) |
| `:limit` | integer | Max entries to return (default: 100) |
| `:offset` | integer | Skip N entries (default: 0) |
| `:order` | atom | `:desc` (newest first, default) or `:asc` (oldest first) |

## Query examples

### Filter by level

```elixir
# All errors
{:ok, result} = TimelessLogs.query(level: :error)

# All warnings
{:ok, result} = TimelessLogs.query(level: :warning)
```

### Time range queries

```elixir
# Last hour
{:ok, result} = TimelessLogs.query(
  since: DateTime.add(DateTime.utc_now(), -3600))

# Specific time range
{:ok, result} = TimelessLogs.query(
  since: ~U[2024-01-15 00:00:00Z],
  until: ~U[2024-01-16 00:00:00Z])
```

### Message search

```elixir
# Substring match (case-insensitive)
{:ok, result} = TimelessLogs.query(message: "timeout")

# Also searches metadata values
{:ok, result} = TimelessLogs.query(message: "abc123")
```

### Metadata filtering

```elixir
# Exact metadata match
{:ok, result} = TimelessLogs.query(metadata: %{request_id: "abc123"})

# Multiple metadata keys (all must match)
{:ok, result} = TimelessLogs.query(metadata: %{service: "api", env: "production"})
```

### Combined filters

```elixir
# Errors from the last hour mentioning "timeout" in the api service
{:ok, result} = TimelessLogs.query(
  level: :error,
  message: "timeout",
  metadata: %{service: "api"},
  since: DateTime.add(DateTime.utc_now(), -3600))
```

### Pagination

```elixir
# First page
{:ok, page1} = TimelessLogs.query(level: :error, limit: 50, offset: 0)

# Second page
{:ok, page2} = TimelessLogs.query(level: :error, limit: 50, offset: 50)

# Oldest first
{:ok, result} = TimelessLogs.query(level: :error, limit: 50, order: :asc)
```

## Streaming

For large result sets, `stream/1` returns a lazy `Stream` that decompresses blocks on demand:

```elixir
stream = TimelessLogs.stream(level: :error)
```

The stream yields `%TimelessLogs.Entry{}` structs. Blocks are decompressed one at a time as the stream is consumed, keeping memory usage constant regardless of total result size.

### Stream examples

```elixir
# Take first 10 errors
TimelessLogs.stream(level: :error)
|> Enum.take(10)

# Count errors in the last 24 hours
TimelessLogs.stream(level: :error, since: DateTime.add(DateTime.utc_now(), -86400))
|> Enum.count()

# Custom filtering on stream
TimelessLogs.stream(since: DateTime.add(DateTime.utc_now(), -3600))
|> Stream.filter(fn entry -> String.contains?(entry.message, "timeout") end)
|> Enum.to_list()

# Export to file
TimelessLogs.stream(level: :error)
|> Stream.map(fn entry -> "#{entry.timestamp} [#{entry.level}] #{entry.message}\n" end)
|> Stream.into(File.stream!("/tmp/errors.log"))
|> Stream.run()
```

### Stream vs query

| Feature | `query/1` | `stream/1` |
|---------|-----------|------------|
| Memory | Loads all matching entries | Constant memory (one block at a time) |
| Sorting | Fully sorted by timestamp | Block order (oldest blocks first) |
| Pagination | `:limit`, `:offset`, `:order` supported | Use `Enum.take/2`, `Stream.drop/2` |
| Use case | Dashboards, API responses | Export, aggregation, large scans |

## How queries are optimized

### Inverted index

When you filter by `:level` or `:metadata`, TimelessLogs uses an inverted term index to skip blocks that can't contain matching entries. For example, querying `level: :error` only decompresses blocks known to contain error-level entries.

### Time range pruning

When you use `:since` or `:until`, blocks whose `ts_max < since` or `ts_min > until` are skipped entirely.

### Parallel decompression

Multi-block queries decompress blocks in parallel using `Task.async_stream` with concurrency equal to `System.schedulers_online()`.

## Query performance

Benchmarked with 1.1M indexed entries:

| Query | Median latency |
|-------|---------------|
| Specific metadata key (`request_id: "abc123"`) | 0.6ms |
| Last 1h + level=error | 2.4ms |
| Last 1 hour (all levels) | 4.4ms |
| level=error (all time) | 226ms |
| Message substring search | 420ms |
| Last 24 hours | 244ms |
| Full scan (no filters) | 1.4s |

Queries using the term index (level or metadata filters) are significantly faster than message substring searches, which must scan entry contents.
