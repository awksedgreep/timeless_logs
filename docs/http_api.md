# HTTP API

TimelessLogs includes an optional HTTP API compatible with [VictoriaLogs](https://docs.victoriametrics.com/victorialogs/). Enable it in your config:

```elixir
# Port 9428, no auth
config :timeless_logs, http: true

# Custom port with auth
config :timeless_logs, http: [port: 9428, bearer_token: "my-secret-token"]
```

## Endpoints summary

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check (no auth required) |
| `POST` | `/insert/jsonline` | NDJSON log ingest |
| `GET` | `/select/logsql/query` | Query logs with filters |
| `GET` | `/select/logsql/stats` | Storage statistics |
| `GET` | `/api/v1/flush` | Force buffer flush |
| `POST` | `/api/v1/backup` | Online backup |

## Health check

Always accessible without authentication. Suitable for load balancer health checks.

```bash
curl http://localhost:9428/health
```

Response:

```json
{
  "status": "ok",
  "blocks": 48,
  "entries": 125000,
  "disk_size": 24000000
}
```

## Ingest

### POST /insert/jsonline

Ingest logs in NDJSON format (one JSON object per line). Compatible with VictoriaLogs ingest format.

```bash
curl -X POST 'http://localhost:9428/insert/jsonline?_msg_field=_msg&_time_field=_time' -d '
{"_msg": "Request completed", "_time": "2024-01-15T10:30:00Z", "level": "info", "request_id": "abc123"}
{"_msg": "Connection timeout", "level": "error", "service": "api"}
{"_msg": "Cache miss", "_time": "1705312200", "level": "warning", "cache": "redis"}'
```

**Query parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `_msg_field` | `_msg` | JSON field name containing the log message |
| `_time_field` | `_time` | JSON field name containing the timestamp |

**Timestamp formats:**

The `_time` field accepts:
- ISO 8601 strings: `"2024-01-15T10:30:00Z"`
- Unix timestamps (seconds) as integers: `1705312200`
- Unix timestamps (seconds) as strings: `"1705312200"`
- If omitted, the current time is used

**Level field:**

The `level` field (always named `level`) accepts: `"debug"`, `"info"`, `"warning"`, `"warn"`, `"error"`. Defaults to `"info"` if missing or unrecognized.

**Other fields:**

All other JSON fields become metadata key/value pairs.

**Response:**

- `204 No Content` on success (no errors)
- `200` with `{"entries": N, "errors": M}` if some lines failed to parse
- `413` if body exceeds 10 MB
- `400` on read error

## Query

### GET /select/logsql/query

Query logs with filters. Returns NDJSON (one JSON object per line).

```bash
curl 'http://localhost:9428/select/logsql/query?level=error&start=2024-01-15T00:00:00Z&limit=50'
```

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `level` | string | Filter by level: `debug`, `info`, `warning`, `error` |
| `message` | string | Case-insensitive substring match |
| `start` | string | Lower time bound (ISO 8601 or unix seconds) |
| `end` | string | Upper time bound (ISO 8601 or unix seconds) |
| `limit` | integer | Max entries to return (default: 100) |
| `offset` | integer | Skip N entries (default: 0) |
| `order` | string | `asc` (oldest first) or `desc` (newest first, default) |

**Response:**

Content-Type: `application/x-ndjson`

```json
{"_time":"2024-01-15T10:30:00Z","_msg":"Connection timeout","level":"error","service":"api"}
{"_time":"2024-01-15T10:29:55Z","_msg":"Request failed","level":"error","request_id":"abc123"}
```

Each line is a JSON object with:
- `_time`: ISO 8601 timestamp
- `_msg`: log message
- `level`: log level string
- All metadata fields as additional keys

### Examples

```bash
# Recent errors
curl 'http://localhost:9428/select/logsql/query?level=error'

# Errors in the last hour
curl 'http://localhost:9428/select/logsql/query?level=error&start=2024-01-15T09:30:00Z'

# Message search
curl 'http://localhost:9428/select/logsql/query?message=timeout'

# Paginated results, oldest first
curl 'http://localhost:9428/select/logsql/query?level=warning&limit=50&offset=100&order=asc'
```

## Statistics

### GET /select/logsql/stats

Return storage statistics without reading any blocks.

```bash
curl http://localhost:9428/select/logsql/stats
```

Response:

```json
{
  "total_blocks": 48,
  "total_entries": 125000,
  "total_bytes": 24000000,
  "disk_size": 24000000,
  "index_size": 3200000,
  "oldest_timestamp": 1700000000000000,
  "newest_timestamp": 1700086400000000,
  "raw_blocks": 2,
  "raw_bytes": 50000,
  "zstd_blocks": 10,
  "zstd_bytes": 5000000,
  "openzl_blocks": 36,
  "openzl_bytes": 18950000
}
```

## Flush

### GET /api/v1/flush

Force flush the buffer, writing any pending log entries to disk immediately.

```bash
curl http://localhost:9428/api/v1/flush
```

Response:

```json
{"status": "ok"}
```

## Backup

### POST /api/v1/backup

Create a consistent online backup.

```bash
# Backup to a specific directory
curl -X POST http://localhost:9428/api/v1/backup \
  -H 'Content-Type: application/json' \
  -d '{"path": "/tmp/logs_backup"}'

# Backup to default location (data_dir/backups/timestamp)
curl -X POST http://localhost:9428/api/v1/backup
```

Response:

```json
{
  "status": "ok",
  "path": "/tmp/logs_backup",
  "files": ["index.db", "blocks"],
  "total_bytes": 24000000
}
```

## Authentication

When `bearer_token` is configured, all endpoints except `/health` require authentication:

```bash
# Via Authorization header
curl -H "Authorization: Bearer my-secret-token" \
  'http://localhost:9428/select/logsql/query?level=error'

# Via query parameter (for browser access)
curl 'http://localhost:9428/select/logsql/query?level=error&token=my-secret-token'
```

Unauthenticated requests receive:
- `401 Unauthorized` if no token is provided
- `403 Forbidden` if the token is incorrect

## VictoriaLogs compatibility

The HTTP API is designed to be compatible with VictoriaLogs tooling. The ingest endpoint (`/insert/jsonline`) and query endpoint (`/select/logsql/query`) use the same URL paths and query parameter names, so existing pipelines (Vector, Fluent Bit, etc.) can be pointed at TimelessLogs with minimal configuration changes.

### Vector sink example

```toml
[sinks.logs]
type = "http"
inputs = ["my_logs"]
uri = "http://localhost:9428/insert/jsonline"
encoding.codec = "json"
framing.method = "newline_delimited"
```
