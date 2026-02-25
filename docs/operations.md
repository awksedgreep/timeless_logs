# Operations

This guide covers monitoring, backup, retention, and troubleshooting for TimelessLogs.

## Statistics

Get aggregate storage statistics without reading any blocks:

### Elixir API

```elixir
{:ok, stats} = TimelessLogs.stats()
```

Returns a `%TimelessLogs.Stats{}` struct:

| Field | Description |
|-------|-------------|
| `total_blocks` | Number of stored blocks (raw + compressed) |
| `total_entries` | Total log entries across all blocks |
| `total_bytes` | Total block storage size |
| `disk_size` | On-disk storage size |
| `index_size` | SQLite index file size |
| `oldest_timestamp` | Timestamp of oldest entry (microseconds) |
| `newest_timestamp` | Timestamp of newest entry (microseconds) |
| `raw_blocks` | Number of uncompressed raw blocks |
| `raw_bytes` | Size of raw blocks |
| `raw_entries` | Entries in raw blocks |
| `zstd_blocks` | Number of zstd-compressed blocks |
| `zstd_bytes` | Size of zstd blocks |
| `zstd_entries` | Entries in zstd blocks |
| `openzl_blocks` | Number of OpenZL-compressed blocks |
| `openzl_bytes` | Size of OpenZL blocks |
| `openzl_entries` | Entries in OpenZL blocks |
| `compression_raw_bytes_in` | Total raw bytes processed by compactor |
| `compression_compressed_bytes_out` | Total compressed bytes produced |
| `compaction_count` | Number of compaction runs |

### HTTP API

```bash
curl http://localhost:9428/select/logsql/stats
```

```bash
curl http://localhost:9428/health
```

## Flushing

Force flush the buffer to write pending entries to disk immediately:

```elixir
TimelessLogs.flush()
```

```bash
curl http://localhost:9428/api/v1/flush
```

Use before backups or graceful shutdowns.

## Backup

Create a consistent online backup without stopping the application.

### Elixir API

```elixir
{:ok, result} = TimelessLogs.backup("/tmp/logs_backup")
# => {:ok, %{path: "/tmp/logs_backup", files: ["index.db", "blocks"], total_bytes: 24000000}}
```

### HTTP API

```bash
curl -X POST http://localhost:9428/api/v1/backup \
  -H 'Content-Type: application/json' \
  -d '{"path": "/tmp/logs_backup"}'
```

### Backup procedure

1. The buffer is flushed (all pending entries written to disk)
2. SQLite index is snapshot via `VACUUM INTO` (atomic, consistent)
3. Block files are copied in parallel to the target directory
4. Returns the backup path, file list, and total bytes

### Restore procedure

1. Stop the TimelessLogs application
2. Replace the data directory contents with the backup files
3. Start the application -- it will load from the restored data

## Retention

Retention runs automatically to prevent unbounded disk growth. Two independent policies are enforced:

### Age-based retention

Delete blocks with `ts_max` older than the cutoff:

```elixir
config :timeless_logs,
  retention_max_age: 7 * 86_400  # 7 days (default)
```

### Size-based retention

Delete oldest blocks until total size is under the limit:

```elixir
config :timeless_logs,
  retention_max_size: 512 * 1024 * 1024  # 512 MB (default)
```

### Disable retention

```elixir
config :timeless_logs,
  retention_max_age: nil,    # No age limit
  retention_max_size: nil    # No size limit
```

### Manual trigger

```elixir
TimelessLogs.Retention.run_now()
# => :noop or {:ok, count_deleted}
```

### Check interval

```elixir
config :timeless_logs,
  retention_check_interval: 300_000  # 5 minutes (default)
```

## Telemetry events

TimelessLogs emits telemetry events for monitoring and observability:

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:timeless_logs, :flush, :stop]` | `duration`, `entry_count`, `byte_size` | `block_id` |
| `[:timeless_logs, :query, :stop]` | `duration`, `total`, `blocks_read` | `filters` |
| `[:timeless_logs, :retention, :stop]` | `duration`, `blocks_deleted` | |
| `[:timeless_logs, :compaction, :stop]` | `duration`, `raw_blocks`, `entry_count`, `byte_size` | |
| `[:timeless_logs, :block, :error]` | | `file_path`, `reason` |

### Attaching handlers

```elixir
:telemetry.attach_many(
  "my-log-metrics",
  [
    [:timeless_logs, :flush, :stop],
    [:timeless_logs, :query, :stop],
    [:timeless_logs, :compaction, :stop]
  ],
  fn event, measurements, metadata, _config ->
    IO.inspect({event, measurements, metadata})
  end,
  nil
)
```

### Key metrics to monitor

| Metric | Source | Alert threshold |
|--------|--------|-----------------|
| Flush duration | `[:flush, :stop]` duration | Sustained > 100ms |
| Flush entry count | `[:flush, :stop]` entry_count | Sustained at max_buffer_size |
| Query latency | `[:query, :stop]` duration | > 5s for typical queries |
| Blocks read per query | `[:query, :stop]` blocks_read | Growing linearly |
| Compaction entry count | `[:compaction, :stop]` entry_count | Not firing (raw blocks accumulating) |
| Block read errors | `[:block, :error]` | Any occurrence |
| Retention blocks deleted | `[:retention, :stop]` blocks_deleted | 0 when disk is growing |

## Troubleshooting

### High memory usage

- Check `raw_blocks` in stats -- many uncompacted raw blocks use more memory
- Trigger compaction: `TimelessLogs.Compactor.compact_now()`
- Reduce `max_buffer_size` to flush smaller batches
- Check for slow subscribers blocking the buffer

### Disk space growing

- Verify retention is configured: check `retention_max_age` and `retention_max_size`
- Trigger retention manually: `TimelessLogs.Retention.run_now()`
- Check stats for `total_bytes` and `total_entries` trends
- Reduce retention age or size limits

### Slow queries

- Use `:level` and `:metadata` filters to leverage the term index
- Avoid full scans (no filters) on large datasets
- Reduce the time range with `:since` and `:until`
- Check `raw_blocks` count -- many small raw blocks are slower to query than fewer compressed blocks

### Logs not appearing in queries

- Flush the buffer: `TimelessLogs.flush()`
- Check that the Logger handler is installed: `:logger.get_handler_config(:timeless_logs)`
- Verify the data_dir exists and is writable
- Check for block read errors in telemetry events

### Compaction not running

- Check `raw_blocks` and `raw_entries` in stats
- Verify `compaction_threshold` isn't set too high for your log volume
- Trigger manually: `TimelessLogs.Compactor.compact_now()`
- Check that `compaction_max_raw_age` is reasonable (default: 60 seconds)
