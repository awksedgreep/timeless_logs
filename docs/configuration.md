# Configuration Reference

All configuration is set via `config :timeless_logs` in your `config/config.exs`.

## Complete options table

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `data_dir` | string | `"priv/log_stream"` | Root directory for block files and SQLite index |
| `storage` | atom | `:disk` | Storage backend: `:disk` (block files) or `:memory` (SQLite BLOBs) |
| `flush_interval` | integer (ms) | `1_000` | Buffer flush interval |
| `max_buffer_size` | integer | `1_000` | Max entries before auto-flush |
| `query_timeout` | integer (ms) | `30_000` | Query operation timeout |
| `zstd_compression_level` | integer (1-22) | `3` | Zstd compression level |
| `openzl_compression_level` | integer (1-22) | `9` | OpenZL columnar compression level |
| `compaction_format` | atom | `:openzl` | Compaction output format: `:zstd` or `:openzl` |
| `compaction_threshold` | integer | `500` | Min raw entries to trigger compaction |
| `compaction_interval` | integer (ms) | `30_000` | Compaction check interval |
| `compaction_max_raw_age` | integer (sec) | `60` | Force compact raw blocks older than this |
| `merge_compaction_target_size` | integer | `2_000` | Target entries per merged compressed block |
| `merge_compaction_min_blocks` | integer | `4` | Min small compressed blocks before merge triggers |
| `retention_max_age` | integer (sec) or nil | `604_800` (7 days) | Delete logs older than this (`nil` = keep forever) |
| `retention_max_size` | integer (bytes) or nil | `536_870_912` (512 MB) | Max total block storage (`nil` = unlimited) |
| `retention_check_interval` | integer (ms) | `300_000` (5 min) | Retention check interval |
| `index_publish_interval` | integer (ms) | `2_000` | ETS-to-SQLite batch flush interval |
| `http` | boolean or keyword | `false` | Enable HTTP API (see below) |

## Full configuration example

```elixir
# config/config.exs
config :timeless_logs,
  data_dir: "/var/lib/my_app/logs",
  flush_interval: 2_000,
  max_buffer_size: 2_000,
  zstd_compression_level: 5,
  compaction_format: :openzl,
  openzl_compression_level: 9,
  compaction_threshold: 1_000,
  compaction_interval: 60_000,
  compaction_max_raw_age: 120,
  retention_max_age: 30 * 86_400,
  retention_max_size: 2 * 1024 * 1024 * 1024,
  retention_check_interval: 600_000,
  http: [port: 9428, bearer_token: "my-secret-token"]
```

## HTTP server options

The `http` option accepts:

| Value | Behavior |
|-------|----------|
| `false` | HTTP server disabled (default) |
| `true` | HTTP server on port 9428, no authentication |
| keyword list | HTTP server with custom options |

HTTP keyword options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | integer | `9428` | HTTP listen port |
| `bearer_token` | string or nil | `nil` | Bearer token for API authentication. When `nil`, all endpoints are open |

```elixir
# No auth (trusted network)
config :timeless_logs, http: true

# With auth
config :timeless_logs, http: [port: 9428, bearer_token: "my-secret-token"]

# Custom port only
config :timeless_logs, http: [port: 9500]
```

## Storage backends

### Disk storage (default)

```elixir
config :timeless_logs,
  storage: :disk,
  data_dir: "/var/lib/my_app/logs"
```

Block files are stored in `data_dir/blocks/` as numbered files with format-specific extensions (`.raw`, `.zst`, `.ozl`). The SQLite index is stored at `data_dir/index.db`.

### Memory storage

```elixir
config :timeless_logs,
  storage: :memory
```

Block data is stored as BLOBs in an in-memory SQLite database. Useful for testing or ephemeral environments. Data does not survive restarts.

## Tuning guidance

### Buffer settings

`flush_interval` and `max_buffer_size` control when buffered log entries are written to disk.

- **Lower flush_interval** (e.g., 500ms): lower latency for queries to see recent logs, more I/O
- **Higher flush_interval** (e.g., 5000ms): better batching, higher throughput
- **Higher max_buffer_size** (e.g., 5000): larger blocks, better compression ratios
- **Lower max_buffer_size** (e.g., 200): more frequent flushes, lower data loss risk

### Compression settings

The compaction format determines how raw blocks are compressed:

| Format | Ratio | Throughput | Best for |
|--------|-------|-----------|----------|
| `:zstd` | ~11x | ~500K entries/sec | General use |
| `:openzl` (level 1) | ~11x | ~1.7M entries/sec | High throughput |
| `:openzl` (level 9) | ~12.5x | ~763K entries/sec | Balance (default) |
| `:openzl` (level 19) | ~14x | ~23K entries/sec | Maximum compression |

### Retention settings

Both age-based and size-based retention are enforced independently. Set either to `nil` to disable that policy:

```elixir
# Keep 30 days, no size limit
config :timeless_logs,
  retention_max_age: 30 * 86_400,
  retention_max_size: nil

# Keep 1 GB, no age limit
config :timeless_logs,
  retention_max_age: nil,
  retention_max_size: 1_073_741_824

# Disable all retention (keep forever)
config :timeless_logs,
  retention_max_age: nil,
  retention_max_size: nil
```

### Compaction settings

Compaction merges raw (uncompressed) blocks into compressed blocks. Tune these based on your log volume:

- **High volume** (>1000 entries/sec): increase `compaction_threshold` to 2000+ and `compaction_interval` to 60_000 for larger, more efficient batches
- **Low volume** (<10 entries/sec): decrease `compaction_max_raw_age` to 30 for faster compaction
- **Testing**: set `compaction_threshold` to 10 and `compaction_max_raw_age` to 5 for immediate compaction

### Merge compaction settings

After initial compaction, many small compressed blocks accumulate (one per flush cycle). Merge compaction consolidates them into larger blocks for better compression and fewer blocks to scan during queries.

- **High volume**: the defaults (target 2000 entries, min 4 blocks) work well
- **Low volume** (<10 entries/sec): lower `merge_compaction_min_blocks` to 2 so merges happen with fewer blocks
- **Large queries**: increase `merge_compaction_target_size` to 5000+ to produce fewer, larger blocks

Merge compaction can also be triggered manually via `TimelessLogs.merge_now()`.
