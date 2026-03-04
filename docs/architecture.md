# Architecture

This document describes the internal architecture of TimelessLogs.

## Supervision tree

```
TimelessLogs.Supervisor (:one_for_one)
├── Registry (TimelessLogs.Registry, :duplicate)
│     Real-time log subscription registry
├── TimelessLogs.Index (GenServer)
│     ETS tables for block metadata and term index, persisted via snapshots + disk log
├── Task.Supervisor (TimelessLogs.FlushSupervisor)
│     Concurrent flush task execution
├── TimelessLogs.Buffer (GenServer)
│     Entry accumulation, auto-flush, backpressure
├── TimelessLogs.Compactor (GenServer)
│     Background raw → compressed block merging
├── TimelessLogs.Retention (GenServer)
│     Periodic age/size-based cleanup
└── TimelessLogs.HTTP (Bandit, optional)
      VictoriaLogs-compatible HTTP API
```

## Logger integration

TimelessLogs installs itself as an OTP `:logger` handler on application start. All `Logger.info/2`, `Logger.error/2`, etc. calls are automatically captured. The handler:

1. Extracts the log level, message, and metadata
2. Filters out internal metadata keys (`pid`, `mfa`, `file`, `line`, `domain`, `report_cb`, `gl`, `time`)
3. Converts the entry to a map with `timestamp` (microseconds), `level`, `message`, and `metadata`
4. Sends it to the Buffer

## Write path

```
Logger.info("Request completed", request_id: "abc123")
  │
  ▼
TimelessLogs.Handler.log/2
  │  (extract level, message, metadata)
  ▼
TimelessLogs.Buffer.log/1
  │  (broadcast to subscribers, accumulate in buffer)
  ▼
Buffer flush (every flush_interval ms or max_buffer_size entries)
  │
  ▼
TimelessLogs.Writer.write_block/4
  │  (serialize entries to raw block, write to disk or memory)
  ▼
TimelessLogs.Index.index_block_async/3
  │  (index block metadata + terms in ETS immediately, journal to disk log)
  ▼
Data is queryable
```

### Backpressure

The Buffer uses a Task.Supervisor for concurrent flushes. When the number of in-flight flush tasks reaches `System.schedulers_online()`, it falls back to synchronous flushing to prevent unbounded memory growth.

## Read path

```
TimelessLogs.query(level: :error, since: one_hour_ago)
  │
  ▼
Index: build query terms ["level:error"]
  │
  ▼
ETS term_index lookup → MapSet of matching block IDs
  │
  ▼
ETS blocks lookup → filter by timestamp range (ts_min/ts_max)
  │
  ▼
Parallel block decompression (Task.async_stream)
  │  (only decompress blocks that match both term index AND time range)
  ▼
Per-entry filtering (message substring, metadata match)
  │
  ▼
Sort by timestamp → paginate (offset/limit) → return Result
```

Queries run entirely in the caller's process using public ETS tables -- no GenServer round-trip needed for the hot path.

## Storage format

### Block files

Each flush creates a block file in `data_dir/blocks/`:

| Extension | Format | Description |
|-----------|--------|-------------|
| `.raw` | Raw | Erlang `term_to_binary`, uncompressed |
| `.zst` | Zstd | ETF compressed with Zstandard |
| `.ozl` | OpenZL | Columnar split + OpenZL compression |

Filenames are 12-digit zero-padded block IDs: `000000000001.raw`, `000000000002.ozl`, etc.

### Columnar format (OpenZL)

The OpenZL format splits entries into columns for better compression:

1. **Timestamps**: 8-byte little-endian unsigned integers
2. **Levels**: 1-byte unsigned integers (0=debug, 1=info, 2=warning, 3=error)
3. **Messages**: Length-prefixed strings
4. **Metadata**: Batched Erlang term_to_binary

Each column is independently compressed with OpenZL, allowing the compressor to exploit per-column redundancy.

### ETS index

All index state lives in ETS tables — the authoritative source of truth at runtime:

| Table | Type | Purpose |
|-------|------|---------|
| `timeless_logs_blocks` | ordered_set | Block metadata (block_id → file_path, byte_size, entry_count, ts_min, ts_max, format, created_at) |
| `timeless_logs_term_index` | bag | Inverted term index (term → block_id) |
| `timeless_logs_compression_stats` | set | Lifetime compression statistics |
| `timeless_logs_block_data` | set | In-memory block data (memory storage mode only) |

All tables are public with `read_concurrency: true` for lock-free query access.

### Persistence

Index durability uses a snapshot + write-ahead log strategy:

- **`index.snapshot`**: Periodic full dump of all ETS tables (Erlang `term_to_binary`, compressed). Written every 1000 index operations or on graceful shutdown.
- **`index.log`**: Erlang `:disk_log` that journals every index mutation (block inserts, deletes, compactions). Replayed on startup after loading the snapshot.

On startup: load snapshot → replay log entries newer than the snapshot → index is fully reconstructed in ETS. No external database required.

## Inverted index

The term index enables fast filtering without decompressing blocks. Each block's entries contribute terms of the form:

- Level terms: `"level:error"`, `"level:info"`, etc.
- Metadata terms: `"request_id:abc123"`, `"service:api"`, etc.

When querying with `:level` or `:metadata` filters, the index intersects the matching block ID sets, then only decompresses those blocks.

## Compaction pipeline

The Compactor runs periodically and merges raw blocks into compressed blocks:

```
Raw blocks (uncompressed, one per flush)
  │
  ▼
Trigger: entry_count >= compaction_threshold
    OR   oldest_raw_block >= compaction_max_raw_age seconds
  │
  ▼
Read all raw block entries
  │
  ▼
Compress in parallel chunks (concurrency = schedulers_online)
  │  (format determined by compaction_format: :zstd or :openzl)
  ▼
Write new compressed block files
  │
  ▼
Update index (ETS + disk log): remove old blocks, add new
  │
  ▼
Delete old raw block files
```

## Merge compaction

After initial compaction produces many small compressed blocks (e.g. one per flush cycle), the Compactor runs a second pass that merges them into fewer, larger blocks. Larger blocks compress better (bigger dictionary window) and reduce per-block I/O overhead during reads.

```
Small compressed blocks (one per flush cycle)
  │
  ▼
Trigger: compressed blocks with entry_count < target_size >= min_blocks
  │
  ▼
Group into batches where sum(entry_count) ≈ merge_compaction_target_size
  │  (sorted by ts_min for time locality)
  ▼
For each batch: decompress → merge → recompress
  │
  ▼
Update index (ETS + disk log): remove old blocks, add new
  │
  ▼
Delete old compressed block files
```

The merge pass runs automatically after every compaction timer tick and can also be triggered manually via `TimelessLogs.merge_now()`.

| Configuration | Default | Description |
|---------------|---------|-------------|
| `merge_compaction_target_size` | 2000 | Target entries per merged block |
| `merge_compaction_min_blocks` | 4 | Minimum small blocks before merge triggers |

## Retention

The Retention process runs periodically and enforces two independent policies:

| Policy | Configuration | Behavior |
|--------|--------------|----------|
| Age-based | `retention_max_age` | Delete blocks where `ts_max < now - max_age` |
| Size-based | `retention_max_size` | Delete oldest blocks until `total_bytes <= max_size` |

Both policies run independently -- a block is deleted if it violates either policy.

## Further reading

- [Configuration Reference](configuration.md) -- all config options and tuning guidance
- [Querying](querying.md) -- full query API
- [Storage & Compression](storage.md) -- compression formats and ratios
- [Operations](operations.md) -- backup, monitoring, troubleshooting
- Architecture livebook at `livebook/architecture.livemd` for interactive diagrams
