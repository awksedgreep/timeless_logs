# Storage & Compression

This document covers TimelessLogs' block-based storage format, compression options, and the compaction pipeline.

## Block-based storage

Log entries are stored in **blocks** -- batches of entries written together and indexed as a unit. Each block contains:

- A batch of serialized log entries (typically 500-2000)
- Block metadata: block_id, byte_size, entry_count, timestamp range (ts_min, ts_max), format
- An inverted index of terms for fast querying

## Block formats

### Raw (`.raw`)

Uncompressed Erlang binary serialization (`term_to_binary`). This is the initial format when entries are flushed from the buffer. Raw blocks are temporary -- the compactor merges them into compressed blocks.

### Zstd (`.zst`)

Erlang term_to_binary compressed with [Zstandard](https://facebook.github.io/zstd/). Good general-purpose compression with fast decompression.

| Metric | Value |
|--------|-------|
| Compression ratio | ~11.2x |
| Compression throughput | ~500K entries/sec |
| Configurable level | 1-22 (default: 3) |

### OpenZL (`.ozl`)

Columnar format with [OpenZL](https://github.com/nicholasgasior/ex_openzl) compression. Entries are split into separate columns (timestamps, levels, messages, metadata) and each column is independently compressed. This exploits per-column redundancy for better ratios.

| Level | Ratio | Throughput |
|-------|-------|-----------|
| 1 | 10.9x | 1.7M entries/sec |
| 5 | 11.4x | 1.2M entries/sec |
| 9 (default) | 12.5x | 763K entries/sec |
| 19 | 14.0x | 22.6K entries/sec |

### Choosing a format

Set the compaction output format:

```elixir
config :timeless_logs,
  compaction_format: :openzl,       # or :zstd
  openzl_compression_level: 9,      # 1-22
  zstd_compression_level: 3         # 1-22
```

| Use case | Recommended format | Level |
|----------|-------------------|-------|
| General use | `:openzl` | 9 |
| Maximum throughput | `:openzl` | 1 |
| Maximum compression | `:openzl` | 19 |
| Legacy/simple | `:zstd` | 3-5 |

## Compaction

New log entries are first written as raw (uncompressed) blocks for low-latency ingestion. A background Compactor process periodically merges raw blocks into compressed blocks.

### Compaction triggers

Compaction runs when any of these conditions are met:

1. **Entry threshold**: total raw entries >= `compaction_threshold` (default: 500)
2. **Age threshold**: oldest raw block >= `compaction_max_raw_age` seconds (default: 60)
3. **Manual trigger**: `TimelessLogs.Compactor.compact_now()`
4. **Periodic check**: every `compaction_interval` ms (default: 30,000)

### Compaction process

1. Read all raw block entries from disk
2. Merge entries into larger batches
3. Compress in parallel chunks (concurrency = `System.schedulers_online()`)
4. Write new compressed block files
5. Update the index (delete old block metadata, add new)
6. Delete old raw block files
7. Update compression statistics

### Compaction configuration

```elixir
config :timeless_logs,
  compaction_threshold: 500,           # Min raw entries to trigger
  compaction_interval: 30_000,         # Check interval (ms)
  compaction_max_raw_age: 60,          # Force compact after this many seconds
  compaction_format: :openzl,          # Output format
  openzl_compression_level: 9          # Compression level
```

### Manual compaction

```elixir
TimelessLogs.Compactor.compact_now()
# => :ok or :noop (if nothing to compact)
```

## Disk layout

```
data_dir/
├── index.db          # SQLite index (WAL mode)
├── index.db-wal      # SQLite WAL file
├── index.db-shm      # SQLite shared memory
└── blocks/
    ├── 000000000001.raw   # Raw block (temporary)
    ├── 000000000002.raw   # Raw block (temporary)
    ├── 000000000003.ozl   # OpenZL compressed block
    ├── 000000000004.ozl   # OpenZL compressed block
    └── ...
```

Block filenames are 12-digit zero-padded block IDs with format-specific extensions.

## Memory storage mode

For testing or ephemeral environments, use in-memory storage:

```elixir
config :timeless_logs, storage: :memory
```

In memory mode:
- Block data is stored as BLOBs in an in-memory SQLite database
- No block files are written to disk
- The ETS index still provides lock-free read access
- Data does not survive application restarts

## Compression statistics

Track compression efficiency via the stats API:

```elixir
{:ok, stats} = TimelessLogs.stats()

stats.compression_raw_bytes_in       # Total uncompressed bytes processed
stats.compression_compressed_bytes_out  # Total compressed bytes produced
stats.compaction_count               # Number of compaction runs
```

The compression ratio is `compression_raw_bytes_in / compression_compressed_bytes_out`.
