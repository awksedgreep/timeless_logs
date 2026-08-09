# libSQL engine port plan (2026-08-09)

Status: **Session 1 in progress.** Mirrors the completed timeless_metrics
arc (notes in that repo: `libsql_storage_engine_migration_plan_2026-07-31.md`,
shipped as its 6.2.3 opt-in → 6.3.0 default). Goal: replace the deprecated
embedded Elixir block engine (SQLite index + `blocks/*.zst|.ozl` + buffer/
compactor/hot-tail) with an in-process engine over the timeless-libsql
v0.5.0 `timeless_logs` virtual table. In-process is the point: the canvas/
dashboard latency budget assumes no HTTP hop, and both embedded and
external (Rust `timeless-logs-api`) modes then share ONE on-disk format —
graduating a host from embedded to Stack becomes stop-one-owner/
start-the-other, no migration.

## What already exists (reuse, don't rebuild)

- **Batch encoder**: `LibsqlCandidate.encode_batch/1` — rich-v1 (`0x02`)
  blobs, exact severity, µs timestamps, canonical typed JSON metadata.
  Validated by the migration cold-parity tests over 8,193 entries.
- **Capability preflight**: `LibsqlCandidate.require_capability/1`
  (data ABI 1 + rich-v1) — becomes the engine's startup check.
- **Vtab DDL**: `CREATE VIRTUAL TABLE logs USING timeless_logs(
  index_keys='...', timestamp_unit='us'[, retention='Ns'])`.
- **Migration machinery**: ReleaseMigration/ReleaseStartup convert legacy
  block dirs → `logs.db`, v0.5.0-validated. Becomes the upgrade path for
  existing embedded installs when the default flips.
- **Unused read surfaces in the extension**: `timeless_log_count`,
  `timeless_log_buckets`, `timeless_log_values`, `timeless_log_query_stats`
  — plus vtab SELECT with ts/level/severity/index-key/message_contains
  pushdown and bounded work guards.

## Design

### Seam (metrics `storage_engine.ex` pattern, app-scoped)

`TimelessLogs.StorageEngine` dispatches on
`:persistent_term {TimelessLogs, :engine}` (set by the supervisor from
`Config`/opts; default stays `:elixir` until the flip release):

| facade fn | :elixir today | :libsql |
|---|---|---|
| `ingest/1`, `log*` | `Buffer.log_many` | writer batch insert (rich-v1 blob) |
| `flush/0` | `Buffer.flush` | `INSERT INTO logs(logs) VALUES ('flush')` |
| `query/1` | `Index.query` | vtab SELECT w/ pushdown via reader pool |
| `count/1` | `Index.count` | `timeless_log_count` TVF |
| `field_values/2` | `Index.field_values` | `timeless_log_values` TVF |
| `field_names/1` | `Index.field_names` | `timeless_log_values`/stats-backed |
| `stream/1` | `Index.stream` | reader cursor over vtab SELECT |
| `stats/0` | `Index.stats` | `timeless_stats('logs')` + query_stats TVF |
| `merge_now/0` | `Compactor.merge_now` | `'optimize'` command (bounded form) |
| `backup/1` | file copy | single-snapshot `VACUUM INTO` (metrics pattern) |
| `subscribe/unsubscribe` | engine-independent (stays in facade) | same |

### Engine module shape (metrics `libsql_engine.ex` as template)

- Writer GenServer: opens `data_dir/logs.db`, loads extension (path
  resolution: opts → app env → `TIMELESS_EXT_PATH` → priv), preflight,
  vtab create with `index_keys` from Config, micro-transaction ingest
  batching, flush/optimize/retention timers (`'flush'`, `'optimize:<N>'`
  bounded, `'prune:<ts>'`), `Process.flag(:trap_exit)` + terminate
  commit+flush (the metrics shutdown-durability lesson).
- Reader pool: prepared statements per query shape, publication-gate retry
  (the metrics `barriered_read` lesson — "blocked by a pending writer
  transaction" errors re-barrier and retry bounded).
- Startup refusal: non-empty legacy block dir + `:libsql` engine → loud
  error naming `ReleaseMigration` (metrics `reject_unmigrated_rust_store!`
  pattern).

### Decisions (following metrics precedent unless noted)

- Opt-in first (`config :timeless_logs, engine: :libsql`), default flip in
  a separate release, legacy engine removal on the existing deprecation
  schedule. Per the git/dependency policy: all work on main, releases
  tagged+published before anything downstream consumes them.
- Subscriptions stay facade-level (tap the ingest path pre-engine) so both
  engines share them.
- `mode: :memory` and any legacy-only features follow the metrics
  playbook: inventoried before the legacy engine is deleted, not blockers
  for the opt-in engine.

## Sessions

1. **Seam + writer + core reads** (in progress): StorageEngine dispatch,
   LibsqlEngine writer (ingest/flush/preflight/terminate), query/count via
   vtab+TVF, suite additions, all green with default `:elixir` untouched.
2. Reader pool + remaining reads (field_values/names, stream, stats,
   backup) + publication-gate retry + engine-contract test running the
   shared suite against both engines.
3. Perf gate (bench vs Elixir engine: ingest, query, storage bytes —
   expect codec-8 message compression to show here) + soak.
4. Flip release (default `:libsql`), migration UX pointer, changelog.
5. (Separate repo) traces port, same shape.

## Gates

- Full suite green after every session; engine-contract parity between
  `:elixir` and `:libsql` on the shared read/write tests.
- Cold-reopen durability (write → stop → reopen → query).
- Bench: no read-latency regression vs the Elixir engine on dashboard-
  shaped queries; storage bytes/entry improvement recorded (codec 8).
