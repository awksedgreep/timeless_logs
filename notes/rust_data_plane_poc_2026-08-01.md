# Rust telemetry data-plane POC

Date: 2026-08-01

Branch: `poc/rust-data-plane`

This branch is the first workload for a standalone Rust telemetry data plane.
Logs were chosen before traces and metrics because their HTTP and query surface
is small enough to port completely, but large enough to prove ingest, storage,
query, discovery, cancellation, and process-failure behavior. If the boundary
works, traces follow next. Metrics can reuse it after that; PromQL stays last.

The matching `timeless-libsql` branch is also `poc/rust-data-plane`. The daemon
and signal-neutral API code live there. This repository owns the compatibility
suite and the eventual Phoenix control-plane client/supervision integration.
The empty `timeless_metrics` POC branch remains parked for later.

## Frozen boundary

- `timelessd` is a normal OS process, never a NIF.
- Rust owns public telemetry ingest, query, discovery, and health endpoints as
  well as the SQLite/libSQL connections and Timeless virtual tables.
- Phoenix owns configuration, users, cluster membership, administration, the
  UI/canvas/stack, and process supervision.
- The control boundary is coarse and asynchronous. A telemetry request does
  not cross Rust/Elixir more than once and never bounces between runtimes.
- The durable seam is the SQLite/libSQL database. Rust loads the same
  distributable `timeless-ext` shared library available to direct
  SQLite/libSQL users.
- Protocol compatibility is measured at the HTTP boundary, not by sharing
  Elixir structs or internal storage files.

## Main-branch baseline

All measurements below were taken from `timeless_logs/main` at `4b70ffe`
(`Release v1.5.0`) before either POC branch was created. The matching
`timeless-libsql/master` baseline was `b18c04a`.

Environment-specific results should be compared on the same machine and with
the same commands. They are acceptance anchors, not general product claims.

### Correctness

Command: `mix test`

- 212 tests passed in 15.7 seconds.

### Embedded ingest

Command: `mix timeless_logs.ingest_benchmark`

| Path | Result |
| --- | ---: |
| Writer only | 118.0K entries/s |
| Buffer pipeline | 580.7K entries/s |
| Batch ingest API | 471.0K entries/s |
| Handler path | 320.1K entries/s |
| Shards: 1 / 2 / 4 / 8 | 427.8K / 728.9K / 870.2K / 798.8K entries/s |

The benchmark's writer-plus-index and Logger/stdout comparisons are sensitive
to cache order and console noise; they are retained in terminal history but are
not acceptance gates for the POC.

### Embedded search

Command: `mix timeless_logs.search_benchmark`

Dataset: approximately 1.1M entries, 1,128 blocks, 239.4 MB block data, 1,008
KB index, 240.4 MB total. Dataset generation and indexing took 2.78 seconds.

| Query | Median |
| --- | ---: |
| `level:error` | 357.4 ms |
| `level:error`, limit 10 | 359.2 ms |
| level plus metadata filter | 366.4 ms |
| message contains `timeout` | 898.9 ms |
| common message scan | 883.4 ms |
| exact request id, 3 matches | 386.3 ms |
| all logs, page 1 | 1.04 s |
| all logs, page 50 | 1.07 s |

The relative-time cases returned zero rows and sub-millisecond timings. Their
fixture timestamps no longer overlap their canonical-microsecond windows, so
they are a benchmark-harness gap, not valid read-path baselines. The POC must
use current timestamps for all relative-window comparisons.

### Mixed HTTP workload

Command:

```console
mix run --no-start ../timeless_logs/bench/container_http_workload.exs \
  --url http://127.0.0.1:19428 \
  --writers 4 --batch 250 --step-seconds 4 \
  --start-interval 500 --query-workers 2 --warmup 1
```

The auto-ramp wrote NDJSON to `/insert/jsonline` while two workers ran a fixed
LogsQL mix. It ended with 897,436 entries in 1,044 blocks and no HTTP errors.

| Write rate | Write p50 | Write p99 | Query rate | Query p50 | Query p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2.0K/s | 1.15 ms | 2.22 ms | 37.3/s | 2.28 ms | 11.04 ms |
| 4.0K/s | 1.24 ms | 1.94 ms | 37.0/s | 2.70 ms | 6.77 ms |
| 7.9K/s | 1.25 ms | 2.65 ms | 36.3/s | 2.95 ms | 15.21 ms |
| 15.9K/s | 1.26 ms | 2.64 ms | 35.5/s | 2.83 ms | 25.16 ms |
| 31.1K/s | 1.25 ms | 2.45 ms | 32.3/s | 3.23 ms | 65.22 ms |
| 62.0K/s | 1.26 ms | 2.42 ms | 31.3/s | 3.11 ms | 92.77 ms |
| 122.8K/s | 1.31 ms | 3.48 ms | 25.8/s | 3.29 ms | 152.58 ms |
| 241.6K/s | 1.43 ms | 4.23 ms | 17.3/s | 3.84 ms | 330.79 ms |

Peak observed ingest was 241.6K entries/s. The important comparison is the
whole latency curve: a Rust result is not useful if it improves peak ingest by
starving reads.

## POC implementation sessions

### Session 1: standalone vertical slice

- [x] Add signal-neutral `timeless-api` and `timelessd` crates to
  `timeless-libsql`.
- [x] Open SQLite, load the distributable `timeless-ext`, and create a
  `timeless_logs` virtual table.
- [x] Implement `/health`, VictoriaLogs-compatible NDJSON ingest, basic LogsQL
  query, and graceful shutdown.
- [x] Unit-test parsing and integration-test immediate reads plus restart
  durability.

### Session 2: complete logs compatibility

- Port the supported LogsQL filters, order, limit, offset, and count query.
- Implement field names, field values, stats, flush, and backup endpoints.
- Run the existing HTTP compatibility corpus against both servers and compare
  normalized status, headers, and response bodies.

### Session 3: production process behavior

- Add bounded request bodies, concurrency/backpressure limits, timeouts, query
  cancellation, structured errors, and graceful drain.
- Add Phoenix-side configuration, client, supervision, readiness, restart, and
  rolling-upgrade behavior without putting Elixir on the telemetry hot path.
- Test daemon crash, SQLite busy/lock behavior, client disconnects, and restart
  recovery.

### Session 4: performance verdict

- Fix the relative-window benchmark fixture before comparing read paths.
- Run the frozen embedded and mixed HTTP workloads against both servers.
- Compare throughput, p50/p95/p99 latency, RSS/peak memory, database size, and
  recovery time with identical data and query mixes.
- Profile before optimizing; promote extension-level improvements whenever
  direct SQLite/libSQL users benefit too.

### Session 5: boundary decision

- Record protocol gaps, operational complexity, and performance deltas.
- Decide whether to proceed to traces, revise the boundary, or stop the POC.
- Only after that decision, reuse the proven daemon/control-plane seam for
  metrics. PromQL remains a separate later decision.

## Initial acceptance criteria

- The original Elixir suite remains green.
- The Rust server passes a differential HTTP corpus for every supported logs
  endpoint, including malformed inputs.
- Acknowledged writes survive a process restart.
- A killed or wedged daemon does not take down the BEAM and is recoverable by
  supervision.
- No unbounded request body, result materialization, or internal work queue.
- Mixed-load measurements include the full latency curve and memory, not only
  peak throughput.
- The POC must make the cross-runtime boundary simpler. If per-request control
  calls or shared internal representations are required, the design has failed
  even if its benchmark is fast.

## Session 1 checkpoint

Implemented on the paired `timeless-libsql/poc/rust-data-plane` branch:

- `timeless-api` parses NDJSON and the supported LogsQL subset, normalizes
  timestamps at the protocol/storage boundary, renders compatible NDJSON, and
  publishes the logs batch-v0 encoder for other Rust hosts.
- `timelessd` loads the actual `libtimeless_ext.so`, owns a bounded writer
  queue plus four bounded read workers, coalesces adjacent durable commits, and
  retries the extension's documented cross-connection busy signal.
- The daemon implements health, NDJSON ingest, GET/POST query, count, flush,
  bearer/query-token authentication, a 10 MB body limit, a 10K result cap,
  graceful drain, and explicit overload responses.
- `timeless_stats('logs')` now exposes metadata-only `entries`; traces expose
  the symmetric `spans`. This is an extension improvement for all direct SQL
  users. A correctness regression covers buffered/persisted/reopen totals.
- Count queries with only time and optional level reuse
  `timeless_log_buckets`, including its block-metadata fast path, rather than
  materializing matching rows in daemon code.

Validation:

- Main extension workspace: all unit, integration, and doc tests pass.
- Daemon workspace: 6 tests pass, including wire responses, auth, malformed
  line isolation, immediate queries, literal message matching, and reopen
  durability.
- Extension R1 statement/savepoint/maintenance correctness script passes.
- After a forced, non-graceful daemon termination, reopening the mixed-load
  database reported exactly 117,000 acknowledged entries in 1,544 blocks; a
  one-hour error count returned 5,664.

### First mixed-load comparison

Same machine, release builds, same workload and client settings as the frozen
baseline. The Rust daemon used four read workers, `service,app,node` index keys,
and the extension's fair `message_index=none` default.

| Write rate | Elixir write p99 | Rust write p99 | Elixir query p99 | Rust query p99 |
| ---: | ---: | ---: | ---: | ---: |
| 2.0K/s | 2.22 ms | 7.72 ms | 11.04 ms | 9.29 ms |
| 4.0K/s | 1.94 ms | 26.26 ms | 6.77 ms | 23.01 ms |
| 7.9K/s | 2.65 ms | 38.71 ms | 15.21 ms | 51.65 ms |
| ~15K/s | 2.64 ms | 132.46 ms | 25.16 ms | 109.91 ms |

- Elixir peak: 241.6K entries/s with 4.23 ms write p99.
- Rust Session 1 peak: 15.1K entries/s with 132.46 ms write p99.
- Rust low-load query p99 already slightly beats the donor, but the current
  mixed write ceiling is about 16x lower.
- Rust ended at 23.7 MB for 117K entries (about 202 bytes/entry). The Elixir
  run ended at 201.7 MB for 897K entries (about 225 bytes/entry). These are
  directional because the ramp produced different totals.
- The durability semantics differ: Rust replies only after a coalesced SQLite
  commit and extension flush; the Elixir HTTP path acknowledges entries still
  resident in its buffer. Keep that distinction visible in every write chart.

The remaining ceiling is understood. Ordinary `SELECT ... FROM logs LIMIT n`
still makes the virtual table materialize every matching entry before SQLite
applies order/limit. The extension holds its read permit for that work, so the
writer gate eventually waits behind increasingly wide service, substring, and
tail scans. Adding daemon threads cannot fix that. Session 2 should add a
bounded native log query/page API to the extension (and direct SQL users), then
route compatibility endpoints through it. This result supports the proposed
Rust/Phoenix process boundary; it identifies the next SQLite-extension seam.
