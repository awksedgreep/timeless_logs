# Rust logs data-plane POC: clean restart

This POC starts from `timeless_logs` `main` and `timeless-libsql` `master` on
fresh paired branches named `poc/rust-logs-data-plane-v2`. The earlier
`poc/rust-data-plane` branches are retained only as an audit trail. Their
performance results are invalid and must not be used: that implementation
forced a flush for each coalesced request group and measured a predominantly
raw-block store instead of the designed compressed steady state.

## Boundary

The intended production boundary remains:

- Rust owns public telemetry data-plane APIs, protocol parsing, admission,
  storage scheduling, queries, and result streaming.
- SQLite/libSQL plus the Timeless extension owns durable blocks, indexes,
  compression, retention primitives, and query kernels.
- Elixir/Phoenix owns cluster administration, topology, configuration tables,
  UI/canvas/stack, and control-plane calls into the Rust service.

The POC begins below that boundary with storage scheduling only. No HTTP or
auth code is allowed until the storage lifecycle gate passes.

## Fidelity contract

The replacement must preserve these existing `timeless_logs` semantics:

- Admission is bounded by both batch slots and entry credits. Credits are not
  released merely because the storage worker dequeued a batch; they remain in
  use until raw durability. An ingest acknowledgement means accepted into the
  queue, not individually flushed or compressed.
- Log batches remain batches and use the extension's columnar batch ingest
  path.
- Entries accumulate across requests. The default raw flush boundary is
  1,000 entries or 1 second for low-volume traffic.
- A raw flush performs cheap framing and persistence only. Compression is
  separate background work.
- Compaction is triggered by raw entry debt or raw age, runs bounded entry
  budgets, and repeats without loading an arbitrary backlog into memory.
- Queries remain exact across buffered, raw, mixed, compressed, and reopened
  states.
- Graceful shutdown drains the accepted tail to raw storage. A crash may lose
  acknowledged-but-not-durable buffer entries; the future API must state this
  honestly or offer a separate durable acknowledgement mode.

## Gates

### Gate 0 — storage lifecycle

Implemented first in `timeless-libsql/tools/logs-poc`:

- bounded producer queue;
- batch-blob ingest with no per-request flush;
- aggregate threshold and timer raw flushes;
- public raw/compressed debt statistics;
- bounded `optimize:<max_source_entries>` maintenance;
- exact-query checks at every lifecycle state;
- cold-reopen and graceful-tail checks.

This gate is correctness evidence only. It intentionally publishes no API
latency or throughput result.

### Gate 1 — fair baseline

Before adding a Rust server, capture the current Elixir service on one pinned,
deterministic workload. Record accepted and durable write latency separately,
raw-flush throughput, compression throughput, steady-state file size, read
latency, and peak memory. Use the same batch sizes, index keys, durability
boundary, retention settings, and compressed steady state for both systems.

### Gate 2 — log-specific API shell

Only after Gates 0 and 1, add a crate named `timeless-logs-api`. Initially it
contains only the minimum VictoriaLogs-compatible ingest and query surfaces
needed by the comparison. No auth, cluster administration, generic telemetry
abstractions, or production deployment polish belongs in this gate.

### Gate 3 — honest comparison

Compare identical inputs and query answers after background maintenance has
reached the same steady state. Report queue-admission and durable-flush writes
as different rows. A result is invalid if one side compresses synchronously,
forces request-boundary flushes, remains raw, or uses different batching.

### Gate 4 — control-plane integration

If the POC passes, connect Phoenix administration to the Rust data plane via
explicit control APIs. Metrics and traces remain later candidates; no shared
generic API crate is justified until at least two real implementations expose
the same stable seam.

## Explicit non-goals for the POC

- authentication and authorization;
- TLS, rate limiting, or deployment packaging;
- a generic `timeless-api` crate;
- metrics or traces endpoints;
- claiming performance before Gate 3;
- replacing the current default engine.
