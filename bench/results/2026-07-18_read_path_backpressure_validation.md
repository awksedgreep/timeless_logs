# Read Path + Backpressure Validation (i185)

**Date:** 2026-07-18 (same day as the audit; branch perf/read-path-and-backpressure)
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers
**Method:** identical workload to the audit's container leg
(`bench/container_http_workload.exs`, 16 writers x 500-entry NDJSON POSTs,
auto-ramp, 10 LogsQL query workers), against a local prod BEAM running the
branch. Baseline numbers are the 0.6.6 container from
`2026-07-18_perf_audit_i185.md` (same host; the container also ran idle
metrics/traces/UI, so treat small deltas as noise — the orders of
magnitude are the story).

## Headline: bounded, honest, and queries never collapse

| Metric | Audit baseline (1.4.18) | This branch |
|--------|------------------------:|------------:|
| Server RSS after run | **32.4 GB** (still churning minutes later) | **0.99 GB** (0.67 GB after 30s) |
| Backlog at bench end | ~35M entries in memory (3.77M of ~38.7M on disk) | **zero** (~6.9M sent == 6.9M on disk) |
| "Peak ingest" | 928K/s *accept* (fiction — enqueue only) | 212K/s *durable* (honest wall, write p99 170ms) |
| Query p50 @ 63K/s ingest | 2.6s | **40ms** |
| Query p50 @ 126K/s | 7.3s | **96ms** |
| Query p50 @ 212–248K/s | 13.3s | **252ms** |
| Queries at saturation | 0 completed in 15s windows | 31 qps, p99 578ms |
| Compaction during run | 45 compacted vs 7,634 raw | 1,048 compacted vs 4,786 raw and draining |

Burst absorption is preserved: below the watermark ingest is still
cast-speed (steps at 8–126K/s show identical ~2ms write p50 to before);
the watermark only pace-matches producers once the durable pipeline is
truly behind.

## What it took (three iterations of the gauge — recorded because the
failed shapes are instructive)

1. **Server-side gauge mirror** (buffer + pending + in-flight, updated by
   the shard): missed casts sitting in the shard *mailbox* — which is
   exactly where overload accumulates. RSS still 9.4GB.
2. **Producer-side add, decrement at flush_done**: missed the *Index*
   GenServer's mailbox/pending, where each cast carries the full entry
   list until the SQLite insert lands. RSS still 7.2GB.
3. **Producer-side add, credit on index durability, producer sleep-polls
   the gauge when over watermark**: waiting on the shard's call reply was
   meaningless (shards hand entries downstream faster than the pipeline
   persists them); waiting on the *gauge* couples acceptance to the
   drain. RSS 0.99GB, zero backlog. This is the shipped design.

Lesson for the ecosystem: a backpressure gauge must span the entry's
whole journey to durability. Any hop it doesn't cover is where the memory
goes.

## Also in this branch (see audit findings 1–7)

- `count_total` off on the LogsQL HTTP path; per-term index counts
  (schema v2): `level=error limit 10` 114ms → 5ms; exact
  `stats count(*)` 114ms → 0.2ms on 500K entries
- Correctness: metadata queries on non-indexed keys no longer silently
  return empty (request_id lookups now work, via scan)
- Correctness: canonical microsecond timestamps (handler stored seconds,
  jsonline microseconds — time filters, ordering, and age retention
  disagreed by ingest path; age retention never matched µs blocks)
- Compactor: bounded continuous passes (no more whole-backlog
  materialization), adaptive level under debt, feeds the raw-debt gauge
- Per-query decompression capped at cores/2; retention skips size/term
  cleanup while ingest is backed up; no atom creation from client input;
  shard count scales with cores (4→8 on i185)

## Known ceiling / next lever

The durable wall (~212K entries/s here) is the write+index drain —
single Index GenServer batching into SQLite. Raising it (larger insert
batches, parallel index partitions) and the queryable hot tail
(serve `_time:5m`-style queries from an in-memory ring instead of
re-decompressing the newest raw blocks) are the next levers if log
volume demands them. Ingest bursts above the wall are absorbed at full
cast speed up to the watermark (default 50K entries/shard).
