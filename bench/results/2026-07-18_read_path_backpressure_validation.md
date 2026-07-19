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

## Hot tail (added after the numbers above)

Recent entries are additionally served from an in-memory ETS tail
(insert at accept — entries are queryable the moment ingest returns;
window 30s / 250K entries; queries partition at a boundary timestamp so
tail∪disk is exact with no dedup; retention purges the matching range).

Same workload, tail enabled:

| Ingest | Query p50 (no tail) | Query p50 (tail) | Query p99 (tail) |
|-------:|--------------------:|-----------------:|-----------------:|
| 8K/s | 8.0ms | **3.7ms** | 40ms |
| 63K/s | 34ms | **4.9ms** | 263ms |
| 126K/s | 96ms | **5.9ms** | 482ms |
| 227K/s (wall) | 252ms | **11.4ms** | 1.97s |

RSS stays bounded (1.24GB at bench end); the durable ingest wall is
unchanged. The p99s are the `stats count(*)` queries: exact counts over
the hot window are a linear chunked pass over up to 250K tail entries —
the one remaining linear cost (candidate fix: per-term counters in the
tail, if it ever matters).

It took two iterations to make the tail *help* under load — the first
full-selected the tail per query (12s p50, 9GB RSS: worse than no tail).
Same lesson as the disk side, in memory: reads must be bounded
(prev/next walks for pages, select-continuation chunks for counts,
select_delete for pruning).

## The 227K wall, decomposed and moved to 388K

Per-stage microbench of the drain (50K realistic entries, budget at
227K/s = 4.41µs/entry of serialized time):

| Stage | µs/entry before | after |
|-------|----------------:|------:|
| extract_terms (regex heuristics, runs 2x/entry) | **14.52** | **1.93** |
| Registry.count_match per entry | 2.63 | ~0 (per batch) |
| :json.decode per NDJSON line | 2.01 | 2.01 (parallel acceptors) |
| openzl L3 compress | 1.22 | 1.22 |
| hot tail insert | 0.74 | 0.74 |
| raw block write | 0.48 | 0.48 |
| SQLite index insert (batched) | 0.25 | 0.25 |

The wall was NOT the single Index GenServer or SQLite (0.25µs/entry —
17x headroom); it was CPU in term extraction: regex-powered
cardinality heuristics per metadata value per entry, twice (flush +
compaction) ≈ 6.6 cores of regex at 227K/s.

Fixes: verdicts memoized per distinct {key, value} pair per block (log
batches are repetitive — heuristics now run once per distinct pair, and
non-whitelisted keys skip the memo since their set-lookup rejection is
already cheap), byte-walk implementations with exact regex-equivalent
semantics, subscriber check per batch, no per-entry serialization in
compactor accounting.

**Result: durable wall 227K → 388K entries/s (+71%).** The former wall
(247K step) now cruises at write p99 12.3ms. Query p50 3.4–17ms across
the whole ramp; zero backlog at bench end (13.2M sent == 13.2M on
disk); RSS 4.1GB at the wall (SQLite mmap of a 2.5GB dataset accounts
for ~half).

## Known ceiling / next lever

Remaining per-entry costs are :json.decode (~2µs, parallel across HTTP
acceptors) and the general movement of entry maps through the pipeline.
Next levers if volume demands: parallel index partitions, NDJSON
decode in flush tasks, or binary-oriented entry representation. Ingest
bursts above the wall are absorbed at full cast speed up to the
watermark (default 50K entries/shard).

## Downstream compatibility (breaking-change sweep)

timeless_ui and timeless_canvas: no timeless_logs usage that the shape
changes touch (canvas already normalizes timestamp units by magnitude).
timeless_logs_dashboard: metadata access was already dual-shape-safe and
its LogsQL filters already string-keyed; the one break was
`DateTime.from_unix/1` (seconds assumption) in `format_timestamp` —
fixed with magnitude detection (dashboard commit 541e5dd), compatible
with both 1.4.x mixed-unit data and 1.5+ canonical µs.
