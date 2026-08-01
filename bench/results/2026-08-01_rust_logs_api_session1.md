# Rust logs API performance work — Session 1 baseline

Date: 2026-08-01  
Branches: `poc/rust-logs-api-v3` in `timeless_logs` and `timeless-libsql`

This is the pinned baseline for the mixed-workload performance plan. It
supersedes the randomized orientation run in
`2026-08-01_rust_logs_api_poc_baseline.md`.

## Method

Both APIs used fresh data directories and the same deterministic workload:

- seed 42;
- four HTTP writers;
- 500 NDJSON entries per request;
- one second of warmup followed by three-second ramp steps;
- request intervals of 100, 50, 25, 12.5, 6.25, and 3.125 ms;
- zero, one, or two query workers running the same round-robin LogsQL mix;
- an explicit flush and drain-to-zero after producers stopped.

Optimize/compaction was deferred for one hour in both servers. Every final
stats response confirmed that all blocks were raw. This isolates API and raw
ingest/query concurrency from background maintenance.

The Elixir servers listened on dedicated ports 29428–29430. An earlier run on
19428 was discarded after discovering that an existing `timeless_stack`
process and the benchmark process both listened on that port. No number from
the colliding run appears below.

## Side-by-side saturation boundary

The last row for each mode is the highest step reached, not necessarily an
equal offered load. `Admit/s` is HTTP acceptance; `done/s` accounts for queue
depth at the step boundary.

| query workers | API | admit/s | done/s | queued at boundary | write p99 | query p99 | final drain |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0 | Elixir | 497.5K | 496.6K | 2.8K | 5.27ms | — | 42.98ms |
| 0 | Rust | 478.7K | 478.7K | 0 | 1.39ms | — | 4.61ms |
| 1 | Elixir | 489.7K | 489.5K | 2.4K | 5.92ms | 1.20s | 24.25ms |
| 1 | Rust | 178.0K | 162.3K | 47.0K | 2.75ms | 1.07s | 102.36ms |
| 2 | Elixir | 465.8K | 465.5K | 5.1K | 7.31ms | 1.79s | 26.02ms |
| 2 | Rust | 98.0K | 85.5K | 56.0K | 381.93ms | 524.75ms | 135.71ms |

No-query ingest is already in the same range: the Rust path is 3.8% below
Elixir at the final client step. The gap is specifically introduced by
concurrent queries. With one query worker the Rust writer completes about 33%
of the Elixir rate at its saturation boundary; with two it completes about
18%.

## Deterministic ramp detail

### No query workers

| target | Elixir admit/s | Elixir p99 | Rust admit/s | Rust p99 |
|---:|---:|---:|---:|---:|
| 20K | 19.8K | 2.96ms | 19.8K | 1.33ms |
| 40K | 39.2K | 2.68ms | 39.0K | 1.53ms |
| 80K | 76.7K | 2.69ms | 76.5K | 1.70ms |
| 160K | 152.7K | 3.48ms | 151.3K | 1.41ms |
| 320K | 281.2K | 4.42ms | 281.8K | 1.15ms |
| 640K | 497.5K | 5.27ms | 478.7K | 1.39ms |

### One query worker

| target | Elixir admit/s | Elixir write p99 | Elixir query p99 | Rust admit/s | Rust done/s | Rust write p99 | Rust query p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 20K | 19.8K | 3.16ms | 23.89ms | 19.8K | 19.8K | 1.64ms | 60.04ms |
| 40K | 39.2K | 2.83ms | 57.88ms | 39.0K | 39.0K | 1.24ms | 183.21ms |
| 80K | 76.7K | 3.80ms | 118.83ms | 76.0K | 76.0K | 1.33ms | 391.42ms |
| 160K | 152.8K | 3.64ms | 187.59ms | 151.0K | 151.0K | 1.51ms | 758.04ms |
| 320K | 281.2K | 4.57ms | 416.50ms | 178.0K | 162.3K | 2.75ms | 1.07s |
| 640K | 489.7K | 5.92ms | 1.20s | — | — | — | — |

### Two query workers

| target | Elixir admit/s | Elixir write p99 | Elixir query p99 | Rust admit/s | Rust done/s | Rust write p99 | Rust query p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 20K | 19.8K | 4.21ms | 25.38ms | 19.8K | 19.8K | 1.66ms | 58.85ms |
| 40K | 39.2K | 3.75ms | 53.33ms | 39.0K | 39.0K | 1.33ms | 171.29ms |
| 80K | 76.5K | 3.59ms | 112.36ms | 76.2K | 70.0K | 1.38ms | 310.34ms |
| 160K | 152.5K | 4.75ms | 252.86ms | 98.0K | 85.5K | 381.93ms | 524.75ms |
| 320K | 281.8K | 5.51ms | 611.91ms | — | — | — | — |
| 640K | 465.8K | 7.31ms | 1.79s | — | — | — | — |

## Rust phase attribution

The Rust API and extension counters are cumulative over each complete run.

| measurement | no queries | one query worker | two query workers |
|---|---:|---:|---:|
| API query count / time | 0 / 0s | 107 / 9.85s | 208 / 13.86s |
| engine query count / time | 0 / 0s | 107 / 4.93s | 208 / 6.50s |
| candidate blocks | 0 | 5,950 | 8,854 |
| decoded entries | 0 | 10,310,021 | 14,986,768 |
| returned entries | 0 | 8,543,677 | 12,809,574 |
| read permits / aggregate hold | 10 / 12.30ms | 116 / 7.53s | 216 / 10.31s |
| read conflicts | 0 | 66 | 203 |
| writer waits / aggregate wait | 0 / 0s | 70 / 7.06s | 80 / 7.56s |
| writer timeouts | 0 | 0 | 0 |
| flush total | 2.61s | 1.12s | 0.60s |
| flush partition/order | 40.89ms | 18.65ms | 9.83ms |
| flush encode + terms | 1.01s | 433.81ms | 237.51ms |
| flush store | 299.67ms | 101.43ms | 50.37ms |
| optimize calls | 0 | 0 | 0 |

The final no-query run ingested 3,143,500 entries in 6,287 batches. Its API
timers recorded 2.68s parsing NDJSON, 82.91ms encoding batch blobs, and 6.47s
inside SQLite insertion. The extension split that insertion work into 2.66s
wire decoding, 208.54ms metadata normalization, 15.35ms buffer append, and
2.61s flush work. The remaining roughly 970ms includes virtual-table and
autocommit overhead not covered by those inner timers.

Severity partition/order work is 1.56% of flush time and 0.63% of SQLite
insertion time. It is not the current bottleneck, which supports deferring
partition changes.

The mixed runs show the actual boundary. Queries decode and materialize
millions of entries even though the API eventually returns limits of 50 or
100. While that work is active, read permits accumulate 7.53–10.31 seconds of
hold time and writers accumulate 7.06–7.56 seconds of wait time. New readers
can also barge ahead of an already-waiting writer. This directly explains the
queue growth and completion-rate collapse.

## Session 1 verdict

- The workload is deterministic and reports admission, completion, queue
  depth, oldest Rust queue age, and final drain separately.
- Extension telemetry reports flush, query, optimize, candidate/decode/result,
  permit, conflict, and writer-wait work without changing storage behavior.
- The SQLite statement timer contains batch-blob parsing, engine append/flush,
  and autocommit completion; finer residual timing can be added if a later
  result makes it necessary.
- Peak RSS remains a required field for the final production matrix. It was
  not sampled in these short runs, so no memory conclusion is drawn here.
- The next justified change is Session 2 writer fairness and shorter read
  permit lifetime. Query-limit pushdown remains Session 4; this checkpoint
  does not conflate the two effects.

## Session 2 — writer fairness result

Session 2 added one scheduling rule to the shared extension gate: once a
writer is waiting, later readers retry instead of barging ahead. It also
releases the logs virtual-table read permit after engine materialization and
before metadata JSON rendering. Storage, the 8,192-entry buffer, query
semantics, and block formats are unchanged.

| query workers | measurement | Session 1 | Session 2 | change |
|---:|---|---:|---:|---:|
| 1 | completed ingest at 320K/s offered | 162.3K/s | 225.5K/s | +38.9% |
| 1 | write p99 at that step | 2.75ms | 2.20ms | -20.0% |
| 1 | query p99 at that step | 1.07s | 1.41s | +31.8% |
| 2 | completed ingest at 160K/s offered | 85.5K/s | 152.0K/s | +77.8% |
| 2 | write p99 at that step | 381.93ms | 1.39ms | -99.6% |
| 2 | query p99 at that step | 524.75ms | 713.54ms | +36.0% |

The comparisons use equal offered-load steps. In the two-worker case, the
160K step saturated Session 1 but drained completely with low write latency in
Session 2. The next Session 2 step offered 320K/s, completed 145K/s, and
crossed the write-p99 ceiling; it is not used as the clean boundary above.

Both Session 2 runs had zero HTTP errors and zero writer timeouts, then drained
to zero. The one-worker run rejected 316 reader barges and the two-worker run
rejected 2,401. Those counters demonstrate that the gain came from removing
reader starvation. The query-p99 increase at maximum write pressure is the
intentional fairness tradeoff: writers now advance instead of allowing an
unbounded sequence of new reads. Sessions 3 and 4 address the underlying
query critical-section size and over-materialization rather than weakening
fairness.

## Session 3 — protected snapshot and streamed materialization

Session 3 splits a query into two phases. Under the engine transition guard
and extension read permit it resolves posting lists, captures stable block
locations, and clones matching buffered entries. It then releases both guards
before reading/decoding one block at a time, filtering, sorting, and rendering
metadata JSON.

The first prototype copied every candidate payload before releasing the
guards. It reached 424.8K completed entries/s with one query worker, but its
widest query retained 304,952,873 payload bytes (290.83MiB). That design was
rejected. The final extension path relies on the host SQLite SELECT snapshot,
which keeps deleted/replaced row versions readable on the same reader while a
writer publishes. It retains locations and streams payloads individually;
stores without snapshot isolation keep the conservative owned-payload path.

### Final throughput

| query workers | API | completed entries/s | write p99 | query p99 | queued | final drain |
|---:|---|---:|---:|---:|---:|---:|
| 1 | Elixir baseline | 489.5K | 5.92ms | 1.20s | 2.4K | 24.25ms |
| 1 | Rust Session 2 | 225.5K | 2.20ms | 1.41s | 0 | 267.67ms |
| 1 | Rust Session 3 | 479.7K | 1.61ms | 2.40s | 0 | 72.36ms |
| 2 | Elixir baseline | 465.5K | 7.31ms | 1.79s | 5.1K | 26.02ms |
| 2 | Rust Session 2 | 152.0K | 1.39ms | 713.54ms | 0 | 2.36ms |
| 2 | Rust Session 3 | 463.3K | 2.55ms | 2.09s | 0 | 8.64ms |

Session 3 reaches 98.0% of Elixir's one-worker completion rate and 99.5% of
its two-worker rate. Relative to Session 2's highest clean step, completed
ingestion improves 2.13x with one query worker and 3.05x with two.

### Boundary attribution

| measurement | Session 2, 1 worker | Session 3, 1 worker | Session 2, 2 workers | Session 3, 2 workers |
|---|---:|---:|---:|---:|
| read-permit hold | 7.35s | 85.51ms | 8.32s | 191.67ms |
| writer wait | 6.90s | 5.01ms | 7.56s | 40.16ms |
| payload bytes accumulated in snapshots | not measured | 0 | not measured | 0 |
| payload bytes streamed | not measured | 1.99GB | not measured | 4.01GB |
| decoded entries | 15.47M | 13.16M | 18.79M | 26.55M |
| materialized entries | 13.03M | 11.15M | 16.06M | 21.96M |
| HTTP errors / writer timeouts | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |

The protected portion fell by 98.8% with one worker and 97.7% with two;
writer wait fell by 99.9% and 99.5%. This is why ingestion now stays near its
no-query ceiling under concurrent reads.

### Memory verdict

Linux `/proc/<pid>/status` reported these process high-water marks:

- rejected all-payload prototype, one worker and 2.97M stored entries:
  3.86GiB HWM;
- final streamed path, one worker and 3.13M stored entries: 4.51GiB HWM;
- final streamed path, two workers and 3.08M stored entries: 7.50GiB HWM.

The final telemetry proves the streamed path accumulated zero payload bytes,
so Session 3 itself did not add a database-sized payload copy. Overall memory
is still unacceptable because each broad query decodes and materializes
millions of owned `LogEntry` and output rows before SQLite applies `LIMIT 50`
or `LIMIT 100`. Two readers duplicate that work. This is now the explicit
Session 4 acceptance gate: bounded query pushdown must reduce materialized
rows and process HWM, not merely preserve ingest throughput.

## Session 4 — bounded timestamp windows

Session 4 teaches the `timeless_logs` virtual table to consume exact
`ORDER BY ts ASC|DESC LIMIT/OFFSET` plans. The engine retains at most
`LIMIT + OFFSET` rows in a bounded heap, traverses blocks in timestamp-bound
order, and stops when no remaining block can displace the current window.
SQLite continues to recheck predicates and apply LIMIT/OFFSET. The extension
declines the bounded plan for message LIKE, strict timestamp inequalities,
duplicate predicates, and unsupported filters so a SQLite-side rejection can
never make the selected prefix incomplete.

### Pinned mixed-workload result

| query workers | Session | completed entries/s | write p99 | query p99 | queued | final drain | process HWM |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | Session 3 | 479.7K | 1.61ms | 2.40s | 0 | 72.36ms | 4.51GiB |
| 1 | Session 4 | 458.8K | 8.11ms | 1.83s | 0 | 9.32ms | 5.66GiB |
| 2 | Session 3 | 463.3K | 2.55ms | 2.09s | 0 | 8.64ms | 7.50GiB |
| 2 | Session 4 | 467.3K | 3.96ms | 1.95s | 0 | 5.04ms | 6.84GiB |

Both Session 4 runs completed every admitted write, reported zero HTTP errors
and writer timeouts, and held zero queued entries at every step boundary. The
one-reader throughput difference is within the noise created by a small
number of multi-second unbounded queries and allocator pressure; the
two-reader run slightly exceeds Session 3. Query p99 improved 23.8% and 6.7%.

| engine measurement | one reader | two readers |
|---|---:|---:|
| queries | 152 | 298 |
| bounded queries | 92 | 179 |
| bounded capacity requested | 9,200 | 17,900 |
| maximum bounded capacity | 100 | 100 |
| blocks skipped by timestamp bound | 8,087 | 13,415 |
| total candidate blocks | 15,142 | 27,047 |
| total decoded entries | 11,248,046 | 22,636,974 |
| total matched entries visited | 10,969,079 | 22,176,467 |
| total engine rows returned | 9,820,506 | 19,487,994 |
| streamed payload bytes | 1.70GB | 3.43GB |

The apparently contradictory result—bounded calls request only 50 or 100
rows while aggregate returned-row counts remain in the millions—is expected.
Three of every five workload templates are now bounded (`errors_5m`,
`service_1h`, `tail_5m`). The substring template still requires SQLite's exact
LIKE recheck, and the count template still asks SQLite to aggregate a full
virtual-table rowset. Those two Session 5 shapes dominate query work and peak
memory.

### Isolated latest-100 proof

After the two-reader run, a single unfiltered latest-100 request was measured
by stable extension-counter deltas on the 3,109,000-entry raw database:

| measurement | result |
|---|---:|
| HTTP elapsed | 78.99ms |
| engine elapsed | 77.91ms |
| snapshot / materialize | 1.60ms / 76.31ms |
| engine rows returned | 100 |
| candidate blocks | 1,492 |
| blocks skipped by bound | 1,424 |
| blocks decoded | 68 |
| entries decoded | 141,000 |
| payload bytes read | 20.45MiB |

The dense benchmark packs 3.1M entries into only a 21-second timestamp span,
so 68 blocks share an overlapping newest bound and cannot be skipped safely.
Even in that adversarial layout, Session 4 cuts decoded entries by 95.5% and
bounds owned result rows at exactly 100. On ordinary continuously advancing
log time ranges, block-bound pruning should be more selective.

### Session 4 verdict

- Exact ASC/DESC, OFFSET, overlapping-block, duplicate-timestamp, buffered,
  sparse-filter, and fallback regressions are pinned.
- The complete Rust workspace, all CLI/oracle/crash sections, and the ignored
  extension-backed 8,192-entry API contract test pass.
- Limited row queries now satisfy the embedded-memory shape: bounded results
  plus the decoded blocks whose metadata can overlap the requested edge.
- Whole mixed-workload memory is not yet acceptable. Native exact message
  filtering and count are the remaining Session 5 memory gate.

## Session 5 — native exact filtering and scalar count

Session 5 closes the two remaining database-sized rowsets without changing
storage policy. The extension now exposes exact case-insensitive substring
search through the hidden `message_contains` column; unlike compatibility
`message LIKE`, it filters inside the engine and is safe for bounded ordered
execution. The one-row `timeless_log_count` TVF counts exact matches without
materializing rows. Fully covered unfiltered and level-pure blocks use their
persisted entry counts, while boundary, metadata, legacy-mixed, and message
filters decode one block at a time.

The Rust API uses those public extension surfaces. They are not API-only
shortcuts and do not replace the 8,192-entry buffer, raw-first flush, level
partitioning, block codec, compaction, or transaction model.

### Pinned mixed-workload result

| query workers | Session | completed entries/s | write p99 | query p99 | queued | final drain | process HWM |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | Session 4 | 458.8K | 8.11ms | 1.83s | 0 | 9.32ms | 5.66GiB |
| 1 | Session 5 | **477.7K** | **1.48ms** | **237.02ms** | 0 | 12.61ms | **124,504KiB** |
| 2 | Session 4 | 467.3K | 3.96ms | 1.95s | 0 | 5.04ms | 6.84GiB |
| 2 | Session 5 | **471.7K** | **1.59ms** | **242.36ms** | 0 | **4.55ms** | **105,060KiB** |

Both Session 5 runs completed every admitted write with zero HTTP errors,
zero writer timeouts, and zero queued entries at every boundary. Query p99
fell about 87% in both modes. Peak RSS fell roughly 48x with one reader and
67x with two; the two-reader process finished at 98,396KiB RSS.

| engine measurement | one reader | two readers |
|---|---:|---:|
| API calls (row + count) | 255 | 509 |
| bounded row queries | 204 | 407 |
| native scalar counts | 51 | 102 |
| row-query decoded entries | 7,409,593 | 14,393,576 |
| row-query returned entries | 17,850 | 35,600 |
| blocks skipped by timestamp bound | 33,558 | 67,450 |
| native-count metadata blocks | 3,764 | 7,637 |
| native-count metadata entries | 1,444,311 | 2,910,678 |
| native-count decoded blocks / entries | 0 / 0 | 0 / 0 |
| native-count payload bytes | 0 | 0 |
| read-permit aggregate hold | 181.70ms | 340.43ms |
| writer aggregate wait | 33.46ms | 91.82ms |

All four row-returning templates, including substring, are now bounded. The
fifth template is a native scalar count, which explains why row-query count
plus native-count count equals the API call count.

### Independent query-shape probes

These cold probes used the final two-reader raw database: 3,107,000 entries,
1,500 blocks, and a deliberately dense 21-second timestamp span.

| query | HTTP | engine | exact work |
|---|---:|---:|---|
| `_time:1h level:error \| stats count(*)` | 2.80ms | 1.91ms | 147,885 entries from 375 block headers; zero payload reads |
| `_time:15m "timeout" \| limit 50` | 150.42ms | 149.00ms | returned 50; skipped 1,388/1,500 blocks; decoded 228,000 entries / 34,677,722 bytes |
| `_time:1h level:info service:api \| stats count(*)` | 965.69ms | 964.39ms | returned 134,996; decoded 375 blocks / 1,406,253 entries / 208,962,674 bytes |
| latest 100 | 124.39ms | 123.19ms | returned 100; skipped 1,388/1,500 blocks; decoded 228,000 entries |

The service-plus-level count is the honest hard case. A `service:api` posting
proves only that a block contains at least one matching row, not that every
row matches, so exactness requires decoding the selected info blocks. It is
still block-streamed and scalar: CPU/I/O remains linear, but peak memory no
longer grows with the 134,996 matching rows.

### Session 5 verdict

- Direct SQLite/libSQL and the API share the same exact filtering and count
  primitives.
- Core tests prove metadata-only count performs zero payload reads and every
  decoded fallback equals the ordinary row-query oracle, including ASCII and
  Unicode case folding, boundary ranges, metadata filters, and buffered rows.
- CLI section 42 pins SQL planning, exact results, counters, zero matches, and
  the reserved hidden-column contract. The extension-backed HTTP test pins
  the scalar count boundary over the established 8,192-entry flush.
- The complete workspace suite and final 42-section CLI/oracle/crash suite
  pass, including the new native-count and exact-substring section.
- Session 5 meets the whole-workload embedded-memory gate. Session 6 can now
  focus independently on compaction rewrite amplification.

## Session 6 — bounded size-tiered compaction

Session 6 removes the append-to-compressed-tail behavior measured after
Session 5. Raw blocks are compressed independently on their first optimize
turn. Existing compressed cohorts are planned separately and merge only when
their output is at least half of the 8,192-entry target and at least twice the
largest source. Compressed merges may reach 125% of the target; that narrow
overshoot lets two equal ~4,300-entry tiers consolidate instead of remaining
permanent half-full blocks. The one-hour time-span cap, level partitions,
atomic SQLite swap, retention behavior, codec, and recovery format are
unchanged.

The public extension surface now accepts:

```sql
INSERT INTO logs(logs) VALUES ('optimize:65536');
```

The value caps source entries selected for one maintenance turn. One complete
cohort can exceed a smaller budget so work cannot stall. `timeless_stats`
reports raw compression and compressed merging independently (groups, blocks,
entries, input/output bytes, and duration), budget counts, exact actionable
raw/merge backlog, and deferred tails. The Rust API's 30-second timer is only
a wake-up: it reads those public stats, samples candidate bytes, targets at
most 32 MiB of source data, and skips the turn when no cohort is actionable.

### Parent versus Session 6

`tools/bench/session6_log_compaction.py` generated 262,144 deterministic
realistic entries as 128 arrivals of 2,048. Every arrival was explicitly
flushed and optimized, which is intentionally more aggressive than the API's
normal timer and repeatedly exercises small tails. Source entries and bytes
were measured from the exact block IDs removed by each atomic optimize. The
parent extension was built from `bfd2619`; both runs used the same machine,
dataset, script, and release profile.

| measurement | parent `bfd2619` | Session 6 | change |
|---|---:|---:|---:|
| ingested entries | 262,144 | 262,144 | exact parity |
| source entries rewritten | 2,032,948 | **632,837** | **68.9% lower** |
| entry rewrite amplification | 7.755x | **2.414x** | **3.21x lower** |
| raw bytes initially written | 33,196,198 | 33,196,198 | identical |
| source bytes rewritten | 39,176,201 | **34,931,284** | **10.8% less amplification** |
| byte rewrite amplification | 1.180x | **1.052x** | — |
| optimize aggregate | 1,901.30ms | **737.06ms** | **61.2% lower** |
| optimize p50 | 15.13ms | **3.15ms** | **79.2% lower** |
| optimize p95 | 22.19ms | **13.18ms** | **40.6% lower** |
| optimize p99 | 24.05ms | **19.31ms** | **19.7% lower** |
| optimize max | 25.05ms | **21.36ms** | **14.7% lower** |
| final blocks | 37 | 62 | more time-local generations |
| compressed payload | 827,870 bytes | 845,463 bytes | 2.1% larger |
| compressed bytes/entry | 3.158 | 3.225 | 2.1% larger |

### Query regression gate

Each p95 is 50 measured calls after one warm-up over the final optimized
database. Every result was checked for repeatability and total count parity.

| query | parent p95 | Session 6 p95 | result |
|---|---:|---:|---:|
| metadata-only error count | 0.035ms | 0.038ms | +0.003ms |
| decoded info + service count | 44.14ms | 49.05ms | 11.1% slower |
| bounded latest 100 | 4.88ms | **0.73ms** | **85.1% faster** |

The extra block generations impose a small cost on the decoded full-range
shape, while their tighter time bounds materially improve newest-first
pruning. The compressed payload penalty is 2.1%, not the 10.8% seen before
adding the bounded 125% consolidation ceiling. The existing one-shot
1M-entry realistic benchmark remains 8.9 bytes/entry and 13.5x smaller than
the plain table because its raw grouping path is unchanged.

### Regression coverage and verdict

- Core fixtures pin the original 40 × 256 tiny-tail workload: the parent
  rewrote 144,384 source entries (14.1x), while Session 6 rewrites 22,528
  (2.2x), returns all 10,240 entries exactly, and reports eight deferred
  256-entry tails.
- A second fixture pins 125% consolidation of two uneven 4,298-entry tiers,
  and the budget fixture proves oldest-first progress, per-turn limits, exact
  results, and rejection of a zero budget.
- CLI section 43 exercises the public bounded command, phase/backlog stats,
  direct count parity, and the 2.2x amplification fixture through SQLite.
- The extension-backed API test proves its scheduler uses the established
  8,192-entry level-partitioned flush and then the public bounded optimize
  command; no host buffer, block writer, or compactor was introduced.
- The complete workspace tests and 43-section CLI/oracle/crash suite pass.

Session 6's exit criterion is met: entry and byte rewrite amplification and
maintenance pauses are lower, the budgeted public surface bounds API work,
compression remains effectively unchanged, and the query trade is small with
a large win for the latency-sensitive latest-tail shape.

## Session 7 — API scheduling and final boundary verdict

Session 7 uses the same deterministic seed-42 workload, four HTTP writers,
500 entries/request, one-second warm-up, three-second steps, and
100→50→25→12.5→6.25→3.125ms interval ramp as Sessions 1–5. Automatic
compaction was deferred during the raw ingest/query matrix. Every row below is
extension-completed throughput at the final step, not request admission.

### SQLite reader sweep

Each sweep run used two concurrent query workers over all five pinned LogsQL
shapes. Every run had zero HTTP errors, zero writer timeouts, and zero queued
entries at every measurement boundary and after final drain.

| SQLite readers | completed entries/s | write p99 | query p99 | query/s | final drain | process HWM |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 470.3K | **1.61ms** | 383.18ms | 11.0 | 8.49ms | 71,200KiB |
| **2** | 470.2K | 1.66ms | **260.51ms** | 15.3 | 15.88ms | **62,340KiB** |
| 4 | 468.2K | 1.77ms | 250.53ms | 16.3 | **5.12ms** | 97,088KiB |
| 8 | **477.7K** | 2.58ms | 286.96ms | 15.7 | 8.29ms | 127,888KiB |

Two readers are the embedded balance. One leaves useful query parallelism on
the table. Four buys only 10ms of p99 at 34,748KiB more HWM, while eight is
slower at the tail and consumes twice the two-reader HWM. The API now defaults
to two and exposes `TIMELESS_LOGS_READER_CONNECTIONS` for deployments with a
different measured workload.

The sweep rejects two proposed changes on evidence:

- API query admission is unnecessary: the extension's writer fairness kept
  the queue empty and completed throughput flat even with eight readers.
- Grouping already admitted inserts into host transactions has no measured
  writer bottleneck to solve. It would also make the POC exercise transaction
  behavior different from direct SQLite/libSQL users.

### Final Rust versus Elixir raw matrix

Both servers used their established storage path and asynchronous HTTP
admission semantics. Rust used the selected two-reader pool. Counts vary
slightly because each concurrent three-second step ends on a wall-clock
boundary; the generated content and query sequence remain deterministic.

| API | query workers | completed entries/s | write p99 | query p99 | query/s | final queue | final drain | process HWM |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Rust/libSQL | 0 | 465.8K | 7.27ms | — | — | 0 | 13.12ms | 85,716KiB |
| Elixir | 0 | **492.7K** | **5.74ms** | — | — | 0 | 16.34ms | 1,311,760KiB |
| Rust/libSQL | 1 | 471.5K | **1.67ms** | **234.32ms** | **9.0** | 0 | 6.06ms | **73,436KiB** |
| Elixir | 1 | **489.0K** | 6.52ms | 1.25s | 4.0 | 0 | **1.66ms** | 1,479,264KiB |
| Rust/libSQL | 2 | **470.2K** | **1.66ms** | **260.51ms** | **15.3** | 0 | 15.88ms | **62,340KiB** |
| Elixir | 2 | 466.9K | 7.13ms | 1.61s | 8.0 | 0 | **1.91ms** | 1,663,480KiB |

The no-query Rust write p99 is a run-level outlier relative to its own earlier
steps (1.20–1.49ms) and to the mixed runs, but it is reported unchanged. The
completed-throughput verdict does not depend on it: Rust is within 5.5% of
Elixir with no queries, within 3.6% with one, and 0.7% faster with two. Under
query load, Rust's query tail is 5.3–6.2x faster and process HWM is 20–27x
smaller.

Raw final storage for the two-query runs:

| measurement | Rust/libSQL | Elixir |
|---|---:|---:|
| entries | 3,104,500 | 3,103,501 |
| raw blocks | 1,496 | 3,091 |
| logical raw payload | 472,182,063 bytes | 697,797,634 bytes |
| total physical footprint | 475,918,336-byte SQLite file | 860,630,490 bytes (blocks + index) |

SQLite pages and shadow indexes share one file, while Elixir reports its block
files and persistent index separately; the physical row accounts for that
difference. Logical block payload remains the codec measurement. Rust uses
32.3% less raw payload, 51.6% fewer blocks, and 44.7% less physical space in
this raw run.

The audit also found that the Rust compatibility endpoint had called a term
posting-row count `index_size`, whereas Elixir's field is bytes. The endpoint
now obtains real SQLite page bytes from `dbstat` and reports the row count as
`term_postings`; unit and extension-backed tests pin the units. The already
compacted two-reader file could not be re-sampled in its raw state, but the
same-size zero/one-query raw databases allocate 2,129,920 and 2,125,824 index
bytes respectively. Physical footprint above was always measured from the
whole SQLite file and is unaffected by this reporting correction.

### Retained-dataset maintenance drain

The final two-query datasets were restarted without writers and drained
through each implementation's existing compactor. Rust used the Session 6
public 32MiB source-byte budget. Elixir used `Compactor.compact_now/0`, whose
established bounded per-core passes loop until raw debt is zero.

| measurement | Rust/libSQL | Elixir |
|---|---:|---:|
| entries | 3,104,500 | 3,103,502 |
| initial raw blocks / bytes | 1,496 / 472,182,063 | 3,091 / 697,797,634 |
| bounded optimize turns | 9 | internal bounded passes |
| aggregate optimize / drain time | **5.55s** | 13.90s |
| final compressed blocks / bytes | **430 / 27,576,327** | 1,608 / 46,776,363 |
| compressed bytes/entry | **8.88** | 15.07 |
| maintenance process HWM | **32,404KiB** | 863,576KiB |
| final physical footprint | 477,851,648-byte SQLite file | **223,930,971 bytes** (blocks + index) |

Rust's measured extension work is 2.5x shorter, its logical compressed payload
is 41.0% smaller, and its maintenance HWM is 26.7x smaller. Elixir, however,
returns obsolete block files immediately and its post-drain physical footprint
is 2.1x smaller. SQLite reuses freed pages but does not return them to the
filesystem without a vacuum policy. Session 7 does not add one: long-lived
ingest can reuse that space, and embedded callers should choose their own
vacuum/checkpoint policy.

### Final boundary decision

- The POC succeeds as a Rust data-plane boundary. Its HTTP layer feeds the
  public extension unchanged; it does not reproduce buffering, blocks,
  partitioning, querying, or compaction.
- Two SQLite readers are the default. More remain an explicit knob, not a
  CPU-count heuristic.
- No API admission controller or host transaction grouper is justified by the
  final evidence.
- Direct SQLite/libSQL users receive the query and maintenance improvements
  from Sessions 2–6 without running this API.
- The complete workspace suite, strict POC clippy, release builds,
  extension-backed 8,192-entry HTTP contract, 150K-op oracle, and five
  kill/reopen rounds all pass in the final 43-section CLI suite.
- Cluster administration, membership, and Phoenix UI ownership stay outside
  this data-plane POC.

Session 7's exit criterion is met: the boundary decision is based on completed
work, exact final drains, measured query tails, logical and physical storage,
and Linux process high-water memory.
