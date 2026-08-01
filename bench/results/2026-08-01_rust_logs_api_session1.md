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
