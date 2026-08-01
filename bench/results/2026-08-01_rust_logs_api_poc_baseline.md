# Rust logs API POC baseline — 2026-08-01

This records the first API-only comparison. Both servers were started from
fresh data directories and exercised through the existing
`bench/container_http_workload.exs` client. The workload used four writers,
500 NDJSON entries per request, two concurrent query workers, three-second
ramp steps, and one second of warmup.

The workload shape is the same but its generated lines are randomized, so
these are orientation numbers rather than the final pinned-dataset result.

## Untouched Elixir API (`main`)

| admitted entries/s | write p99 | query p99 | query errors |
|---:|---:|---:|---:|
| 19.8K | 3.07ms | 19.20ms | 0 |
| 39.0K | 3.98ms | 36.33ms | 0 |
| 76.3K | 3.09ms | 83.20ms | 0 |
| 151.2K | 4.79ms | 178.91ms | 0 |
| 277.0K | 4.92ms | 302.80ms | 0 |
| 456.5K | 8.16ms | 915.69ms | 0 |

Peak: 456.5K admitted entries/s; saturation stopped at the client's minimum
interval rather than the 100ms write-p99 ceiling.

## Rust API POC over unchanged `timeless_logs` vtab

| admitted entries/s | write p99 | query p99 | query errors |
|---:|---:|---:|---:|
| 19.8K | 1.54ms | 52.26ms | 0 |
| 39.0K | 1.53ms | 173.81ms | 0 |
| 76.3K | 1.60ms | 361.82ms | 0 |
| 124.0K | 112.29ms | 740.11ms | 0 |

The Rust run saturated at 124.0K admitted entries/s when the bounded writer
queue filled behind concurrent reads. An earlier insertion-ack run exposed
retryable publication conflicts as HTTP errors; the API now retries those
conflicts, and the table above has zero errors.

## Interpretation

- The HTTP/JSON admission path is not the current limit: write p99 remains
  around 1.6ms through 76.3K entries/s.
- SQLite writer drain loses ground when concurrent vtab queries hold the
  extension publication boundary. Queueing then converts that drain limit
  into admission backpressure at the next ramp step.
- This is an API scheduling/query-concurrency problem. The POC leaves the
  extension's 8,192-entry buffer, automatic raw flush, batch format, block
  partitioning, and optimize behavior unchanged.
- The API exposes queued batch/entry counts so later measurements can report
  admission separately from completed SQLite ingestion.

## Correctness checks

- `bench/api_parity.py` sends the same deterministic 100-entry NDJSON batch
  through both servers, flushes, waits for each query path to publish the
  result, and compares every returned field after timestamp normalization.
  Result: `api_parity|100|exact`.
- The Rust API end-to-end test proves that 100 entries remain in the vtab
  buffer with zero raw blocks, then the next 8,092 entries trigger the
  extension's existing 8,192-entry automatic flush. The API performs no
  intermediate flush and defines no competing entry threshold.

## Commands

```bash
mix run --no-start ../timeless_logs/bench/container_http_workload.exs \
  --url http://127.0.0.1:19428 --writers 4 --batch 500 \
  --step-seconds 3 --start-interval 100.0 --query-workers 2 --warmup 1

mix run --no-start ../timeless_logs/bench/container_http_workload.exs \
  --url http://127.0.0.1:19429 --writers 4 --batch 500 \
  --step-seconds 3 --start-interval 100.0 --query-workers 2 --warmup 1
```
