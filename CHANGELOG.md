# Changelog

This changelog starts at 1.5.5; earlier releases are recorded by git
tags and `bench/results/*.md` session documents.## 1.8.0 (2026-08-10)

**Message search is message-only and pushes into the storage engine.**
`{:message, term}` used to also match any metadata value. The engine matches
the message, so that predicate could not be pushed down: the store returned
rows and the shared filter applied the term in Elixir, decoding the whole store
on every search. Metadata is searched with `:metadata` / `:metadata_any`, which
push down through the indexed key columns.

Minor rather than patch: a search that relied on matching metadata values needs
the metadata filters instead.

Over all history on 200k entries — message 769ms to 8ms, `service` 22,218ms to
133ms, `path`/`status` 5ms.

**`count_total: false` bounds the fetch.** It was accepted and ignored. An exact
total costs a full materialisation: every match crosses into Elixir to be
counted, linear in matches rather than store size. The bound is applied only
when the engine can evaluate every predicate itself, since an unindexed
metadata key is re-checked in Elixir and a SQL `LIMIT` would cut the page short.

**Indexed keys now cover the alias spellings** `normalize_filters/1` expands
`service` and `host` into. Postings are written at insert time, so widening the
list is not retroactive: startup reindexes an existing store and reconnects.
Against an extension older than the `reindex` command it logs a warning and
continues on the narrower list rather than refusing to start. Requires the
timeless-libsql v0.6.0 extension to migrate.

**Freed pages are returned to the filesystem.** Retention deletes blocks
continuously inside the extension, but SQLite's default `auto_vacuum` leaves
those pages on the freelist forever — one production store held 813 KB of
blocks in a 1.86 GB file. Startup converts the store to incremental auto-vacuum
and reclaims the backlog; a worker returns pages every minute after that,
tunable with `:vacuum_interval` and `:vacuum_pages_per_turn`. Both paths
checkpoint, because in WAL mode a VACUUM leaves the file at its old size until
the log is checked back in.

## 1.7.1 (2026-08-09)

**The Logger handler no longer drops every entry on `engine: :libsql`.** It
called `Buffer.log/1` directly, which casts to shard processes
`libsql_children/0` never starts. `GenServer.cast` to an unregistered name
returns `:ok`, so log lines were discarded silently and no subscriber was
notified — live tail could not work, because nothing was ingesting. A
production store took no writes for three and a half hours after switching
engines.

The handler now goes through `StorageEngine.ingest_one/1`. The Elixir branch
stays on `Buffer.log/1` rather than `log_many/1`, which can block the caller
on backpressure — a handler runs in the calling process.

`ingest_one/1` is total on purpose. `:logger` removes a handler that raises or
exits, permanently, and two cases hit that immediately: the engine logs from
inside its own process, so persisting those events was a `GenServer.call` to
self, and the handler outlives the engine, so a shutdown notice reaching a
dead engine exited with `:noproc`. Events emitted by the engine itself still
reach subscribers.

## 1.7.0 (2026-08-09)

**Automatic legacy conversion.** Starting on `engine: :libsql` over an
unmigrated legacy block store now runs the journaled, resumable,
digest-verified `ReleaseStartup.prepare/2` conversion automatically at
startup (exclusive owner lock; source retained for rollback), instead
of refusing. Set `auto_migrate: false` to restore the strict refusal.
The legacy Elixir block engine is deprecated for removal in roughly
three months (~2026-11).

## 1.6.0 (2026-08-09)

**Opt-in libSQL storage engine** (`config :timeless_logs, engine: :libsql`)
— the port of the runtime to the timeless-libsql v0.5.0 logs virtual
table, replacing the deprecated Elixir block engine for hosts that opt
in. One `logs.db` holds everything; rich log messages get CLP codec-8
template compression at optimize; embedded and external (Rust
`timeless-logs-api`) modes share one on-disk format, so a host graduates
to the Rust owner by switching owners, not migrating data.

- Full facade coverage: ingest (rich-v1 batches), flush/optimize,
  query/count (ts+level pushdown + the shared Filter residuals — parity
  with the Elixir engine by construction), stream and the field
  aggregations, stats, VACUUM INTO backup, subscriptions.
- Startup refuses an unmigrated legacy block store loudly (run
  `TimelessLogs.ReleaseMigration` first); cold-reopen durability via a
  final flush on shutdown.
- Default engine remains `:elixir`, completely unchanged; the flip ships
  as its own release. No Elixir HTTP child under `:libsql` — HTTP serving
  belongs to the Rust services.
- Port plan: `notes/libsql_engine_port_plan_2026-08-09.md`.

## 1.5.5 (2026-08-08)

Validated against the released **timeless-libsql v0.5.0** and re-pinned
the CI/release workflows to that tag (they previously built a pre-0.4.0
development rev, 409 commits behind). The capability preflight —
data ABI 1 + rich-v1 logs batches — passes unchanged, and the full
suite (230 tests, including crash-boundary migration resume and
cold-parity validation over 8,193 entries) is green against v0.5.0.

- **Message compression:** the migration's final `optimize` under a
  v0.5.0 extension now template-compresses rich log message columns
  (CLP-style codec 8; timeless-libsql measured 1.29–2.44× smaller
  blocks on message-dominated corpora, with a per-block fallback that
  never regresses). Consequence: once a migrated `logs.db` has been
  optimized by v0.5.0, its readers need timeless-libsql ≥ 0.5.0 —
  pre-0.5.0 extensions refuse the new blocks loudly.
- The libSQL migration candidate now traps exits so its connection and
  WAL close even when the linked migration caller dies mid-run.
