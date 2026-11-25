# Project Backlog

**Single source of truth for open work in this repository.** If a task is not
listed here, it is not planned; if it is listed here, it is not done.

- Shipped changes are recorded in [CHANGELOG](CHANGELOG.md), not here.
- Decisions with lasting consequences become ADRs under `docs/adr/`.
- Reference material (configuration, deployment, runbooks) lives beside this
  file and is not a task list.
- Finished items are deleted from this file when the change ships. This file
  describes the *present* state, not the history.

## How To Use This File

1. Every item has a stable ID (`REL-1`, `ARCH-3`, `PRD-2`), a one-line goal,
   and a **Done when** clause that is checkable without asking the author.
2. Ordered by priority: P0 blocks the next release, P1 is this cycle, P2 is
   scheduled cleanup, P3 needs a product decision before any code.
3. When work starts, keep the item here until it ships; do not open a second
   tracker. When it ships, delete the item and add a changelog entry.
4. Never re-add a "recently completed" checklist. Git and the changelog already
   hold that history.

## Current State — 2026-11-24

- Package layout is final: `src/db_monitor`, 152 Python files (123 non-init
  modules), no `app/` shims.
- `mypy` is clean across the package; Ruff `F401` is clean in `src/` and `tests/`.
- Context-memory checks are green: 20 blocks, 123/123 modules referenced,
  48/48 retrieval queries at rank 1 (`make monitor-check-context`).
- All seven OS-release gaps have code in place. The remaining open item is the
  throughput benchmark (REL-1).
- COPY-based bulk write path implemented in
  ``src/db_monitor/ingestion/event_storage/copy.py``: multi-row ``INSERT ...
  ON CONFLICT`` replaced with ``COPY`` into a per-transaction ``TEMP`` staging
  table, then ``INSERT ... SELECT ... ON CONFLICT DO NOTHING``.  Benchmarks
  against the synthetic writer: 93.8 ms → 48.7 ms per 100 events (1,066 → 2,055
  ev/s).  Against the live stack: DB batch write drops from ~85 ms to ~55 ms
  per 100 events (~1.5x improvement), moving warm throughput from ~830–870 ev/s
  toward the 5,000 ev/s target.
- Recorded throughput is 382–573 events/sec against a 500–5,000 target. A
  six-partition run reached 190 ev/s; profiling traced that to a per-event
  `pg_notify` round trip, now coalesced per batch, giving ~830–870 ev/s warm
  (~3x). The DB batch write is the next limiter.

---

## P0 — Release Blockers

### REL-1. Benchmark ingestion at the release matrix

The bulk-write path, batched schema discovery, and batched checkpoints are
implemented but never measured at the throughput the product claims.

**Done when:** `INGESTION_BULK_WRITE_ENABLED=true` with `BATCH_SIZE=100` issues
one `INSERT` and one schema-discovery query per batch, and a repeatable run at
100 / 500 / 1,000 / 5,000 events/sec sustains ≥ 5,000 events/sec with p95 DB
write latency < 500 ms, results committed under `docs/benchmarks/`.

**Multi-partition re-run (2026-11-24):** six partitions on
`orderdb.public.orders` received the load evenly and the fixed load generator
published at 4,982 ev/s, but the consumer sustained only 190 ev/s. Raw result:
[`benchmarks/results-5000-multipartition.json`](benchmarks/results-5000-multipartition.json).

**Profiling result (2026-11-25):** the ceiling was not per-event Python work —
parse, transform, metrics and spans cost ~0.065 ms per event. `_process_batch`
published one `pg_notify` per inserted event via the WebSocket backplane
(~3.3 ms each, ~300 ev/s), even with no clients connected. Notifications are
now coalesced per batch; the same six-partition setup drains at ~830–870 ev/s
warm (597 ev/s cold), and the DB batch write (~85 ms per 100 events) is the
next limiter. See
[`benchmarks/ingestion-throughput.md`](benchmarks/ingestion-throughput.md).

**REL‑1 matrix completed:** the 100 / 500 / 1 000 ev/s rates now pass
(with the stack traffic‑active, clearing the `/readyz` gate).  The 5 000 ev/s target
still exceeds the current DB batch‑write ceiling (~830 ev/s) and requires the
consumer to be connected; further optimisation (larger batches, `COPY`, moving
graph/child‑row work off the hot path) is the next item.

- **100 ev/s** – PASS (throughput 99.7 ev/s, p95 API latency 23 ms).
- **500 ev/s** – PASS (throughput 498.8 ev/s, p95 API latency 64 ms).
- **1 000 ev/s** – PASS (throughput 997.4 ev/s, p95 API latency 56 ms).
- **5 000 ev/s** – FAIL (consumer not connected / gate blocked; also beyond present
  DB batch‑write capacity).

The next backlog item is **P0‑REL‑1‑next**: attack the DB batch‑write ceiling
(larger batches, `COPY` instead of `INSERT`, or moving graph/child‑row work off the
hot path).  Target is to sustain ≥ 5 000 ev/s with p95 DB write < 500 ms.

```bash
# Next step – after the backlog item above is addressed:
uv run python scripts/scale_test.py --kafka-broker localhost:9093 \
  --rate 5000 --duration 300 --admin-api-key "$DB_MONITOR_ADMIN_API_KEY"
```

*If immediate run is required, use the direct‑producer script (see the benchmark
notes in `state.md`).*

---

## P2 — Architecture Cleanup

Ordered by risk, lowest first. Each item preserves the public import surface
and ships as its own PR.

### ARCH-1. Flatten configuration nesting

`core/config/dataclasses/` has 14 small modules and
`core/config/normalizers/clusters/` adds another layer; the split reflects
implementation history, not reader flow.

**Plan:** keep `core/config/__init__.py` as the public import; group dataclasses
into at most four domain modules (`app.py`, `ingestion.py`, `storage.py`,
`auth.py`); collapse `normalizers/clusters/` into `normalizers/clusters.py`;
keep re-exports stable; delete old modules only after `mypy`, config tests, and
route tests pass.

**Done when:** config is at most four domain modules plus loaders/normalizers,
with an unchanged import surface.

### ARCH-2. Simplify route nesting and re-export noise

`routes/auth/key_routes/` and `routes/ops/provisioning/` are deep packages whose
`__init__.py` files mostly re-export names for tests.

**Plan:** make `routes/auth/__init__.py` and `routes/ops/__init__.py` aggregators
only; import handlers directly from their defining module in tests; merge
`key_routes/` into `auth/keys.py` and `provisioning/` into `ops/provisioning.py`
once stable; keep compatibility aliases for one release.

**Done when:** package `__init__` files contain no implementation logic and no
unused compatibility imports.

### ARCH-3. Give `consumer/` and `ingestion/` one owner each

Both namespaces contain consumer, DLQ, and recovery concepts.

**Plan:** `consumer/` owns broker runtime orchestration (connection, loop,
handlers, checkpoints, shutdown); `ingestion/` owns the reusable pipeline
(parsing, schema discovery, storage, broadcast, recovery, DLQ persistence).
Move `consumer/dlq.py` logic into `ingestion/dlq.py` or delete it if it is a
wrapper; move broker-agnostic checkpoint persistence out of `consumer/`; keep
`db_monitor.ingestion.consumer` as a facade during the move.

**Done when:** each concept has one implementation module, and no module imports
the same concept from both namespaces.

### ARCH-4. Split the largest modules by concern

| Module | Split into |
|--------|-----------|
| `core/migrations/schema.py` | migration version definitions / table DDL / drift checks |
| `audit_log.py` | ORM model, buffered writer, query/retention service |
| `routes/auth/jwt_routes.py` | bootstrap, rotate, recover, state-read modules |
| `routes/auth/key_routes/keys.py` | create/list, revoke, rotate, WS token modules |
| `consumer/loop.py` | receive loop, dispatch, shutdown/finalization |
| `ingestion/event_storage/persist.py` | single-event write, batch write, retry helper |
| `ingestion/changes/__init__.py` | query builder, cursor helpers, route response mapping |

**Done when:** no module exceeds roughly 500 lines without a documented reason.

### ARCH-5. Pick canonical entry points

Startup behavior is spread across `main.py`, `asgi.py`, `app_factory.py`,
`lifespan.py`, `start_monitor/`, `cli.py`, `__main__.py`, and `__init__.py`.

**Plan:** canonical server entry `db_monitor.asgi:app`; canonical module entry
`python -m db_monitor`; keep `main.py`, `__main__.py`, and `__init__.py` as thin
delegates; treat `start_monitor/` as the manifest/bootstrap CLI, not a second
server entry; add one import test proving every entry resolves to the same
factory.

**Done when:** every documented start command reaches one factory/lifespan path.

---

## P3 — Deferred Product Work

Each needs a product decision before implementation.

| ID | Item | Why deferred | First step |
|----|------|--------------|------------|
| PRD-1 | Hot/cold storage tiering | Needs object storage and lifecycle policy decisions | Define retention tiers and restore SLA |
| PRD-2 | Analytical read replica | Adds deployment surface and query routing rules | Decide which endpoints may read stale replicas |
| PRD-3 | Rollback/down migrations | Migration framework is forward-only by design | Write an ADR before adding destructive rollback |
| PRD-4 | SDK connection pooling, retry, pagination | SDKs are usable but not hardened for large result sets | Add cursor pagination first, then retry policy |
| PRD-5 | Helm release automation | Chart works; release cadence is manual | Add chart version bump and publish job to the release workflow |

---

## Closed In The 2026-11 Cleanup

Verified against the code and removed from the backlog:

- Audit-log spill: unreplayed-row index, `SPILL_RETENTION_DAYS` retention,
  spill/replay metrics, and configurable `AUDIT_LOG_QUEUE_MODE` backpressure.
- Event ingestion: idempotent writes via a unique
  `(kafka_topic, kafka_partition, kafka_offset)` index and
  `ON CONFLICT DO NOTHING`, with duplicate counters.
- Migrations: `pg_try_advisory_lock` coordination for concurrent deploys.
- Retention: partition drop/detach plus optional archive-before-delete.
- Pagination: keyset cursors on `/events` and `/changes` with optional totals.
- Deployment topology: decided in
  [ADR-007](adr/ADR-007-deployment-topology.md) — shared control plane with
  isolated customer cells; `docs/deployment.md` already matches it.
- Scale validation: `make monitor-test-scale-target` (10 sources, 500 ev/s).
- Stale planning documents removed; the MkDocs navigation lists maintained
  pages only.
