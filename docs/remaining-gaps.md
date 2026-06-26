# Remaining Gaps — Comprehensive TODO

**Generated:** 2026-06-23
**Source:** Aggregated from all `docs/*.md` files, `TODO.md`, and codebase audit.

---

> This file catalogs every open gap, workstream, and action item identified
> across the project's documentation. When all items here are marked done,
> DB Monitor will have addressed every improvement suggested in the docs.
>
> Items are grouped by **source document** with the source's priority/phase
> label. Each item includes a **Done when** acceptance criterion.

---

## A. docs/system-quality-improvement-plan.md — Workstreams A–H

### Workstream A: Resolve the Dual Codebase (Priority: P0, Effort: 5-7 days) ✅ COMPLETE

The entire dual-codebase consolidation has been executed. All real logic now
lives in `src/db_monitor/`. The `app/` directory has been deleted.
`legacy_runtime.py` has been removed. No module uses `sys.path.insert`.
235 core tests pass. See `docs/module-inventory.md` for the full migration log.

#### A.1 — Module inventory already exists (`docs/module-inventory.md`) ✅
#### A.2 — Consolidate into a single tree (`src/db_monitor/`)

- [x] **A.2a**: Move COPY modules (`app_factory.py`, `app/api/factory.py`, `app/api/router.py`) — keep one copy in `src/db_monitor/`, delete from `app/`
- [x] **A.2b**: Move leaf proxy targets with no internal `app/` deps first: `metrics.py`, `ws_manager.py`, `tracing.py`, `retention.py`, `extensions.py`
- [x] **A.2c**: Move core modules: `core/models.py`, `core/responses.py`, `lifecycle_manager.py`, `auth.py`, `audit_log.py`
- [x] **A.2d**: Move ingestion modules: `event_parser.py`, `event_pipeline.py`, `schema_discovery.py`, `message_brokers.py`, `change_processor.py`, `source_metadata.py`, `row_identity.py`, `ingestion_quota.py`, `webhooks.py`
- [x] **A.2e**: Move consumer: `consumer/recovery.py`, `consumer/runtime_state.py`, `consumer_service.py`
- [x] **A.2f**: Move routes, repositories, remaining: `routes/*`, `repositories/*`, `lifespan.py`
- [x] **A.2g**: Move entry points and config: `config.py`, `main.py`, `start_monitor.py`, `migrate.py`, `migrations.py`

**Done when:** `pip install -e .` gives a fully functional `db_monitor` package. All tests import from `db_monitor.*` and pass. `python -m db_monitor` works. No module uses `sys.path.insert` / `ensure_legacy_path()`. ✅

#### A.3 — Update all imports

- [x] Change `tests/` imports from `from app.*` → `from db_monitor.*`
- [x] Update SDK imports
- [x] Update script imports

**Done when:** No `from app import` or `from app.` remains in any `.py` file outside `app/` itself. ✅

#### A.4 — Remove dead code

- [x] Delete `app/core/config.py` (the proxy)
- [x] Delete `app/core/__init__.py` (lazy loader)
- [x] Delete `app/__init__.py` (empty)
- [x] Delete `app/config.py` (replaced by canonical version)
- [x] Delete all proxy/wrapper `__getattr__` modules in `src/db_monitor/`
- [x] Delete `src/db_monitor/legacy_runtime.py`
- [x] ~~Delete `src/db_monitor/asgi.py`~~ — Refactored to clean re-export (not a proxy). KEPT.
- [x] Delete `src/db_monitor/core/db.py`
- [x] Delete `src/db_monitor/core/lifecycle.py`
- [x] Delete the entire `app/` directory
- [x] Update `Dockerfile` and `container-entrypoint.sh` to reference `src/db_monitor/` only

**Done when:** Full CI pipeline passes, container builds, `pip install .` works. ✅

---

### Workstream B: Unify Configuration (Priority: P0, Effort: 3-5 days, Depends on: A) ✅ COMPLETE

The entire config unification has been executed. All config is loaded from a single `AppSettings.from_env()` call in `src/db_monitor/core/config.py`. The old module-level `os.getenv()` pattern has been replaced with typed `@dataclass` fields. 236 core tests pass, 100% of consumer files updated.

#### B.1 — Create a single config module

- [x] Rewrite config as `@dataclass` `AppSettings` with lazy loading (not module-level `os.getenv()`)
- [x] Centralize validation in `validate()` method
- [x] Type all config values with typed fields
- [x] Add docstrings per field (purpose, default, units)
- [x] Place at `src/db_monitor/core/config.py`

**Done when:** All config is loaded from one `AppSettings.from_env()` call. No module does `os.getenv()` at import time. Tests use `monkeypatch` on the config class, not environment variables. ✅

#### B.2 — Remove redundant config files

- [x] Delete `app/config.py` (deleted as part of Workstream A)
- [x] Delete `app/core/config.py` (deleted as part of Workstream A)
- [x] Delete `src/db_monitor/config.py` (old module-level config)
- [x] Update `src/db_monitor/core/__init__.py` to export `config_loader` only

#### B.3 — Update all consumers

- [x] Change all 16 consumer files from `from db_monitor.config import ...` → `from db_monitor.core.config import settings` + `settings.attr`
- [x] Update all 9 test files to use `monkeypatch.setattr` on `settings.*` attributes

**Done when:** Only one canonical config import path exists. ✅

---

### Workstream C: Break Up Monoliths (Priority: P1, Effort: 5-8 days, Depends on: A, B)

#### C.1 — Split `consumer_service.py` (~1300 lines)

- [ ] Extract `db_monitor/consumer/consumer.py` — main message loop + connection management
- [ ] Extract `db_monitor/consumer/batch.py` — batch processing logic
- [ ] Extract `db_monitor/consumer/dlq.py` — dead-letter queue operations
- [ ] Extract `db_monitor/consumer/checkpoint.py` — offset checkpoint management
- [ ] Extract `db_monitor/consumer/webhook.py` — webhook delivery (from `_broadcast_event`)

**Done when:** `consumer_service.py` is removed. Each new module is <300 lines and focused on one concern.

#### C.2 — Split `app/config.py` (after consolidation to dataclass)

- [ ] Create `db_monitor/core/config/__init__.py` — exports unified `AppSettings`
- [ ] Create `db_monitor/core/config/settings.py` — dataclass definitions
- [ ] Create `db_monitor/core/config/loaders.py` — file loading, CSV parsing, JSON loading
- [ ] Create `db_monitor/core/config/validators.py` — cross-field validation rules

#### C.3 — Refactor API routes

- [ ] Ensure `/events`, `/changes`, `/tables` each have their own route module
- [ ] Extract middleware and dependency injection into a separate module

#### C.4 — Split `consumer_task` (~500 lines)

- [ ] Extract `_connect_with_retry()` — connection loop
- [ ] Extract `_process_message()` — single message handling
- [ ] Extract `_quota_check()` — quota enforcement step
- [ ] Extract `_handle_error()` — error path with DLQ fallback

**Done when:** Each extracted function is <80 lines and has unit tests.

---

### Workstream D: Formalize Database Migrations (Priority: P1, Effort: 2-3 days, Depends on: A)

#### D.1 — Make migrations the primary path

- [ ] Remove `Base.metadata.create_all()` from the startup lifecycle
- [ ] Ensure `migrations.py` is always called during `db-monitor-migrate`
- [ ] Add a `db-monitor-migrate validate` command that checks schema drift

**Done when:** Startup never calls `create_all()`. Schema is always managed by versioned migrations.

#### D.2 — Migration testing

- [ ] Add test that runs all migrations on a fresh database
- [ ] Verify final schema matches SQLAlchemy model definitions
- [ ] Add test for rollback/down migrations (if supported)

#### D.3 — Document migration workflow

- [ ] Create `docs/migrations.md` explaining how to create, apply, and validate migrations
- [ ] Include migration naming convention and review checklist
- [ ] Reference the advisory-lock behavior (lock ID 7273646)

---

### Workstream E: Test Infrastructure Overhaul (Priority: P1, Effort: 3-5 days, Depends on: A)

#### E.1 — Eliminate timing-dependent tests

- [ ] Audit all tests for `asyncio.sleep()` with values < 0.5s
- [ ] Replace with explicit event coordination (`asyncio.Event`, `anyio.Event`)
- [ ] Add `CI_SAFE_*` constants for lifecycle tests (similar to quota tests)

**Done when:** `make test` has 0 flakes in 10 consecutive runs.

#### E.2 — Add integration test fixtures

- [ ] Create `conftest.py` fixture providing a real (or in-memory) Kafka broker for consumer tests
- [ ] Use `testcontainers` or lightweight embedded broker

#### E.3 — Improve test isolation

- [ ] Ensure each test creates its own state (no shared session factories across tests)
- [ ] Add `autouse` fixtures that clean up global state between tests
- [ ] Add fixture for temporary database with migrations applied

#### E.4 — Add property-based tests

- [ ] For `event_parser.py`: generate random Debezium payload shapes and verify parsing invariants
- [ ] For `ingestion_quota.py`: verify quota decisions are monotonic (more events → less allowed)

---

### Workstream F: Documentation Overhaul (Priority: P1, Effort: 3-4 days, Depends on: A)

#### F.1 — Module-level docstrings

- [ ] Scan every public module and add docstring explaining: what it does, key exports, brief example, related modules
- [ ] Prioritize modules with no docstrings: `app/__init__.py`, `app/routes/utils.py`, `app/message_brokers.py`

#### F.2 — Architecture Decision Records

- [ ] Create `docs/adr/` directory
- [ ] ADR-001: Why two codebases existed (and why consolidated)
- [ ] ADR-002: Why RabbitMQ support alongside Kafka
- [ ] ADR-003: Why quota throttle loop uses sleep instead of pushback from broker
- [ ] ADR-004: Why WebSocket tokens have a short TTL

#### F.3 — Contributing Guide

- [ ] Create `CONTRIBUTING.md` at repo root including:
  - Development environment setup
  - Code style (ruff settings, type annotation requirements)
  - Testing expectations (new code must have tests)
  - Pull request process
  - How to run full CI suite locally

#### F.4 — Changelog

- [ ] Create `CHANGELOG.md` at repo root (Keep a Changelog format)
- [ ] Log all changes from this session and prior releases

#### F.5 — API Documentation

- [ ] Generate static site from FastAPI's OpenAPI schema
- [ ] Host on GitHub Pages or similar
- [ ] Include example requests/responses per endpoint

#### F.6 — Security Policy

- [ ] Create `SECURITY.md` at repo root with:
  - How to report vulnerabilities
  - Supported versions
  - Disclosure timeline

#### F.7 — README Rewrite

- [ ] Restructure README.md as: One-line description → Quick Start (30s) → Features → Architecture diagram → Documentation → Installation → Configuration → Development → License
- [ ] Add framework-first positioning (not demo-first)

---

### Workstream G: Developer Experience (Priority: P2, Effort: 2-3 days, Depends on: A, F)

#### G.1 — Pre-commit hooks

- [ ] Create `.pre-commit-config.yaml` with: `ruff check`, `ruff format`, `mypy`, trailing whitespace + EOF fixers

#### G.2 — Makefile modernization

- [ ] Add `monitor-lint-fix` target (auto-fix lint issues)
- [ ] Add `monitor-test-quota` target (run only quota tests)
- [ ] Add `monitor-test-lifecycle` target (run only lifecycle tests)
- [ ] Rename to shorter aliases: `make test`, `make lint`, `make typecheck`

#### G.3 — VS Code configuration

- [ ] Update `.vscode/settings.json`: `python.defaultInterpreterPath` → `.venv`, `python.analysis.typeCheckingMode = "basic"`, `ruff.enable = true`

#### G.4 — Dockerfile improvement

- [ ] Consolidate to single `Dockerfile` at repo root
- [ ] Install via `pip install .`
- [ ] Use `db-monitor-serve` as entrypoint
- [ ] Support `DB_SCHEMA_MODE` for migration strategy

---

### Workstream H: Packaging and Distribution (Priority: P2, Effort: 2-3 days, Depends on: A, F)

#### H.1 — Version management

- [ ] Add `__version__` to `src/db_monitor/__init__.py` readable without `importlib.metadata`
- [ ] Tag releases in git with `v*.*.*`

#### H.2 — CI/CD pipeline

- [ ] Publish to PyPI on tagged commits (Test PyPI first)
- [ ] Add release workflow that builds wheel + sdist and uploads
- [ ] Add `publish` Makefile target
- [ ] Verify container image build + push is wired
- [ ] Verify Helm chart release is wired

#### H.3 — SDK maturity

- [ ] Python SDK: Add async support
- [ ] Python SDK: Add type annotations and mypy compliance
- [ ] Python SDK: Add docstrings for all public methods
- [ ] Python SDK: Package separately on PyPI as `db-monitor-client`
- [ ] JavaScript SDK: Add `package.json`
- [ ] JavaScript SDK: Make publishable to npm
- [ ] Both SDKs: Add connection pooling, retry, pagination helpers

---

## B. docs/os-release-readiness-assessment.md — Secondary Priorities

#### OS-1 — Hosted documentation site

- [ ] Set up MkDocs or Docusaurus
- [ ] Include: install guide, API reference, configuration reference, architecture deep-dive
- [ ] Host on GitHub Pages or similar
- [ ] Wire CI to rebuild on merge to main

**Done when:** A docs site is live at a stable URL covering the categories above.

#### OS-2 — CONTRIBUTING.md, CHANGELOG.md, CODE_OF_CONDUCT.md

- [ ] Create `CONTRIBUTING.md` (see also F.3 above)
- [ ] Create `CHANGELOG.md` (see also F.4 above)
- [ ] Create `CODE_OF_CONDUCT.md`

**Done when:** All three files exist at repo root with realistic content.

#### OS-3 — CI/CD release pipeline verification

- [ ] Verify PyPI publish is wired (GitHub Action or similar)
- [ ] Verify container image build + push on tag
- [ ] Verify Helm chart release on tag
- [ ] Verify signed/tagged distribution
- [ ] Document verification results

**Done when:** A tagged commit triggers: PyPI publish → container image build → Helm chart release, all verified by CI.

#### OS-4 — SDK maturity (also H.3)

- [ ] See Workstream H.3 above

#### OS-5 — Audit log backpressure policy

- [ ] Define what happens when the audit log buffer fills: drop, block, or durable fallback
- [ ] Make the queue strategy configurable
- [ ] Document the default behavior in `docs/configuration.md`
- [ ] Add tests covering the chosen failure mode

**Done when:** The backpressure policy is configurable, documented, and tested.

#### OS-6 — WebSocket fanout at scale

- [ ] Evaluate whether Postgres `LISTEN/NOTIFY` becomes a bottleneck at high event fanout
- [ ] If needed, implement dedicated pub/sub backplane (Redis)
- [ ] Benchmark before/after

**Done when:** Decision documented in ADR. If bottleneck confirmed, Redis backplane implemented and benchmarked.

#### OS-7 — Multi-tenancy hardening (shared plane)

- [ ] Implement row-level security for tenant isolation
- [ ] Add tenant-scoped authz policies
- [ ] Add tenant-scoped query filters
- [ ] Add per-tenant quotas and budget-based throttling

**Done when:** Shared-plane deployment can isolate N tenants with row-level security, tested at N=10.

---

## C. docs/readiness-review-principal-2026-05-24.md — Phase 3 Scale Hardening

#### RR-1 — Storage scaling strategy

- [ ] Tune index strategy for high-cardinality filters on `events` and `column_changes`
- [ ] Evaluate need for sub-partitioning (by tenant key within time ranges)

#### RR-2 — Data architecture evolution

- [ ] Implement optional hot/cold tiering for historical event data
  - Hot: current partitions on fast storage
  - Cold: detached partitions moved to cheaper storage or archived
- [ ] Implement optional analytical replica / read model for heavy query workloads

#### RR-3 — WebSocket fanout architecture

- [ ] Evaluate dedicated fanout bus for high-scale realtime clients (see also OS-6)
- [ ] Document decision with benchmark data

**Done when:** Hot/cold tiering is documented and configurable. WebSocket fanout benchmarked at target scale and decision documented.

---

## D. docs/framework-transition-plan.md — Framework-First Transition

#### FT-1 — Product framing and docs

- [ ] Rewrite `README.md` overview and quick start around framework-first positioning (see also F.7)
- [ ] Update `docs/architecture.md` to describe generic source systems (demo topology → example docs)
- [ ] Update `docs/configuration.md` to distinguish required runtime settings from example-only values
- [ ] Add explicit example-stack documentation (see also FT-3)

**Done when:** A new reader can tell what is core product vs example. The default docs no longer imply `orderdb` / `catalogdb` / `shippingdb` are required.

#### FT-2 — Source-manifest public contract

- [ ] Document manifest schema and ownership rules as a public configuration API
- [ ] Support template/example manifests distinctly from user manifests
- [ ] Remove assumptions that the checked-in manifest (`connectors/sources.json`) is always the active runtime
- [ ] Ensure docs explain how to provide a custom manifest

**Done when:** Users can point the app and registrar at a manifest they own. Example manifests work through the same contract.

#### FT-3 — Runtime/bootstrap separation

- [ ] Decouple compose layout or introduce profiles/overlays to separate core from demo
- [ ] Make demo source databases optional
- [ ] Ensure app startup messaging guides users when no sources are configured

**Done when:** The product can be deployed without bundling demo databases. The example stack remains easy to launch locally.

#### FT-4 — Script and asset cleanup

- [ ] Classify scripts as core utilities vs example tooling
- [ ] Relocate or relabel seed SQL and demo generators under `examples/sandbox/`
- [ ] Make script names and docs reflect their purpose clearly

**Done when:** Product utilities are discoverable without reading demo-specific scripts. Demo assets are clearly named and grouped.

#### FT-5 — Test suite separation

- [ ] Classify test targets and CI jobs as core vs example-stack
- [ ] Keep demo-backed integration coverage but clarify its role
- [ ] Stabilize live integration workflow separately from product refactoring

**Done when:** CI surfaces whether a failure belongs to core behavior or the example stack.

#### FT-6 — Packaging and adoption flow

- [ ] Document deployable runtime artifacts
- [ ] Define starter templates for manifests and secrets
- [ ] Align SDK/examples with framework-first onboarding

**Done when:** A new adopter has a clear path to onboard their own systems without editing demo-owned files first.

---

## E. docs/module-inventory.md — Migration Phases 1a–1g

(These are the physical steps for Workstream A. Already captured above in A.2a–A.2g.)

- [ ] **Phase 1a**: Low-risk moves — `app_factory.py`, `extensions.py`, `ws_manager.py`, `tracing.py`, `retention.py`, `metrics.py`
- [ ] **Phase 1b**: Core modules — `core/models.py`, `core/responses.py`, `lifecycle_manager.py`, `audit_log.py`, `auth.py`
- [ ] **Phase 1c**: Ingestion modules — `event_parser.py`, `event_pipeline.py`, `schema_discovery.py`, `message_brokers.py`, `change_processor.py`, `source_metadata.py`, `row_identity.py`, `ingestion_quota.py`, `webhooks.py`
- [ ] **Phase 1d**: Consumer — `consumer/recovery.py`, `consumer/runtime_state.py`, `consumer_service.py`
- [ ] **Phase 1e**: Routes, repositories, remaining — `routes/*`, `repositories/*`, `lifespan.py`
- [ ] **Phase 1f**: Entry points and config — `config.py`, `main.py`, `start_monitor.py`, `migrate.py`, `migrations.py`
- [ ] **Phase 1g**: Cleanup — delete all proxies, `legacy_runtime.py`, `app/` directory, update Dockerfile

**Done when:** Full CI pipeline, `pip install .`, `python -m db_monitor`, `db-monitor --help` all work.

---

## F. TODO.md — Should-Fix Soon + Review Follow-ups

#### TODO-A — Choose and document deployment topology decision

- [ ] Document decision: per-startup isolated deployments vs shared multi-tenant plane
- [ ] Reference `docs/pre-v1-tenant-isolation-strategy.md`

**Done when:** Decision is written up, rationale explained, and referenced from `docs/deployment.md`.

#### TODO-B — Retention and archival policy automation

- [ ] Automate retention for `events`, `column_changes`, and DLQ history
- [ ] Scheduled purge/archival jobs with audit trails
- [ ] Configurable TTL per table

**Done when:** Retention policies are configurable, scheduled jobs run on a cron, and audit trails track each purge/archive.

#### TODO-C — Partial index + purge job for `api_audit_log_spill`

- [ ] Add a partial index so unreplayed spill lookups stay fast
- [ ] Add purge/retention job that expires old replayed rows on a schedule
- [ ] Cover with tests

**Done when:** Unreplayed replay queries use a targeted index, old replayed rows are expired/archived on a schedule, tests pass.

#### TODO-D — Spill-rate + replay-observability metrics for audit logging

- [ ] Add Prometheus counters/gauges for: queue pressure, flush failures, spill volume, replay success/failure trends
- [ ] Render metrics in Grafana dashboards
- [ ] Add alert rules for sustained degradation

**Done when:** Prometheus metrics exist, dashboards render them, alert rules fire on sustained degradation.

#### TODO-E — Define audit-log backpressure policy (same as OS-5)

- [ ] See OS-5 above

#### TODO-F — Add partial index and purge/retention job for `api_audit_log_spill`

- [ ] Covered by TODO-C above

---

## G. docs/benchmarks/ — Run + Record Benchmark Results

#### BM-1 — Run baseline benchmark

- [ ] Run `make monitor-test-scale-target` (500 ev/s, 5 min, 10 sources)
- [ ] Record results in `docs/benchmarks/results-500.json`

#### BM-2 — Run full benchmark matrix

- [ ] 100 ev/s for 60s → `docs/benchmarks/results-100.json` (replace placeholder)
- [ ] 500 ev/s for 300s → `docs/benchmarks/results-500.json`
- [ ] 1000 ev/s for 300s → `docs/benchmarks/results-1000.json`
- [ ] 5000 ev/s for 300s → `docs/benchmarks/results-5000.json`

#### BM-3 — Publish benchmark results

- [ ] Update `docs/benchmarks/ingestion-throughput.md` with actual numbers
- [ ] Record in release notes
- [ ] Add benchmark results to CI artifacts on each release

**Done when:** Target thresholds met: ≥5000 ev/s sustained, p95 ingestion <500ms, p95 API <200ms, zero DLQ events.

---

## H. docs/troubleshooting.md — Additional Scenarios Already Added ✅

The following scenarios were added in the 2026-06-23 docs update and are **done**:
- Consumer Lag / Commit Age Alerts ✅
- Ingestion Throttled Or Dropped ✅
- Duplicate Events Observed ✅

---

## I. docs/deployment.md — Improvements Already Added ✅

- Helm chart details expanded ✅
- Partition management + migration locking §3c.7 added ✅

---

## J. docs/architecture.md — Improvements Already Added ✅

- Dual-codebase clarification note added ✅

---

## K. docs/api-stability.md — No Changes Needed ✅

Document is concise, accurate, and up to date.

---

## L. docs/configuration.md — No Critical Gaps ✅

Comprehensive and up to date.

---

## Summary by Effort

| Group | Items | Estimated Person-Days | Dependencies |
|-------|-------|-----------------------|--------------|
| **A (Dual codebase)** | 10 sub-items | 5-7 | None |
| **B (Configuration)** | 3 sub-items | 3-5 | A | ✅ COMPLETE |
| **C (Monoliths)** | 4 sub-items | 5-8 | A, B |
| **D (Migrations)** | 3 sub-items | 2-3 | A |
| **E (Tests)** | 4 sub-items | 3-5 | A |
| **F (Docs)** | 7 sub-items | 3-4 | A |
| **G (DevX)** | 4 sub-items | 2-3 | A, F |
| **H (Packaging)** | 3 sub-items | 2-3 | A, F |
| **OS (Secondary)** | 7 items | 5-8 | A (some) |
| **RR (Scale)** | 3 items | 3-5 | A |
| **FT (Framework)** | 6 items | 4-6 | A |
| **TODO** | 5 items | 3-5 | A (some) |
| **BM (Benchmarks)** | 3 items | 1-2 | Stack running |
| **Total** | **~62 items** | **~41-64 person-days** | |

### Suggested Execution Order

```
Phase 0: Benchmarks (BM-1, BM-2) — needs stack, independent
Phase 1: A (dual codebase) + B (config) — removes biggest confusion ✅ Both complete
Phase 2: C (monoliths) + D (migrations) — navigable codebase
Phase 3: E (tests) + F (docs) + FT-1, FT-2 — foundation for contributions
Phase 4: G (DevX) + H (packaging) + FT-3..FT-6 — polish
Phase 5: OS items + RR items + TODO items — production readiness
```
