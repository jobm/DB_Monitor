# Project Backlog

This file tracks realistic remaining work. Delivered items stay summarized here,
while exploratory or longer-horizon ideas live under `Future ideas` so the
active backlog stays readable.

## Recently Closed

### Framework-First Transition & CI Stabilization

- [x] Reposition the bundled `orderdb` / `catalogdb` / `shippingdb` stack as example assets instead of product-default assumptions
- [x] Define a framework-first configuration and packaging model for DB Monitor so new adopters can bring their own sources without editing repo-owned demo assets
- [x] Split docs, setup flows, and tests into `core-platform` versus `evaluation sandbox` paths so examples remain useful without shaping the runtime contract
- [x] Introduce formal source manifest contracts via JSON schemas (`sources.schema.json` & templates)
- [x] Decouple container services using docker compose profiles (`sandbox`) and align Makefile orchestrations
- [x] Stabilize the CI `Wait for app readiness` pipeline using pg_isready database sockets and verbose loop poll logs
- [x] Clean up and align SDK client workflow targets with the decoupled framework topology
- [x] Move canonical demo automation and seed assets under `examples/sandbox` while keeping top-level script entrypoints as compatibility shims

### Documentation And Developer Experience

- [x] Add comprehensive API documentation
- [x] Create example implementations
- [x] Add developer setup scripts
- [x] Improve API and configuration error messages with clearer remediation hints
- [x] Add architecture documentation
- [x] Create troubleshooting guide
- [x] Document deployment procedures
- [x] Add configuration examples

### Core Product And Operations

- [x] Redesign event storage around structured `KafkaEvent` records
- [x] Add event filtering, validation, and transformation configuration
- [x] Add authentication, RBAC, and API audit logging
- [x] Add Kafka SSL/TLS support
- [x] Add batch processing, caching, query optimization, and pooling
- [x] Add Prometheus metrics, alerting config, and dashboards
- [x] Add pagination, event-type filters, date-range queries, and search
- [x] Add unit, integration, smoke-load, and test-data tooling
- [x] Finalize the TUI change-history workflow and record drilldown UX

## Future Ideas

These are not committed backlog items yet.

### Advanced Features

- [x] Create a dedicated admin interface

### Recently Closed Future-Idea Work

- [x] Add support for custom event processors
- [x] Expand replay beyond current DLQ replay into fuller replay workflows
- [x] Add support for multiple Kafka clusters

### Scalability And Observability

- [x] Validate horizontal scaling under automated tests
- [x] Add distributed tracing
- [x] Add partition-aware scaling and placement controls
- [x] Add clustering support for more stateful components

### Integration Surface

- [x] Add support for additional databases
- [x] Implement webhook notifications
- [x] Create SDKs for common languages
- [x] Add support for different message brokers
- [x] Replace Kafka-style checkpoint semantics with broker-native progress tracking for RabbitMQ and other non-Kafka backends
- [x] Package the Python SDK for normal package imports and add package-level import coverage

## Active Backlog

### Package Standardization For OSS Release

- [x] Phase 1: Establish canonical package metadata and CLI entrypoints
- [x] Phase 2: Migrate runtime imports to a canonical `db_monitor` namespace
- [x] Phase 3: Move package layout to `src/` and remove compatibility shims
- [ ] Phase 4: Define and document public API stability contracts
- [ ] Phase 5: Harden quality gates (ruff, type checks, packaging smoke tests)
- [ ] Phase 6: Add automated release workflows and signed/tagged distribution

#### Remaining Checklist

- [x] Phase 2: Move auth/runtime modules into src/db_monitor and keep app shims
- [x] Phase 2: Move ingestion modules into src/db_monitor and keep app shims
- [x] Phase 2: Move repositories and route composition into src/db_monitor
- [x] Phase 2: Replace legacy flat imports in app/main.py with canonical package imports
- [x] Phase 2: Add package-level import tests for auth, ingestion, repositories, and routing surfaces
- [x] Phase 2: Validate docker compose startup using package-native entry commands end-to-end
- [x] Phase 2: Re-sync uv.lock after all namespace migration edits stabilize

- [x] Phase 3: Remove app compatibility shims after full src/db_monitor migration
- [x] Phase 3: Remove sys.path bridge helpers once legacy imports are eliminated
- [x] Phase 3: Collapse duplicate packaging metadata to one canonical pyproject
- [x] Phase 3: Update docs and make targets to use package-native module invocations only

- [ ] Phase 4: Define supported public Python API modules and document stability guarantees
- [ ] Phase 4: Define deprecation and removal policy with minimum grace window
- [ ] Phase 4: Add versioning policy and release compatibility matrix

- [ ] Phase 5: Add Ruff config and enforce lint gate in CI
- [ ] Phase 5: Add static typing gate (mypy or pyright) for package namespace
- [ ] Phase 5: Add packaging smoke tests for wheel and sdist install/import
- [ ] Phase 5: Add container smoke test for monitor-server readiness in CI

- [ ] Phase 6: Add release workflow with tag-triggered build and publish
- [ ] Phase 6: Add changelog automation and release-note generation
- [ ] Phase 6: Add signed release tags and artifact provenance checks

- [ ] Investigate readiness gate where `last_message_at` is set but
	`last_commit_at` remains null under sandbox startup, causing persistent
	`/readyz` 503 despite connected consumer and healthy DB/lifecycle.

#### In Progress Notes

- Added initial root and app script metadata for `db_monitor` entrypoints.
- Added compatibility-first `src/db_monitor` package scaffolding:
	CLI dispatch, ASGI export, module execution, and legacy runtime bridge.
- Migrated API namespace slice to canonical package modules:
	`src/db_monitor/api/factory.py` and `src/db_monitor/api/router.py`, with
	compatibility wrappers in `app/api/` delegating through namespace bridges.
- Migrated core namespace slice to canonical package modules:
	`src/db_monitor/core/config.py` and `src/db_monitor/core/db.py`, with
	`app/core/config.py` and `app/core/db.py` delegating to canonical proxies.
- Added canonical top-level runtime surfaces:
	`src/db_monitor/config.py` and `src/db_monitor/extensions.py` now proxy to
	legacy runtime modules during migration, with package-surface tests added.
- Added canonical startup surfaces:
	`src/db_monitor/start_monitor.py` and `src/db_monitor/main.py` provide
	package-native startup/ASGI entry modules while delegating to legacy runtime.
- Switched container/runtime bootstrap toward package-native entrypoints:
	`app/container-entrypoint.sh` now runs `python -m db_monitor...`; Docker
	build context now includes `src/` via repo-root build in compose + Dockerfile.
- Added package-level namespace coverage in
	`tests/test_package_phase2_surfaces.py` for auth, ingestion,
	repositories, and routing module imports.
- End-to-end compose startup validation now runs with package-native startup
	entry commands (`python -m db_monitor...`) and reaches connected consumer
	state. Current `/readyz` remains not-ready due existing commit-gate logic
	(`last_message_at` with null `last_commit_at`) tracked as follow-up.
- Phase 3 kickoff: local dev and migration commands now call package-native
	entry modules from Makefile and README workflows.
- Active runtime route/config modules now import canonical `db_monitor`
	namespaces directly instead of app/core shim paths.
- Removed obsolete `app/api` and `app/core` compatibility shim modules after
	runtime rewiring; active tests pass without those shim packages.
- Consolidated packaging metadata into root `pyproject.toml` and root
	`uv.lock`; removed `app/pyproject.toml` and `app/uv.lock` duplicates.
- Removed obsolete `app/ingestion` wrapper modules by pointing canonical
	`src/db_monitor/ingestion/*` modules directly at runtime modules.
- Removed `app/_namespace_bridge.py` and switched `app/main.py` to direct
	canonical `db_monitor.*` imports with no dynamic bridge bootstrap.
- Broader core-marker validation now passes from `tests/` (`132 passed`).
