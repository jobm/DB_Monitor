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

### Pre-V1 Launch Execution (Shared Control Plane + Isolated Cells)

#### Delivery Backlog

- [x] Implement automated customer cell provisioning workflow
	(namespace, secrets, DB target, broker namespace, deploy, migrate, verify)
- [x] Implement customer-scoped bootstrap + rotation workflow for API/JWT
	secrets
- [x] Add deployable namespace guardrails (ResourceQuota, limits,
	NetworkPolicy)
- [x] Add customer lifecycle actions (suspend, resume, upgrade one customer
	cell)
- [x] Add per-customer observability labels and alert routing integration
- [x] Add onboarding orchestration status tracking and audit trail
- [x] Enforce customer scope for mutating operator APIs

#### One-Go Closure Checklist

- [x] Close provisioning workflow end-to-end
	Done when namespace, secrets/config, DB target, broker namespace,
	deployment, migrations, and verify adapters are implemented with real
	provider hooks and integration tests.
- [x] Close scoped bootstrap and rotation for API/JWT secrets
	Done when per-customer secret bootstrap, rotation API, audit trail, and
	recovery path tests are implemented.
- [x] Close deployable namespace guardrails
	Done when ResourceQuota, limits, and NetworkPolicy templates are generated
	per customer with validation tests.
- [x] Close per-customer observability and alert routing
	Done when customer labels propagate to metrics/logs/traces and alert routing
	rules are verified by tests.
- [x] Close customer scope on all mutating operator APIs
	Done when every mutating admin endpoint enforces customer scope and route
	tests cover both allow and reject paths.

#### Started

- [x] Initial slice: require explicit customer scope on mutating
	`/admin/dlq/*/replay` endpoints
- [x] Initial slice: add customer lifecycle control-plane endpoints for
	`/admin/customers/{customer_id}/(lifecycle|suspend|resume|upgrade)` with
	customer scope validation
- [x] Initial slice: add customer provisioning orchestration jobs with
	step-level status and audit trail endpoints under
	`/admin/customers/{customer_id}/provision-jobs*`
- [x] Initial slice: add provisioning control-plane execution endpoint for
	ordered step transitions (`/admin/customers/{customer_id}/provision-jobs/{job_id}/execute`)
- [x] Initial slice: add execution idempotency key, failed-step retries,
	attempt metadata, and provisioning step metrics hooks
- [x] Initial slice: wire provisioning execution to concrete step adapters
	(with stored per-step adapter result payloads and error mapping)
- [x] Initial slice: run concrete DB migration/readiness verification during
	provisioning and sync lifecycle state on bootstrap activation
- [x] Initial slice: add provider-pluggable adapter registry for remaining
	namespace/secrets/broker/deploy provisioning steps

### Package Standardization For OSS Release

- [x] Phase 1: Establish canonical package metadata and CLI entrypoints
- [x] Phase 2: Migrate runtime imports to a canonical `db_monitor` namespace
- [x] Phase 3: Move package layout to `src/` and remove compatibility shims
- [x] Phase 4: Define and document public API stability contracts
- [x] Phase 5: Harden quality gates (ruff, type checks, packaging smoke tests)
- [x] Phase 6: Add automated release workflows and signed/tagged distribution

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

- [x] Phase 4: Define supported public Python API modules and document stability guarantees
- [x] Phase 4: Define deprecation and removal policy with minimum grace window
- [x] Phase 4: Add versioning policy and release compatibility matrix

- [x] Phase 5: Add Ruff config and enforce lint gate in CI
- [x] Phase 5: Add static typing gate (mypy or pyright) for package namespace
- [x] Phase 5: Add packaging smoke tests for wheel and sdist install/import
- [x] Phase 5: Add container smoke test for monitor-server readiness in CI

- [x] Phase 6: Add release workflow with tag-triggered build and publish
- [x] Phase 6: Add changelog automation and release-note generation
- [x] Phase 6: Add signed release tags and artifact provenance checks

- [x] Investigate readiness gate where `last_message_at` is set but
	`last_commit_at` remains null under sandbox startup, causing persistent
	`/readyz` 503 despite connected consumer and healthy DB/lifecycle.

- [x] Reconcile `TODO.md` migration notes with current facade state so
	completed-history comments do not claim removed modules that are present.
- [x] Resolve canonical route namespace ambiguity in `src/db_monitor`
	(`routes.py` module vs `routes/` package) by standardizing on one shape.
- [x] Remove or repurpose empty `app/db_monitor/` placeholder directory.
- [x] Finish remaining flat-import cleanup in `app/extensions.py`,
	`app/core/config.py`, and `app/consumer/runtime_state.py`.
- [x] Fix outstanding lint diagnostic in
	`tests/test_package_namespace.py` (PEP8 line length).

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
- Added thin compatibility facades under `app/api`, `app/core`, and
	`app/ingestion` so the legacy app runtime has a clearer modular package
	shape instead of empty namespace folders.
- Migrated additional legacy imports to package facades by routing
	`app/routes/*` and selected runtime modules (`auth`, `audit_log`,
	`message_brokers`, `webhooks`) through `core.*` wrappers instead of
	direct flat-module imports.
- Completed a high-level import cleanup across runtime internals by routing
	`consumer_service`, `schema_discovery`, `ws_manager`, `consumer/recovery`,
	`change_processor`, `event_pipeline`, `repositories/events`,
	`lifecycle_manager`, `migrate`, and `migrations` through `core.config`,
	`core.db`, and `core.models` facades.
- End-to-end compose startup validation now runs with package-native startup
	entry commands (`python -m db_monitor...`) and reaches connected consumer
	state. `/readyz` commit-gate behavior is now stable: filtered events and
	DLQ-handled failures are acknowledged and checkpointed so
	`last_commit_at` advances and readiness does not remain stuck at 503.
- Startup manifest validation now derives required fields/allowed keys and
	regex patterns from `connectors/sources.schema.json` so runtime checks stay
	aligned with the documented source contract.
- Phase 3 kickoff: local dev and migration commands now call package-native
	entry modules from Makefile and README workflows.
- Active runtime route/config modules now import canonical `db_monitor`
	namespaces directly instead of app/core shim paths.
- Added maintained `app/api` and `app/core` compatibility facades to keep the
	legacy runtime import surface modular while canonical code lives in
	`src/db_monitor`.
- Consolidated packaging metadata into root `pyproject.toml` and root
	`uv.lock`; removed `app/pyproject.toml` and `app/uv.lock` duplicates.
- Added maintained `app/ingestion` compatibility facades so legacy runtime
	modules map cleanly onto package-shaped ingestion namespaces.
- Removed `app/_namespace_bridge.py` and switched `app/main.py` to direct
	canonical `db_monitor.*` imports with no dynamic bridge bootstrap.
- Broader core-marker validation now passes from `tests/` (`132 passed`).
- Added a formal API stability contract doc (`docs/api-stability.md`) with
	public module/symbol list, deprecation window, and compatibility matrix.
- Added machine-readable contract constants in
	`src/db_monitor/public_api.py` plus import/symbol policy tests.
- Added Phase 5 quality gates:
	Ruff + mypy configuration in root `pyproject.toml`.
- Added Make targets for linting, type-checking, packaging smoke checks,
	and monitor container readiness smoke validation.
- Wired new quality gates into CI before the core unit test stage.
- Added Phase 6 release automation workflows:
	tag-triggered build/publish, release-drafter changelog automation,
	and signed-tag verification gates.
- Added artifact integrity/provenance checks in release flow:
	SHA256 verification and GitHub build provenance attestations.
