# Changelog

All notable changes to DB Monitor are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Documentation overhaul (Workstream F):**
  - Architecture Decision Records in `docs/adr/`: consolidation, RabbitMQ
    support, quota throttle design, WebSocket token TTL
  - `CONTRIBUTING.md`: development setup, code style, testing guidelines, PR
    process
  - `CHANGELOG.md`: this file
  - `SECURITY.md`: vulnerability reporting policy and security practices
  - Static API reference page (`docs/api-reference.html`)
  - README rewritten with framework-first positioning
  - Module-level docstring added to `ws_manager.py`
- **Formalized database migrations (Workstream D):**
  - Explicit DDL replaces `Base.metadata.create_all()` in `schema.py`
  - Schema drift detection in `db-monitor-migrate validate`
  - Migration testing: static DDL analysis, drift detection fakes, and live DB
    integration test
  - `docs/migrations.md`: full migration workflow documentation
- **Test infrastructure overhaul (Workstream E):**
  - Timing-dependent `asyncio.sleep(0.5)` replaced with event coordination in
    lifecycle tests
  - Autouse fixture to clear module-level consumer state between tests
  - Hypothesis property-based tests for `event_parser.py` (9 invariants) and
    `ingestion_quota.py` (4 invariants)
  - Fixed pre-existing bug in consumer test (asserted on wrong variable)

### Changed

- README restructured: one-line → quick start → features → architecture →
  documentation → installation → configuration → development → license
- BREAKING: `Base.metadata.create_all()` no longer called at startup — must
  use `db-monitor-migrate apply` instead

### Fixed

- Test order dependency in `test_refresh_dead_letter_backlog_metric` —
  consumer runtime variable assertion matched wrong dict

## [0.0.1] - 2026-04-29

### Added

- Initial project structure and packaging metadata
- FastAPI application with core routes: events, changes, tables, auth
- Kafka multi-topic consumption with manual offset commits
- Batch processing and DLQ forwarding
- Schema discovery and column-level change history
- API-key authentication with `viewer` and `admin` roles
- Buffered API audit logging
- WebSocket event broadcast
- Prometheus metrics and Grafana dashboards
- Structured JSON request logging
- Component health checks (`/health`, `/readyz`, `/livez`)
- RabbitMQ support as alternative broker
- Multi-cluster Kafka and mixed-broker consumption
- Custom event processor hooks
- Webhook notifications with HMAC signing and circuit breaker
- OpenTelemetry tracing (FastAPI, SQLAlchemy, manual spans)
- Ingestion quota controls (throttle and drop modes)
- Retention and archival cleanup with partition-aware DROP
- Cursor/keyset pagination for events and changes
- Cursor-optimized composite indexes
- Idempotent event ingestion (`ON CONFLICT DO NOTHING`)
- Concurrent migration locking via PostgreSQL advisory locks
- Horizontal scale validation and partition-aware placement
- Helm chart with HPA, ServiceMonitor, NetworkPolicy, PDB
- Terraform module for companion infrastructure
- Textual TUI dashboard with admin console
- Python and JavaScript SDKs
- Property-based tests with Hypothesis
- Bulk-write ingestion path for batch performance
- Time-based range partitioning for events and column_changes
- Customer cell provisioning workflow
- Per-customer JWT secret lifecycle and bootstrap
- Deployable namespace guardrails (ResourceQuota, NetworkPolicy)
- Per-customer observability labels and alert routing
- Customer scope enforcement on mutating admin endpoints
- Package standardization: src/ layout, CLI entrypoints, API stability policy
- Automated release workflow with signed tags and PyPI publish
- Quality gates: Ruff lint, mypy type checks, packaging smoke tests

[Unreleased]: https://github.com/jobm/DB_Monitor/compare/main...HEAD
[0.0.1]: https://github.com/jobm/DB_Monitor/releases/tag/v0.0.1
