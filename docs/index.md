# DB Monitor Documentation

**Framework-first CDC monitoring platform and audit-log service for PostgreSQL.**

DB Monitor ingests Change Data Capture (CDC) events from Kafka or RabbitMQ,
normalizes and stores them in PostgreSQL, and exposes structured events,
auto-discovered schema metadata, column-level change history, point-in-time
state lookups, and realtime notifications.

---

## Start Here

| Guide | Description |
|-------|-------------|
| [Architecture](architecture.md) | System design, components, and data flow |
| [Configuration](configuration.md) | Environment variables and source manifest contract |
| [Deployment](deployment.md) | Docker, Helm, Terraform, and production hardening |
| [Migrations](migrations.md) | Database migration workflow |
| [API Reference](api-reference.html) | Interactive API documentation (Redoc) |
| [API Docs](api-docs.md) | HTTP and WebSocket endpoint reference |
| [Troubleshooting](troubleshooting.md) | Common setup and runtime issues |

## Guides

| Guide | Description |
|-------|-------------|
| [Bring Your Own Database](guides/byo-database.md) | Onboarding a real source database |
| [Example Sandbox](example-sandbox.md) | Local evaluation stack walkthrough |
| [Pre-v1 Tenant Isolation](pre-v1-tenant-isolation-strategy.md) | Shared control plane and customer cells |

## Operations

| Runbook | Description |
|---------|-------------|
| [Alerts and Thresholds](runbooks/alerts-and-thresholds.md) | Alert meanings and first actions |
| [SLO Targets and Error Budgets](runbooks/slo-and-error-budgets.md) | Cohort objectives, budgets, and burn-rate alerts |
| [Recovery and Validation](runbooks/recovery-and-validation.md) | Checkpoint, DLQ replay, and post-deploy validation |
| [Ingestion Throughput](benchmarks/ingestion-throughput.md) | Benchmark methodology |
| [Benchmark Results](benchmarks/README.md) | Recorded throughput results |

## Architecture Decisions

- [ADR-001: Dual Codebase Consolidation](adr/ADR-001-dual-codebase-consolidation.md)
- [ADR-002: RabbitMQ Support](adr/ADR-002-rabbitmq-support.md)
- [ADR-003: Quota Throttle via Sleep Loop](adr/ADR-003-quota-throttle-sleep.md)
- [ADR-004: Short-Lived WebSocket Tokens](adr/ADR-004-websocket-token-ttl.md)
- [ADR-005: Redis WebSocket Backplane](adr/ADR-005-websocket-redis-backplane.md)
- [ADR-006: Multi-Tenancy Isolation](adr/ADR-006-multi-tenancy-isolation.md)
- [ADR-007: Deployment Topology](adr/ADR-007-deployment-topology.md)

## Project

- [API Stability](api-stability.md) — Public Python API contract
- [Contributing](CONTRIBUTING.md) — Development setup, code style, PR process
- [Changelog](CHANGELOG.md) — Release history
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security](SECURITY.md) — Vulnerability reporting
- [Backlog](BACKLOG.md) — The single source of truth for open work
