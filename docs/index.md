# DB Monitor

**Framework-first CDC monitoring platform and audit-log service for PostgreSQL.**

DB Monitor ingests Change Data Capture (CDC) event streams from Kafka or
RabbitMQ, normalizes and stores them in PostgreSQL, and exposes structured
events, auto-discovered schema metadata, column-level change history,
point-in-time state lookups, and realtime notifications via API, WebSocket,
and Prometheus metrics.

It is designed as a **pluggable framework** — bring your own databases,
message brokers, and source manifest. The bundled example stack is optional.

---

## Quick Start (30 seconds)

```bash
# 1. Install
pip install db-monitor

# 2. Set environment variables
export POSTGRES_URL="postgresql+asyncpg://user:pass@localhost:5432/dbmonitor"
export KAFKA_BROKER="localhost:9092"
export JWT_SECRET="your-secret-here"

# 3. Run migrations
db-monitor-migrate apply

# 4. Start the server
db-monitor-serve
```

Or use the [bundled sandbox](example-sandbox.md) for local evaluation.

---

## Features

| Area | Capabilities |
|------|-------------|
| **Ingestion** | Multi-broker (Kafka + RabbitMQ), multi-cluster, batch processing, bulk writes, idempotent dedup, custom event processors |
| **Quotas** | Per-source and per-tenant rate limits, throttle or drop modes, windowed enforcement |
| **Schema** | Auto-discovery from Debezium envelopes, column-level change history, point-in-time lookup |
| **Storage** | PostgreSQL with time-based range partitioning, partition-aware retention (DROP not DELETE), JSONL archival |
| **API** | REST (FastAPI), cursor/keyset pagination, structured event querying, admin endpoints for checkpoints & DLQ replay |
| **Realtime** | WebSocket broadcast, cross-replica fanout via Postgres backplane |
| **Auth** | API-key lifecycle (bootstrap, rotate, revoke), JWT bearer tokens, short-lived WS session tokens, RBAC (viewer/admin) |
| **Observability** | Prometheus metrics, structured JSON logging, OpenTelemetry tracing (FastAPI, SQLAlchemy), Grafana dashboards, Alertmanager |
| **Resilience** | Circuit breakers, DLQ with replay, concurrent migration locking, graceful shutdown, HPA-ready |
| **Operations** | Helm chart (Kubernetes), Terraform module, Textual TUI admin console, Python & JavaScript SDKs |

---

## Architecture

```
Source Databases (your apps)
  → Debezium Connect (or CDC connectors)
  → Message Broker (Kafka topics or RabbitMQ queues)
  → FastAPI Core Consumer (normalizes, filters, discovers schemas, records state)
  → Monitor PostgreSQL (events, schema catalog, column history, audit log)
     → REST API / SDKs
     → WebSocket stream
     → Prometheus metrics
     → TUI dashboard / Admin console
```

The system is driven by a **Source Manifest** — a JSON document declaring which
databases and tables to monitor. See the [Configuration reference](configuration.md)
for details.

### Core vs. Example Stack

| Layer | What | Managed by |
|-------|------|-----------|
| **Core Platform** | `monitor-server`, `postgres-monitor`, broker, Debezium Connect, Prometheus/Grafana | Your deployment |
| **Example Stack** (optional) | 3 simulated source DBs, test data generators, connector registrar | Repository Makefile |

The example stack is for local evaluation only. Production deployments use only
the core platform with your own infrastructure.

---

## Installation

### From PyPI

```bash
pip install db-monitor
```

### From source

```bash
git clone https://github.com/jobm/DB_Monitor.git
cd DB_Monitor
uv sync --group dev
```

### Container image

```bash
docker pull ghcr.io/jobm/db-monitor:latest
```

---

## Configuration

DB Monitor reads configuration from environment variables and supports `*_FILE`
variants for secrets mounted from files.

**Minimal production configuration:**

```env
APP_ENV=production
DB_SCHEMA_MODE=validate
POSTGRES_URL=postgresql+asyncpg://user:pass@host:5432/dbmonitor
KAFKA_BROKER=kafka:9092
JWT_SECRET=<random-64-char-hex>
```

See the [Configuration Reference](configuration.md) for all settings and the
[Source Manifest](configuration.md#source-manifest-configuration) guide.

---

## Documentation

| Guide | Description |
|-------|-------------|
| [Architecture](architecture.md) | System design, components, data flow |
| [Configuration](configuration.md) | All environment variables, source manifest contract |
| [Deployment](deployment.md) | Docker, Helm, Terraform, production hardening |
| [Migrations](migrations.md) | Database migration workflow |
| [Troubleshooting](troubleshooting.md) | Common issues and resolutions |
| [API Reference](api-reference.html) | Interactive API documentation |
| [Example Sandbox](example-sandbox.md) | Full sandbox walkthrough |
| [Module Inventory](module-inventory.md) | Codebase module map |

### Project Docs

- [Contributing](CONTRIBUTING.md) — Development setup, code style, PR process
- [Changelog](CHANGELOG.md) — Release history
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security](SECURITY.md) — Vulnerability reporting

---

## Data Model

- **`events`** — Structured CDC events with parsed metadata and raw payloads
- **`monitored_tables`** — Discovered tables keyed by service/database/table
- **`monitored_columns`** — Discovered per-table columns
- **`column_changes`** — Per-column history with old/new values
- **`api_keys`** — Hashed API keys and roles
- **`api_audit_logs`** — Buffered request audit records
- **`consumer_checkpoints`** — Persisted Kafka offsets
- **`dead_letter_events`** — Failed events available for replay

---

## License

MIT
