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

```bash
# Or use the bundled sandbox for local evaluation:
make monitor-up-sandbox
```

See [Quick Start (with example stack)](#quick-start-using-the-local-example-stack) for the full sandbox flow.

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
| **Deployment** | Docker Compose sandbox, production Helm deployment, signed release tags, PyPI publishing |

---

## Architecture

```
Pluggable Source DBs (your apps)
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
databases and tables to monitor. Provide your own manifest at
`connectors/sources.json` (or configure `CONNECTOR_SOURCES_FILE`).

### Core vs. Example Stack

| Layer | What | Managed by |
|-------|------|-----------|
| **Core Platform** | `monitor-server`, `postgres-monitor`, broker, Debezium Connect, Prometheus/Grafana | Your deployment |
| **Example Stack** (optional) | 3 simulated source DBs (`orderdb`, `catalogdb`, `shippingdb`), test data generators, connector registrar | Repository Makefile |

The example stack is for local evaluation only. Production deployments use only
the core platform with your own infrastructure.

---

## Documentation

| Document | Description |
|----------|-------------|
| [API Reference](docs/api-reference.html) | Interactive API documentation (Redoc) |
| [API-Docs.md](api-docs.md) | Full endpoint reference with examples |
| [Architecture](docs/architecture.md) | System design, components, data flow |
| [Configuration](docs/configuration.md) | All environment variables, source manifest contract |
| [Deployment](docs/deployment.md) | Docker, Helm, Terraform, production hardening |
| [Migrations](docs/migrations.md) | Database migration workflow |
| [Troubleshooting](docs/troubleshooting.md) | Common issues and resolutions |
| [Contributing](CONTRIBUTING.md) | Development setup, code style, PR process |
| [Changelog](CHANGELOG.md) | Release history |
| [Security](SECURITY.md) | Vulnerability reporting and security practices |
| [ADR](docs/adr/) | Architecture Decision Records |
| [Example Sandbox Guide](docs/example-sandbox.md) | Full sandbox walkthrough |

### Architecture Decision Records

- [ADR-001](docs/adr/ADR-001-dual-codebase-consolidation.md) — Dual codebase consolidation
- [ADR-002](docs/adr/ADR-002-rabbitmq-support.md) — RabbitMQ support alongside Kafka
- [ADR-003](docs/adr/ADR-003-quota-throttle-sleep.md) — Quota throttle loop design
- [ADR-004](docs/adr/ADR-004-websocket-token-ttl.md) — Short-lived WebSocket session tokens

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

See [Configuration Reference](docs/configuration.md) for all settings.

### Source Manifest

Declare your monitored databases and tables in a JSON manifest:

```json
{
  "connectors": [
    {
      "source_name": "inventory_db",
      "database_hostname": "postgres-inventory-prod",
      "tables": ["public.items", "public.categories"],
      "enabled": true
    }
  ]
}
```

The manifest schema is documented at [connectors/sources.schema.json](connectors/sources.schema.json).

---

## Quick Start (using the Local Example Stack)

Use the bundled Docker Compose sandbox for a fast local evaluation.

### 1. Launch the Stack

```bash
./scripts/setup.sh
ALLOW_BOOTSTRAP=true make monitor-up-sandbox
```

### 2. Verify Readiness

```bash
curl http://localhost:8000/readyz
curl http://localhost:8000/health
```

### 3. Bootstrap Admin Key

```bash
curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
```

This returns an API key (`id.secret`). Exchange it for a bearer token:

```bash
curl -X POST -H "X-API-Key: <id.secret>" http://localhost:8000/auth/token
```

### 4. Query Events

```bash
curl -H "Authorization: Bearer <token>" http://localhost:8000/events
```

For traffic generation, WebSocket checks, and reset workflows, see the
[Example Sandbox Guide](docs/example-sandbox.md).

### Service Endpoints (Sandbox)

| Service | URL |
|---------|-----|
| FastAPI app | `http://localhost:8000` |
| Kafka UI | `http://localhost:8080` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| Alertmanager | `http://localhost:9094` |

---

## Integrating with Your Own Infrastructure

1. **Deploy PostgreSQL** for the monitor database.
2. **Configure environment variables** (see [Configuration](docs/configuration.md)).
3. **Provide a Source Manifest** at `connectors/sources.json`.
4. **Apply migrations**: `db-monitor-migrate apply`
5. **Launch**: `db-monitor-serve` (or deploy via Helm)

For production hardening:
- Use `APP_ENV=production` and `DB_SCHEMA_MODE=validate`
- Provide secrets via mounted files (`*_FILE` env vars)
- Enable retention cleanup and ingestion quotas
- Deploy with the [Helm chart](deploy/helm/db-monitor/)
- Provision infrastructure with the [Terraform module](deploy/terraform/)

---

## Development

```bash
uv sync --group dev
make monitor-lint
make monitor-typecheck
make monitor-test-core
make monitor-tui
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

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

## Known Limitations

- When Debezium schema envelopes are disabled, column discovery falls back
  to inferring column names and coarse types from `before`/`after` payloads.
  Primary key and nullability metadata are best-effort in that mode.
- `/health` checks application lifecycle state, database connectivity, and
  consumer runtime state, but does not expose broker-admin lag or deep Kafka
  diagnostics.
- Multiple replicas share the same in-process schema cache (TTL-based
  invalidation). A dedicated distributed cache is not yet implemented.
- See `SPEC.md` for a detailed assessment of current-state capabilities
  and open gaps.

## License

MIT
