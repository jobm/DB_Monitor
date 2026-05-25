# Deployment

This guide covers the supported local and production-like deployment workflows
for DB Monitor.

For the pre-v1 customer isolation decision and rollout model, see
`docs/pre-v1-tenant-isolation-strategy.md`.

## 1. Local Example Sandbox (Docker Compose)

The easiest way to evaluate DB Monitor locally is to run the bundled Docker Compose sandbox stack, which includes simulated source databases, Debezium configurations, and test data generators.

### Prerequisites

- Docker with Compose support
- `uv` for local helper commands

### Launching the Sandbox

```bash
./scripts/setup.sh
ALLOW_BOOTSTRAP=true make monitor-up-sandbox
```

Use `examples/sandbox/` for example-only generators and validation scripts.
The older top-level demo scripts are compatibility shims for existing tooling.

### Health Validation

```bash
curl http://localhost:8000/readyz
curl http://localhost:8000/health
```

### Bootstrap the First Admin Key

```bash
curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
```

### Stopping the Sandbox

```bash
make monitor-down-sandbox
```

## 2. Local App Development Against Infrastructure

If you are developing or debugging DB Monitor locally, you can start the platform infrastructure in Docker and run the core Python application or consumer service on your local machine.

1. Start the supporting infrastructure core and optional sandbox sources.

```bash
# To start the baseline platform core AND the evaluation sandbox databases:
docker compose --profile sandbox up -d

# To start ONLY the core platform infrastructure (Kafka, Monitor DB, Grafana, etc.):
docker compose up -d
```

2. Run schema migrations locally on the monitor DB.

```bash
make monitor-migrate
```

3. Start the API or consumer service from the app directory.

```bash
cd app
PYTHONPATH=../src:. uv run python -m db_monitor.start_monitor
```

Expected local `.env` values are documented in [docs/configuration.md](docs/configuration.md).

## 3. Production-Like Expectations

The bundled `monitor-server` service already applies several production
constraints:

- non-root container execution
- read-only root filesystem
- tmpfs for writable scratch space
- capability drop and `no-new-privileges`
- explicit `APP_ENV=production`
- `DB_SCHEMA_MODE=validate`
- secrets loaded from mounted files

Before booting the app in production or production-like environments:

1. Apply migrations explicitly.

```bash
cd app
PYTHONPATH=../src:. uv run python -m db_monitor.cli migrate apply
```

2. Confirm bootstrap is disabled.

```text
ALLOW_BOOTSTRAP=false
```

3. Provide secrets through mounted files where possible.

Use the production `*_FILE` examples in `docs/configuration.md` for the
authoritative variable list and naming.

4. Validate readiness after startup.

```bash
curl http://localhost:8000/readyz
```

5. Enable and validate retention cleanup policy.

Use the retention settings in `docs/configuration.md` to define cleanup
windows and optional archival. For production-like deployments:

- keep `RETENTION_CLEANUP_ENABLED=true`
- set explicit per-table retention windows
- if `RETENTION_ARCHIVE_BEFORE_DELETE=true`, mount
	`RETENTION_ARCHIVE_DIR` to durable storage

6. Enable ingestion quota controls for burst protection.

Use ingestion quota settings from `docs/configuration.md` when you need
per-source or per-tenant protection during traffic spikes:

- keep `INGESTION_QUOTA_ENABLED=true` for bounded ingestion
- use `INGESTION_QUOTA_MODE=throttle` to absorb spikes gradually
- use `INGESTION_QUOTA_MODE=drop` only for strict fail-fast pressure control

## 4. Post-Deploy Validation

Recommended order:

1. `curl http://localhost:8000/readyz`
2. `make monitor-test-integration`
3. `make monitor-test-smoke`
4. inspect `/admin/checkpoints` and `/admin/dlq` when validating ingestion

The detailed recovery flow is in `runbooks/recovery-and-validation.md`.