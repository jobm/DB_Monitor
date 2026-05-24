# Deployment

This guide covers the supported local and production-like deployment workflows
for DB Monitor.

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

```text
POSTGRES_URL_FILE=/run/secrets/monitor_postgres_url
JWT_SECRET_FILE=/run/secrets/monitor_jwt_secret
JWT_SECRET_NEXT_FILE=/run/secrets/monitor_jwt_secret_next
```

4. Validate readiness after startup.

```bash
curl http://localhost:8000/readyz
```

## 4. Post-Deploy Validation

Recommended order:

1. `curl http://localhost:8000/readyz`
2. `make monitor-test-integration`
3. `make monitor-test-smoke`
4. inspect `/admin/checkpoints` and `/admin/dlq` when validating ingestion

The detailed recovery flow is in `runbooks/recovery-and-validation.md`.