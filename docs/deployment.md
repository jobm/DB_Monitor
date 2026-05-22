# Deployment

This guide covers the supported local and production-like deployment workflows
for DB Monitor.

## 1. Docker Compose Stack

Use the bundled Compose stack for local integration testing and for validating
the full CDC pipeline.

### Prerequisites

- Docker with Compose support
- `uv` for local helper commands

### First-Time Setup

```bash
./scripts/setup.sh
ALLOW_BOOTSTRAP=true make monitor-up
```

### Health Validation

```bash
curl http://localhost:8000/readyz
curl http://localhost:8000/health
```

### Bootstrap The First Admin Key

```bash
curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
```

### Stop The Stack

```bash
make monitor-down
```

## 2. Run The API Locally Against Docker Infrastructure

This mode is useful when you want live infrastructure but local Python
debugging.

1. Start the infrastructure services.

```bash
docker compose up -d kafka zookeeper postgres-monitor postgres-order postgres-catalog postgres-shipping connect connector-registrar
```

2. Run tracked migrations locally.

```bash
make monitor-migrate
```

3. Start the API from the app directory.

```bash
cd app
uv run python start_monitor.py
```

Expected local `.env` values are documented in `configuration.md`.

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
uv run python migrate.py apply
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