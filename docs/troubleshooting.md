# Troubleshooting

Use this guide for the most common DB Monitor setup and runtime issues.

## App Does Not Become Ready

Symptoms:

- `/readyz` returns `503`
- the `monitor-server` container stays unhealthy

Checks:

```bash
curl http://localhost:8000/readyz
curl http://localhost:8000/health
make monitor-logs-app
```

Common causes:

- `POSTGRES_URL` or `POSTGRES_URL_FILE` is missing or invalid
- `JWT_SECRET` is unset or still using an invalid production value
- migrations have not been applied before a production-style startup
- Kafka is reachable but the consumer has not committed successfully yet

## Bootstrap Fails

Common responses:

- `403 Bootstrap endpoint is disabled in this environment.`
- `400 Cannot bootstrap. System already has active API keys.`

Resolution:

- enable `ALLOW_BOOTSTRAP=true` only for first-run local initialization
- once keys exist, create or rotate admin keys through existing credentials

## Auth Requests Fail

Checks:

1. Confirm the API key format is `id.secret`.
2. Exchange it first at `POST /auth/token`.
3. Re-check expiration or revocation state if a previously working key stops
   working.

Useful commands:

```bash
curl -X POST \
  -H "X-API-Key: <id.secret>" \
  http://localhost:8000/auth/token
```

## No Events Or Tables Appear

Checks:

1. Confirm the source connectors are registered.
2. Verify the consumer is connected and committing.
3. Confirm the app is subscribed to the expected topics.

Useful commands:

```bash
docker compose logs connect
docker compose logs connector-registrar
curl http://localhost:8000/readyz
```

Notes:

- table discovery is event-driven, so `/tables` stays empty until matching
  events have actually been consumed
- local app runs use `localhost:9093`, while the containerized app uses
  `kafka:9092`

## DLQ Backlog Grows

Checks:

1. Inspect `/admin/dlq`.
2. Read the `error_message` for repeated failures.
3. Replay only after the failure cause is understood.

See `runbooks/recovery-and-validation.md` for the replay sequence.

## Migrations Or Schema Validation Fail

Checks:

```bash
cd app
PYTHONPATH=src:app uv run python -m db_monitor.cli migrate validate
PYTHONPATH=src:app uv run python -m db_monitor.cli migrate apply
```

Notes:

- production-style runs should use `DB_SCHEMA_MODE=validate`
- development runs can use `DB_SCHEMA_MODE=apply`

## Setup Script Or Local Sync Fails

Checks:

1. Confirm `uv` is installed and on `PATH`.
2. Re-run `./scripts/setup.sh` from the repo root.
3. Confirm `.env` and `secrets/` files were created.

If dependency sync fails, retry the app and TUI sync commands separately:

```bash
uv sync --group dev
cd tui && uv sync
```