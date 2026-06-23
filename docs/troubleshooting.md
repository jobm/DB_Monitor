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
  (`production` env rejects dev placeholder)
- migrations have not been applied before a production-style startup
- Kafka is reachable but the consumer has not committed successfully yet
- the consumer circuit breaker is open (check consumer health)

Readiness probes check: database connectivity, consumer commit freshness,
consumer lag, and DLQ backlog. See `docs/runbooks/alerts-and-thresholds.md`
for threshold values.

## Bootstrap Fails

Common responses:

- `403 Bootstrap endpoint is disabled in this environment.`
- `400 Cannot bootstrap. System already has active API keys.`

Resolution:

- enable `ALLOW_BOOTSTRAP=true` only for first-run local initialization
- once keys exist, create or rotate admin keys through existing credentials
- to reset, truncate the `api_keys` table directly (dev only) or use the
  admin key rotation API

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

If you get `401` on a key that previously worked, check:

- whether the key TTL has expired (default 90 days)
- whether the key was revoked via the admin API
- whether `JWT_SECRET` was rotated without updating `JWT_SECRET_NEXT`

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
- if using `KAFKA_TOPICS` explicitly, verify the topic names match what
  Debezium is publishing to

## DLQ Backlog Grows

Checks:

1. Inspect `/admin/dlq`.
2. Read the `error_message` for repeated failures.
3. Replay only after the failure cause is understood.

Common DLQ causes:

- transient database connectivity errors (retried automatically up to 5 times)
- schema discovery failures for unexpected column types
- deserialization errors from malformed Debezium payloads
- circuit breaker open during sustained failures

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
- if `pg_try_advisory_lock` fails, another pod is already running migrations.
  The lock ID is `7273646`. Wait for the other process to finish and retry.
- migration files live in `migrations/sql/`. If a migration version is
  missing from the `_migrations` tracking table, re-run `apply`.

## Consumer Lag Or Commit Age Alerts

If alerts fire for `ConsumerLagBacklog` or `ConsumerCommitStalled`:

1. Check `/readyz` — if unhealthy, address readiness first.
2. Check `/admin/checkpoints` for per-partition offset progress.
3. Inspect database connection pool pressure (`DatabasePoolUtilizationHigh`).
4. If lag is isolated to one partition, check whether that partition has a
   stuck or slow producer.
5. If the circuit breaker is open, resolve the underlying error (DB connect,
   schema issue, broker connectivity) — the breaker resets automatically
   after success.

## Ingestion Throttled Or Dropped

If `INGESTION_QUOTA_ENABLED=true` and ingestion appears slow:

1. Check the quota mode: `throttle` (slows down) vs `drop` (silently skips).
2. Inspect `INGESTION_SOURCE_QUOTAS` and `INGESTION_TENANT_QUOTAS` config.
3. Adjust window size (`INGESTION_QUOTA_WINDOW_SECONDS`) or per-source limits.
4. In `drop` mode, over-limit events are acknowledged but not written — check
   metric counters for drops.

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

## Duplicate Events Observed

If you see duplicate entries in `/events`:

1. This should not happen — the `(kafka_topic, kafka_partition, kafka_offset,
   capture_time)` unique constraint and `ON CONFLICT DO NOTHING` prevent it.
2. Check the `events_duplicate_skipped_total` Prometheus counter to confirm
   deduplication is active.
3. If duplicates still appear, verify the unique constraint exists on the
   events table and that inserts use the `ON CONFLICT` path.
4. Run the migration validation to confirm schema is up to date.