# Configuration

DB Monitor reads configuration from environment variables and, for secrets,
also supports `*_FILE` variants that point at files mounted into the runtime.

## Common Local Development Example

```env
APP_ENV=development
DB_SCHEMA_MODE=apply
ALLOW_BOOTSTRAP=true
KAFKA_BROKER=localhost:9093
POSTGRES_URL=postgresql+asyncpg://postgres:postgres@localhost:5437/postgres
JWT_SECRET=dev-insecure-secret-change-me-please-rotate
```

## Production-Like Secret File Example

```env
APP_ENV=production
DB_SCHEMA_MODE=validate
ALLOW_BOOTSTRAP=false
POSTGRES_URL_FILE=/run/secrets/monitor_postgres_url
JWT_SECRET_FILE=/run/secrets/monitor_jwt_secret
JWT_SECRET_NEXT_FILE=/run/secrets/monitor_jwt_secret_next
KAFKA_BROKER=kafka:9092
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_SSL_CAFILE=/run/secrets/ca.pem
KAFKA_SSL_PASSWORD_FILE=/run/secrets/kafka_ssl_password
```

## Core Runtime Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `APP_ENV` | Environment mode used for safety defaults | `development` |
| `DB_SCHEMA_MODE` | Schema handling policy: `apply`, `validate`, or `skip` | `apply` in dev, `validate` in prod |
| `POSTGRES_URL` | Async SQLAlchemy connection string | local monitor DB |
| `KAFKA_BROKER` | Kafka bootstrap server | `kafka:9092` |
| `KAFKA_TOPICS` | Explicit topic list when not using manifest-derived topics | derived from `KAFKA_TOPIC` |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `fastapi-consumer-group` |

## Kafka TLS Settings

| Variable | Purpose |
| --- | --- |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`, `SSL`, or `SASL_SSL` |
| `KAFKA_SSL_CAFILE` | CA certificate path |
| `KAFKA_SSL_CERTFILE` | Client certificate path |
| `KAFKA_SSL_KEYFILE` | Client key path |
| `KAFKA_SSL_PASSWORD` / `KAFKA_SSL_PASSWORD_FILE` | Private key password |

## Database Pool Tuning

| Variable | Purpose | Default |
| --- | --- | --- |
| `DB_POOL_SIZE` | Base pool size | `10` |
| `DB_MAX_OVERFLOW` | Extra burst capacity | `20` |
| `DB_POOL_TIMEOUT_SECONDS` | Wait time for pooled connection checkout | `30` |
| `DB_POOL_RECYCLE_SECONDS` | Recycle interval for pooled connections | `1800` |
| `DB_POOL_PRE_PING` | Validate pooled connections before use | `true` |

## Auth And Session Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `ALLOW_BOOTSTRAP` | Allows one-time admin bootstrap | `true` in dev, `false` in prod |
| `API_KEY_DEFAULT_TTL_DAYS` | Default API-key lifetime | `90` |
| `JWT_SECRET` / `JWT_SECRET_FILE` | Active signing secret | dev placeholder |
| `JWT_SECRET_NEXT` / `JWT_SECRET_NEXT_FILE` | Rotation overlap secret | unset |
| `JWT_ALGORITHM` | JWT algorithm | `HS256` |
| `ACCESS_TOKEN_TTL_MINUTES` | Bearer token lifetime | `15` |
| `WS_SESSION_TOKEN_TTL_SECONDS` | WebSocket session-token lifetime | `60` |

## Consumer And Recovery Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `BATCH_ENABLED` | Enables batch processing | `true` |
| `BATCH_SIZE` | Batch size for consumer writes | `100` |
| `DLQ_ENABLED` | Persists failures to the DLQ table | `true` |
| `READINESS_MAX_COMMIT_AGE_SECONDS` | Readiness threshold for stale commits | `300` |
| `READINESS_MAX_CONSUMER_LAG` | Readiness threshold for lag backlog | `1000` |
| `READINESS_MAX_DLQ_MESSAGES` | Readiness threshold for pending DLQ rows | `0` |

## WebSocket Backplane Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `WS_BACKPLANE_ENABLED` | Enable Postgres-backed cross-replica fanout | `true` |
| `WS_BACKPLANE_CHANNEL` | Postgres notification channel name | `db_monitor_ws_events` |

## Audit And Shutdown Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `AUDIT_LOG_QUEUE_MAXSIZE` | In-memory audit queue limit | `10000` |
| `AUDIT_LOG_BATCH_SIZE` | Audit flush batch size | `100` |
| `AUDIT_LOG_FLUSH_INTERVAL_SECONDS` | Audit flush cadence | `1.0` |
| `APP_SHUTDOWN_TIMEOUT_SECONDS` | Graceful shutdown timeout | `45` |

## Notes

- In production, the app rejects the default Postgres URL and insecure default
  JWT secret.
- `POSTGRES_URL` must use the `postgresql+asyncpg://` scheme.
- The compose stack and local app entrypoint can both derive Kafka topics from
  `connectors/sources.json`, so you often do not need to set `KAFKA_TOPICS`
  manually.