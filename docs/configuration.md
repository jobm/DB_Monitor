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
| `MESSAGE_BROKER` | Active single-broker mode: `kafka` or `rabbitmq` | `kafka` |
| `KAFKA_BROKER` | Kafka bootstrap server | `kafka:9092` |
| `KAFKA_TOPICS` | Explicit topic list when not using manifest-derived topics | derived from `KAFKA_TOPIC` |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `fastapi-consumer-group` |
| `KAFKA_CLUSTERS` | JSON array of per-cluster config entries | unset |
| `BROKER_CLUSTERS` | JSON array of mixed Kafka and RabbitMQ cluster entries | unset |
| `RABBITMQ_URL` | RabbitMQ connection URL | `amqp://guest:guest@localhost/` |
| `RABBITMQ_QUEUES` | Queue list for single-broker RabbitMQ mode | derived from `RABBITMQ_QUEUE` |
| `RABBITMQ_PREFETCH_COUNT` | Prefetch window for RabbitMQ consumers | `100` |
| `RABBITMQ_DLQ_QUEUE` | RabbitMQ dead-letter queue name | `db-monitor-dlq` |

## Kafka TLS Settings

| Variable | Purpose |
| --- | --- |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`, `SSL`, or `SASL_SSL` |
| `KAFKA_SSL_CAFILE` | CA certificate path |
| `KAFKA_SSL_CERTFILE` | Client certificate path |
| `KAFKA_SSL_KEYFILE` | Client key path |
| `KAFKA_SSL_PASSWORD` / `KAFKA_SSL_PASSWORD_FILE` | Private key password |

## Multiple Kafka Clusters

When `KAFKA_CLUSTERS` is set, DB Monitor starts one consumer task per cluster.
Each cluster entry must include a unique `name` and `bootstrap_servers`.
`topics` and `consumer_group` are optional and fall back to the single-cluster
defaults. When you need stable partition placement for horizontally scaled
replicas, set `topic_partitions` to pin one cluster entry to specific
partitions instead of using normal consumer-group subscription.

Example:

```env
KAFKA_CLUSTERS=[
  {"name":"primary","bootstrap_servers":"kafka-a:9092","topics":["orderdb.public.orders"]},
  {"name":"secondary","bootstrap_servers":["kafka-b:9092"],"topics":["shippingdb.public.shipments"],"consumer_group":"shipping-group"}
]
```

Partition-aware placement example:

```env
KAFKA_CLUSTERS=[
  {"name":"orders-a","bootstrap_servers":"kafka-a:9092","topic_partitions":{"orderdb.public.orders":[0,1]},"consumer_group":"orders-placement"},
  {"name":"orders-b","bootstrap_servers":"kafka-a:9092","topic_partitions":{"orderdb.public.orders":[2,3]},"consumer_group":"orders-placement"}
]
```

In this mode DB Monitor uses explicit Kafka partition assignment for the listed
topic partitions instead of consumer-group subscription, which lets operators
place work deterministically across replicas.

Current limitation: all configured clusters share the same security protocol
and TLS settings from the `KAFKA_SECURITY_PROTOCOL` and `KAFKA_SSL_*` values.

## Multiple Broker Clusters

Use `BROKER_CLUSTERS` when you need a single deployment to consume from a mix
of Kafka topics and RabbitMQ queues. Each cluster entry must include a unique
`name` and a `broker_kind` of `kafka` or `rabbitmq`.

- Kafka entries accept `bootstrap_servers`, `topics`, optional
  `consumer_group`, and optional `topic_partitions`.
- RabbitMQ entries accept `connection_url`, `queue_names`, optional
  `consumer_group`, optional `prefetch_count`, and optional
  `dlq_destination`.

Example mixed-broker configuration:

```env
BROKER_CLUSTERS=[
  {"name":"orders-kafka","broker_kind":"kafka","bootstrap_servers":"kafka-a:9092","topics":["orderdb.public.orders"]},
  {"name":"shipping-rabbit","broker_kind":"rabbitmq","connection_url":"amqp://guest:guest@rabbitmq/","queue_names":["shipping.events"],"prefetch_count":50,"dlq_destination":"shipping.events.dlq"}
]
```

When `BROKER_CLUSTERS` is unset, DB Monitor keeps the existing Kafka behavior
through `KAFKA_CLUSTERS`, or uses the single-cluster RabbitMQ defaults when
`MESSAGE_BROKER=rabbitmq`.

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
| `CUSTOM_EVENT_PROCESSORS` | Comma-separated processor import paths | unset |
| `DLQ_ENABLED` | Persists failures to the DLQ table | `true` |
| `READINESS_MAX_COMMIT_AGE_SECONDS` | Readiness threshold for stale commits | `300` |
| `READINESS_MAX_CONSUMER_LAG` | Readiness threshold for lag backlog | `1000` |
| `READINESS_MAX_DLQ_MESSAGES` | Readiness threshold for pending DLQ rows | `0` |

## Custom Event Processors

Use `CUSTOM_EVENT_PROCESSORS` when you need project-specific event mutations
that should run inside the normal ingestion pipeline before masking and
persistence.

Example:

```env
CUSTOM_EVENT_PROCESSORS=my_project.processors:add_summary
```

Each processor should be importable as either `module:function` or
`module.function` and accept a single `KafkaEvent`. It may mutate the event in
place or return a replacement `KafkaEvent`.

## WebSocket Backplane Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `WS_BACKPLANE_ENABLED` | Enable Postgres-backed cross-replica fanout | `true` |
| `WS_BACKPLANE_CHANNEL` | Postgres notification channel name | `db_monitor_ws_events` |

## Webhook Notifications

| Variable | Purpose | Default |
| --- | --- | --- |
| `WEBHOOK_URLS` | Comma-separated outbound webhook targets | unset |
| `WEBHOOK_TIMEOUT_SECONDS` | Per-request webhook timeout | `5.0` |
| `WEBHOOK_SHARED_SECRET` / `WEBHOOK_SHARED_SECRET_FILE` | Optional HMAC signing secret | unset |

Example:

```env
WEBHOOK_URLS=https://ops.example/hooks/db-monitor,https://backup.example/hooks/cdc
WEBHOOK_TIMEOUT_SECONDS=5.0
WEBHOOK_SHARED_SECRET=replace-me
```

When configured, DB Monitor sends a `POST` request for each persisted event
with the same payload shape used by the websocket feed. If a shared secret is
set, deliveries include `X-DB-Monitor-Signature` using `sha256=<hex>` HMAC.

## Distributed Tracing Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `OTEL_TRACING_ENABLED` | Enable OpenTelemetry tracing | `false` |
| `OTEL_SERVICE_NAME` | Service name attached to exported spans | `db-monitor` |
| `OTEL_EXPORTER` | Trace exporter mode: `otlp` or `console` | `otlp` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint when using `otlp` | unset |
| `OTEL_EXPORTER_OTLP_HEADERS` | Comma-separated OTLP headers as `k=v` pairs | unset |

Local console example:

```env
OTEL_TRACING_ENABLED=true
OTEL_EXPORTER=console
OTEL_SERVICE_NAME=db-monitor-dev
```

Collector example:

```env
OTEL_TRACING_ENABLED=true
OTEL_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20token
```

Current tracing coverage includes FastAPI requests, SQLAlchemy queries, and
manual spans around Kafka consumption, DLQ replay, and websocket backplane
fanout.

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