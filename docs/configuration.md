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
| `CONNECTOR_SOURCES_FILE` | Path to the public source manifest file | `/connectors/sources.json` in docker, else `connectors/sources.json` |
| `MESSAGE_BROKER` | Active single-broker mode: `kafka` or `rabbitmq` | `kafka` |
| `KAFKA_BROKER` | Kafka bootstrap server | `kafka:9092` |
| `KAFKA_TOPIC` | Singular fallback Kafka topic if no topic derivation exists | `orderdb.public.orders` |
| `KAFKA_TOPICS` | Explicit topic list. Overrides manifest-derived tracking if provided | derived from manifest |
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

## Ingestion Quota And Throttling Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `INGESTION_QUOTA_ENABLED` | Enable source/tenant ingestion quota checks | `false` |
| `INGESTION_QUOTA_WINDOW_SECONDS` | Quota enforcement window size in seconds | `60` |
| `INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW` | Default per-source event limit per window (`0` disables) | `0` |
| `INGESTION_SOURCE_QUOTAS` | JSON source overrides for per-window limits | unset |
| `INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW` | Default per-tenant event limit per window (`0` disables) | `0` |
| `INGESTION_TENANT_QUOTAS` | JSON tenant overrides for per-window limits | unset |
| `INGESTION_QUOTA_MODE` | Over-limit behavior: `throttle` or `drop` | `throttle` |
| `INGESTION_QUOTA_MAX_THROTTLE_SECONDS` | Max sleep per throttle cycle before retry | `5.0` |
| `INGESTION_BULK_WRITE_ENABLED` | Enable bulk event insert path for batch ingestion | `true` |

Example:

```env
INGESTION_QUOTA_ENABLED=true
INGESTION_QUOTA_WINDOW_SECONDS=60
INGESTION_SOURCE_DEFAULT_EVENTS_PER_WINDOW=2000
INGESTION_SOURCE_QUOTAS={"orderdb":1500,"shippingdb":900}
INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW=500
INGESTION_TENANT_QUOTAS={"tenant-a":350,"tenant-b":150}
INGESTION_QUOTA_MODE=throttle
INGESTION_QUOTA_MAX_THROTTLE_SECONDS=1.0
```

Notes:

- Tenant quotas apply only when an event carries `tenant_id`, `tenant`, or
  `tenantId` in the top-level payload (or nested `payload`).
- In `drop` mode, over-limit events are acknowledged and skipped without DB
  writes to protect downstream storage.

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

## Retention And Archival Settings

| Variable | Purpose | Default |
| --- | --- | --- |
| `RETENTION_CLEANUP_ENABLED` | Enable background retention cleanup task | `false` |
| `RETENTION_CLEANUP_INTERVAL_SECONDS` | Seconds between cleanup cycles | `3600` |
| `RETENTION_CLEANUP_BATCH_SIZE` | Rows deleted per cleanup batch | `1000` |
| `RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE` | Max batches per table in one cycle | `10` |
| `RETENTION_ARCHIVE_BEFORE_DELETE` | Archive rows to JSONL before deleting | `false` |
| `RETENTION_ARCHIVE_DIR` | Directory for JSONL archive files | `retention-archive` |
| `EVENT_RETENTION_DAYS` | Retention window for `events` rows | `30` |
| `COLUMN_CHANGES_RETENTION_DAYS` | Retention window for `column_changes` rows | `30` |
| `DEAD_LETTER_RETENTION_DAYS` | Retention window for `dead_letter_events` rows | `30` |
| `API_AUDIT_LOG_RETENTION_DAYS` | Retention window for `api_audit_logs` rows | `90` |

Example production-like configuration:

```env
RETENTION_CLEANUP_ENABLED=true
RETENTION_CLEANUP_INTERVAL_SECONDS=1800
RETENTION_CLEANUP_BATCH_SIZE=1000
RETENTION_CLEANUP_MAX_BATCHES_PER_TABLE=20
RETENTION_ARCHIVE_BEFORE_DELETE=true
RETENTION_ARCHIVE_DIR=/var/lib/db-monitor/retention-archive
EVENT_RETENTION_DAYS=30
COLUMN_CHANGES_RETENTION_DAYS=60
DEAD_LETTER_RETENTION_DAYS=30
API_AUDIT_LOG_RETENTION_DAYS=90
```

Notes:

- Set any `*_RETENTION_DAYS` value to `0` to disable cleanup for that table.
- Archive output is written as JSONL files grouped by table and UTC date.
- Mount `RETENTION_ARCHIVE_DIR` to durable storage when archival is enabled.

## Notes

- In production, the app rejects the default Postgres URL and insecure default
  JWT secret.
- `POSTGRES_URL` must use the `postgresql+asyncpg://` scheme.
- The compose stack and local app entrypoint can both derive Kafka topics from
  `connectors/sources.json`, so you often do not need to set `KAFKA_TOPICS`
  manually.

## Source Manifest Contract (`CONNECTOR_SOURCES_FILE`)

The monitoring core of DB Monitor is driven by a declarative configuration contract called the **Source Manifest**. This is a JSON document located at `connectors/sources.json` by default (or custom configured via `CONNECTOR_SOURCES_FILE`).

To formally declare, dry-run, or publish your custom databases and tables, provide a manifest that conforms to the JSON Schema at [connectors/sources.schema.json](../connectors/sources.schema.json).

### Schema Specification

The manifest consists of a root `"connectors"` array containing one or more source databases:

| Field | Type | Description | Required | Default / Fallback |
| --- | --- | --- | --- | --- |
| `source_name` | String | A unique system identifier. Also used as Debezium topic prefix. | **Yes** | — |
| `database_hostname` | String | Net hostname or IP address of the PostgreSQL source database. | **Yes** | — |
| `tables` | Array | Non-empty list of schemas and tables to monitor (e.g. `"public.orders"`). | **Yes** | — |
| `enabled` | Boolean | Activates or disables capturing of this database. | No | `true` |
| `connector_name` | String | Custom Debezium connector registration endpoint name. | No | `"{source_name}-connector"` |
| `slot_name` | String | Custom logical replication slot name in Postgres. | No | `"{source_name}_slot"` |
| `history_topic` | String | Custom name for internal schema changes tracker topic. | No | `"dbhistory.{source_name}"` |
| `config_overrides` | Object | Arbitrary extra key/values properties passed directly to the Debezium engine. | No | `{}` |

### Minimal Manifest Example

```json
{
  "connectors": [
    {
      "source_name": "inventory_db",
      "database_hostname": "postgres-inventory-prod",
      "tables": [
        "public.items",
        "public.categories"
      ],
      "enabled": true
    }
  ]
}
```
