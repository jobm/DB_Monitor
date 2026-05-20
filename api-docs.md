# DB Monitor API

This document describes the HTTP and WebSocket interfaces exposed by the monitor service.

## Authentication

Bootstrap and key-management endpoints still use long-lived API keys in the
`X-API-Key` header with the format:

```text
<key_id>.<raw_secret>
```

Roles:

- `viewer`: read-only access to tables, events, changes, and `/info`
- `admin`: viewer access plus API-key creation

Application clients should exchange a valid API key for a short-lived bearer
token via `POST /auth/token`, then use:

```text
Authorization: Bearer <access_token>
```

## Bootstrap and key management

### `POST /auth/bootstrap`

Creates the first admin key. This only works when there are no active API keys yet
and bootstrap is enabled for the environment.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `owner_name` | Yes | Human-readable owner label for the key |
| `ttl_days` | No | Expiration window in days, defaults to the server policy |

Example:

```bash
curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
```

### `POST /auth/keys`

Creates a new API key. Requires an admin key in `X-API-Key`.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `owner_name` | Yes | Human-readable owner label for the key |
| `role` | No | `viewer` or `admin`, defaults to `viewer` |
| `ttl_days` | No | Expiration window in days, defaults to the server policy |

### `POST /auth/keys/{key_id}/rotate`

Rotates an existing key and returns a replacement credential once. Requires an
admin key in `X-API-Key`.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `ttl_days` | No | Expiration window for the replacement key |

### `POST /auth/keys/{key_id}/revoke`

Revokes an existing key. Requires an admin key in `X-API-Key`.

## Recovery endpoints

### `GET /admin/checkpoints`

Returns the persisted Kafka checkpoint snapshot keyed by consumer group, topic,
and partition. Requires an admin credential.

Response shape:

```json
{
  "checkpoints": [
    {
      "consumer_group": "fastapi-consumer-group",
      "kafka_topic": "orderdb.public.orders",
      "kafka_partition": 0,
      "kafka_offset": 123,
      "last_event_time": "2026-05-21T11:00:00+00:00",
      "updated_at": "2026-05-21T11:00:01+00:00"
    }
  ],
  "count": 1
}
```

### `GET /admin/dlq`

Lists persisted DLQ records that can be replayed. Requires an admin
credential.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `limit` | No | `1-1000`, default `100` |
| `include_replayed` | No | Include already replayed records, default `false` |

### `POST /admin/dlq/{dlq_event_id}/replay`

Replays a persisted DLQ record through the normal ingestion path. If the event
was already stored, the response status is `duplicate`; otherwise it is
`replayed`. Requires an admin credential.

### `POST /auth/token`

Exchanges a valid API key or bearer token for a short-lived bearer token.

Response shape:

```json
{
  "access_token": "<jwt>",
  "token_type": "bearer",
  "expires_at": "2026-05-20T12:00:00+00:00",
  "owner_name": "admin",
  "role": "admin"
}
```

### `POST /auth/ws-token`

Exchanges a valid API key or bearer token for a short-lived WebSocket session
token.

Response shape:

```json
{
  "session_token": "<jwt>",
  "expires_at": "2026-05-20T12:00:30+00:00",
  "owner_name": "admin",
  "role": "admin"
}
```

## Public endpoints

### `GET /health`

Returns overall status plus component-level checks for:

- database connectivity
- consumer runtime state
- application lifecycle state

### `GET /livez`

Returns process-level liveness and whether shutdown is in progress.

### `GET /readyz`

Returns readiness state for database connectivity, lifecycle state, and
consumer operational signals including lag, DLQ growth, and last successful
commit age.

### `GET /metrics`

Prometheus metrics endpoint.

## Viewer endpoints

### `GET /info`

Returns application metadata and lifecycle state.

### `GET /tables`

Lists discovered monitored tables.

Response shape:

```json
{
  "tables": [
    {
      "id": 1,
      "service_name": "orderdb",
      "database_name": "postgres",
      "table_name": "orders",
      "topic_name": "orderdb.public.orders",
      "created_at": "2026-04-29T11:00:00+00:00"
    }
  ],
  "count": 1
}
```

### `GET /tables/{service_name}/{table_name}`

Returns a single discovered table and its columns.

### `GET /tables/{service_name}/{table_name}/columns`

Returns only the columns for a discovered table.

### `GET /events`

Returns stored events with pagination and optional filtering.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `limit` | No | `1-1000`, default `100` |
| `offset` | No | Pagination offset, default `0` |
| `service_name` | No | Filter by service/topic prefix such as `orderdb` |
| `event_type` | No | Filter by parsed event type |
| `start_time` | No | Inclusive ISO 8601 timestamp filter |
| `end_time` | No | Inclusive ISO 8601 timestamp filter |
| `search_term` | No | Case-insensitive substring match against `raw_payload` |

### `GET /events/stats`

Returns aggregate counts grouped by service, event type, and operation.

### `GET /changes`

Returns column change history for a table.

Supported query forms:

1. `table_name=orderdb.orders`
2. `service_name=orderdb&table_name=orders`

Additional query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `column_name` | No | Restrict results to a single column |
| `from_time` | No | Inclusive ISO 8601 lower bound |
| `to_time` | No | Inclusive ISO 8601 upper bound |
| `limit` | No | `1-1000`, default `100` |

Example:

```bash
curl \
  -H "X-API-Key: 1.example-secret" \
  "http://localhost:8000/changes?table_name=orderdb.orders&column_name=status&from_time=2026-04-01T00:00:00Z"
```

Response shape:

```json
{
  "service_name": "orderdb",
  "table_name": "orders",
  "changes": [
    {
      "id": 42,
      "column_name": "status",
      "operation": "UPDATE",
      "old_value": "pending",
      "new_value": "paid",
      "changed_at": "2026-04-29T10:15:00+00:00"
    }
  ],
  "count": 1
}
```

### `GET /changes/{service_name}/{table_name}/{column_name}/at`

Canonical point-in-time lookup endpoint.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `timestamp` | Yes | ISO 8601 timestamp |

Example:

```bash
curl \
  -H "X-API-Key: 1.example-secret" \
  "http://localhost:8000/changes/orderdb/orders/status/at?timestamp=2026-04-29T10:15:00Z"
```

### `GET /changes/{service.table}/{column_name}/at`

Legacy-compatible point-in-time lookup route. This accepts the service and table packed into the first path segment, for example:

```text
/changes/orderdb.orders/status/at?timestamp=...
```

## WebSocket endpoint

### `GET /ws/events`

Accepts a WebSocket connection and broadcasts messages shaped like:

```json
{
  "type": "new_event",
  "event": {
    "id": 10,
    "event_type": "u",
    "event_time": "2026-04-29T10:15:00+00:00",
    "user_id": null,
    "service_name": "orderdb.public.orders",
    "operation": "UPDATE",
    "source_table_id": 1,
    "event_data": {}
  }
}
```

Authentication is supplied via a short-lived `session_token` query parameter.
The recommended flow is:

1. Call `POST /auth/token` with `X-API-Key` to get a bearer token.
2. Call `POST /auth/ws-token` with `Authorization: Bearer <access_token>`.
3. Connect to `/ws/events?session_token=<short-lived-token>`.

## Notes

- `service_name` refers to the Kafka topic prefix used by the monitored source, such as `orderdb`, `catalogdb`, or `shippingdb`.
- `/changes` and point-in-time lookups operate on discovered table metadata. If no events have been consumed for a table yet, the table may not appear in discovery endpoints.
- When Debezium schema envelopes are disabled, column metadata is inferred from row payloads and may not include exact database types or primary-key flags.
