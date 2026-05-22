# DB Monitor API

This document describes the HTTP and WebSocket interfaces exposed by DB Monitor.

## Authentication model

Bootstrap and key-management endpoints accept long-lived API keys in the
`X-API-Key` header with this format:

```text
<key_id>.<raw_secret>
```

Roles:

- `viewer`: read-only access to tables, events, changes, and `/info`
- `admin`: viewer access plus key management and recovery endpoints

Recommended client flow:

1. Present `X-API-Key: <key_id>.<raw_secret>`.
2. Exchange it at `POST /auth/token` for a short-lived bearer token.
3. Use `Authorization: Bearer <access_token>` for normal HTTP requests.
4. Exchange that bearer token at `POST /auth/ws-token` before opening a
   WebSocket connection.

## Auth and key management

### `POST /auth/bootstrap`

Creates the first admin key. This succeeds only when there are no active API
keys and bootstrap is enabled.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `owner_name` | Yes | Human-readable owner label for the key |
| `ttl_days` | No | Expiration window in days, default server policy |

Example:

```bash
curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
```

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

### `POST /auth/keys`

Creates a new API key. Requires an admin key.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `owner_name` | Yes | Human-readable owner label for the key |
| `role` | No | `viewer` or `admin`, default `viewer` |
| `ttl_days` | No | Expiration window in days, default server policy |

### `GET /auth/keys`

Lists API keys for operator inventory and lifecycle actions. Requires an admin
key.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `include_inactive` | No | Include expired, revoked, or inactive keys |

Response shape:

```json
{
  "keys": [
    {
      "id": 7,
      "owner_name": "ops-admin",
      "role": "admin",
      "status": "active",
      "is_active": true,
      "created_at": "2026-05-22T12:00:00+00:00",
      "expires_at": "2026-08-20T12:00:00+00:00",
      "revoked_at": null,
      "current_authenticated": true,
      "can_rotate": true,
      "can_revoke": false
    }
  ],
  "count": 1,
  "current_api_key_id": 7
}
```

### `POST /auth/keys/{key_id}/rotate`

Rotates an existing key and returns a replacement credential once. Requires an
admin key.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `ttl_days` | No | Expiration window for the replacement key |

### `POST /auth/keys/{key_id}/revoke`

Revokes an existing key. Requires an admin key.

## Public endpoints

### `GET /health`

Returns overall status plus component-level checks for database connectivity,
consumer state, and application lifecycle state.

### `GET /livez`

Returns process liveness and whether shutdown is in progress.

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

Returns a discovered table and its columns.

### `GET /tables/{service_name}/{table_name}/columns`

Returns only the discovered columns for a table.

### `GET /events`

Returns stored events with pagination and optional filtering.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `limit` | No | `1-1000`, default `100` |
| `offset` | No | Pagination offset, default `0` |
| `service_name` | No | Filter by source prefix such as `orderdb` |
| `source_table_id` | No | Filter by discovered table ID |
| `row_identity` | No | JSON object string used for exact-record filtering |
| `event_type` | No | Filter by parsed event type |
| `start_time` | No | Inclusive ISO 8601 lower bound |
| `end_time` | No | Inclusive ISO 8601 upper bound |
| `search_term` | No | Case-insensitive substring match against raw payload |

Response shape:

```json
{
  "events": [
    {
      "id": 10,
      "event_type": "u",
      "event_time": "2026-04-29T10:15:00+00:00",
      "user_id": null,
      "service_name": "orderdb",
      "operation": "UPDATE",
      "source_table_id": 1,
      "row_identity": {"id": 42},
      "event_data": {}
    }
  ],
  "total": 1,
  "limit": 100,
  "offset": 0
}
```

### `GET /events/stats`

Returns aggregate counts grouped by service, event type, and operation.

### `GET /changes`

Returns column change history for a table.

Supported query forms:

1. `table_name=orderdb.orders`
2. `service_name=orderdb&table_name=orders`

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `table_name` | Yes | Table name or `<service>.<table>` composite |
| `service_name` | No | Explicit source prefix when `table_name` is bare |
| `column_name` | No | Restrict results to one column |
| `row_identity` | No | JSON object string for exact-record history |
| `from_time` | No | Inclusive ISO 8601 lower bound |
| `to_time` | No | Inclusive ISO 8601 upper bound |
| `limit` | No | `1-1000`, default `100` |
| `offset` | No | Pagination offset, default `0` |

Example:

```bash
curl \
  -H "Authorization: Bearer <token>" \
  "http://localhost:8000/changes?table_name=orderdb.orders&column_name=status&row_identity=%7B%22id%22%3A42%7D"
```

Response shape:

```json
{
  "service_name": "orderdb",
  "table_name": "orders",
  "row_identity": {"id": 42},
  "changes": [
    {
      "id": 42,
      "event_id": 10,
      "column_name": "status",
      "operation": "UPDATE",
      "old_value": "pending",
      "new_value": "paid",
      "changed_at": "2026-04-29T10:15:00+00:00"
    }
  ],
  "count": 1,
  "limit": 100,
  "offset": 0
}
```

### `GET /changes/{service_name}/{table_name}/{column_name}/at`

Canonical point-in-time lookup endpoint.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `timestamp` | Yes | ISO 8601 timestamp |

### `GET /changes/{service.table}/{column_name}/at`

Legacy-compatible point-in-time lookup route. This accepts the service and
table packed into the first path segment.

## Admin recovery endpoints

### `GET /admin/checkpoints`

Returns the persisted Kafka checkpoint snapshot keyed by consumer group, topic,
and partition.

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

Lists persisted DLQ records that can be replayed.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `limit` | No | `1-1000`, default `100` |
| `include_replayed` | No | Include already replayed records, default `false` |

Response shape:

```json
{
  "events": [
    {
      "id": 7,
      "kafka_topic": "orderdb.public.orders",
      "kafka_partition": 0,
      "kafka_offset": 99,
      "error_message": "duplicate key value",
      "is_replayed": false,
      "replay_error": null
    }
  ],
  "count": 1
}
```

### `POST /admin/dlq/replay`

Replays multiple persisted DLQ records through the normal ingestion path.

Query parameters:

| Name | Required | Description |
| --- | --- | --- |
| `limit` | No | Maximum number of records to replay, default `100` |
| `include_replayed` | No | Include already replayed rows in the batch |

Response shape:

```json
{
  "results": [
    {
      "dlq_event_id": 7,
      "status": "replayed"
    }
  ],
  "count": 1,
  "requested_limit": 100,
  "include_replayed": false,
  "replayed_count": 1,
  "duplicate_count": 0,
  "skipped_count": 0,
  "failed_count": 0
}
```

### `POST /admin/dlq/{dlq_event_id}/replay`

Replays a persisted DLQ record through the normal ingestion path. The response
status is typically `replayed` or `duplicate`.

## WebSocket endpoint

### `GET /ws/events?session_token=<token>`

Accepts a WebSocket connection and broadcasts new-event messages shaped like:

```json
{
  "type": "new_event",
  "event": {
    "id": 10,
    "event_type": "u",
    "event_time": "2026-04-29T10:15:00+00:00",
    "user_id": null,
    "service_name": "orderdb",
    "operation": "UPDATE",
    "source_table_id": 1,
    "row_identity": {"id": 42},
    "event_data": {}
  }
}
```

Recommended flow:

1. Call `POST /auth/token` with an API key.
2. Call `POST /auth/ws-token` with the bearer token.
3. Connect to `/ws/events?session_token=<short-lived-token>`.

## Notes

- `service_name` refers to the monitored source prefix such as `orderdb`,
  `catalogdb`, or `shippingdb`.
- `row_identity` filters must be supplied as a JSON object string.
- `/changes` and point-in-time lookups operate on discovered table metadata.
  If a table has never been observed, it may not appear in discovery endpoints.
- When Debezium schema envelopes are disabled, column metadata is inferred from
  row payloads and may not include exact database types or primary-key flags.
