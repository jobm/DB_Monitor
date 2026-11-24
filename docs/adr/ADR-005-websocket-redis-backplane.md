# ADR-005: Redis Pub/Sub Backplane for WebSocket Fanout

**Status:** Implemented  
**Date:** 2026-07-05  
**Deciders:** Project leads  
**Context:** [WebSocket endpoint](../api-docs.md#websocket-endpoint), [Backplane config](../configuration.md#websocket-backplane-settings)

---

## Context

DB Monitor broadcasts CDC events to connected WebSocket clients in real-time.
When running multiple replicas for horizontal scaling, events consumed by one
replica must be fanned out to WebSocket clients connected to other replicas.

The initial implementation used **Postgres LISTEN/NOTIFY** as the cross-replica
backplane. This works well at low scale but has known limitations:

- **No persistence.** If a listener disconnects, notifications are lost.
- **Shared database dependency.** All replicas connect to the same Postgres
  instance for notification delivery, adding load to the monitor database.
- **Payload size limit.** The effective payload limit is ~8KB.
- **No channel topology control.** Every subscriber receives every notification.
- **No independent scalability.** The backplane cannot be scaled separately
  from the database.

Benchmark results (BM-2) showed the system can sustain ~460-570 ev/s with a
single consumer. At higher throughput targets (2000+ ev/s), the Postgres
backplane becomes a bottleneck because:

1. Every event triggers a `pg_notify` + a DB query (`SELECT ... WHERE id = N`)
   on the receiving replica.
2. With multiple replicas, each event generates N-1 DB queries for fan-out.
3. At 2000+ ev/s, this multiplies to thousands of additional queries per second.

## Decision

Implement a **Redis Pub/Sub backplane** as an alternative to Postgres
LISTEN/NOTIFY, configurable via `WS_BACKPLANE_BACKEND=redis`.

### Architecture

```
Replica A                    Redis                    Replica B
  │                           │                         │
  ├── consume event ──────────┤                         │
  │                           │                         │
  ├── broadcast local WS ─────┤                         │
  │                           │                         │
  ├── redis.publish() ────────┼─── PUBLISH ─────────────┤
  │                           │    db_monitor_ws        │
  │                           │    channel              ├── pubsub.listen()
  │                           │                         ├── deliver locally
  │                           │                         └── broadcast to local WS
```

### Rationale

1. **Lower latency.** Redis Pub/Sub delivers messages with sub-millisecond
   latency, independent of the monitor database. Postgres LISTEN/NOTIFY must
   serialize through the Postgres WAL and connection pool.

2. **Reduced database load.** With Redis, the backplane traffic bypasses the
   monitor database entirely. The monitor DB handles only CDC event storage
   and API queries.

3. **Independent scalability.** Redis can be scaled vertically or clustered
   independently of the application replicas and the monitor database.

4. **Persistent pub/sub subscribers.** Redis handles subscriber disconnects
   gracefully; the `async for message in pubsub.listen()` loop auto-reconnects
   with exponential backoff built into the `redis-py` client.

5. **Pluggable architecture.** A `BackplaneProtocol` abstract interface allows
   both Postgres and Redis backends to coexist. The factory picks the active
   backend based on `WS_BACKPLANE_BACKEND`. New backends (e.g., NATS, RabbitMQ)
   can be added without changing the WebSocket manager.

### Implementation Details

- The `BackplaneProtocol` defines three methods: `start()`, `stop()`, `publish()`.
- `PostgresBackplane` wraps the existing `pg_notify` + `asyncpg` listener logic.
- `RedisBackplane` uses `redis.asyncio` (from the `redis` PyPI package) with
  `pubsub.subscribe()` and `pubsub.listen()` for async message delivery.
- The `SchemaCacheBackplane` was also updated to use the pluggable backplane
  on a dedicated channel (`<ws_channel>_schema`).
- `WS_BACKPLANE_BACKEND` defaults to `postgres` for backward compatibility.
- `REDIS_URL` is required when the backend is `redis`.

### Configuration

```env
# Use Redis backplane (recommended for 2000+ ev/s)
WS_BACKPLANE_BACKEND=redis
REDIS_URL=redis://redis:6379/0

# Or use Postgres backplane (default, no additional infrastructure)
WS_BACKPLANE_BACKEND=postgres
```

### Rejected alternatives

- **Keep Postgres LISTEN/NOTIFY only.** Viable at current throughput (~500 ev/s)
  but would limit future scaling. The pluggable design makes both available.

- **Use Kafka for the backplane.** Kafka adds operational complexity and is
  already used for the primary event pipeline. Redis is lighter weight for
  ephemeral pub/sub.

- **Direct inter-replica HTTP calls.** Requires service discovery and would
  create N×(N-1) fan-out connections. Not practical beyond 2-3 replicas.

- **gRPC streaming.** Adds significant complexity and requires managing
  long-lived streams between all replica pairs.

## Consequences

**Positive:**

- Redis backplane provides sub-millisecond fanout latency, independent of DB.
- Pluggable design allows operators to choose the backend that fits their
  infrastructure and scale requirements.
- Postgres backplane remains the default (no additional infrastructure needed
  for small deployments).
- Schema cache invalidation also benefits from the Redis backplane.

**Negative:**

- Redis is an additional infrastructure dependency for deployments that choose
  the Redis backend.
- Two backends to test and maintain.
- The `redis` PyPI package adds ~2MB to the deployment size.

## Migration

Deployments currently using the Postgres backplane (default) are unaffected.
To switch to Redis:

1. Deploy Redis (e.g., `bitnami/redis` Helm chart or managed Redis).
2. Set `WS_BACKPLANE_BACKEND=redis` and `REDIS_URL=redis://redis:6379/0`.
3. No downtime — the WebSocket manager re-creates the backplane on restart.

## Compliance

- [x] `BackplaneProtocol` abstract interface in `db_monitor/backplane/base.py`
- [x] `PostgresBackplane` implementation in `db_monitor/backplane/postgres.py`
- [x] `RedisBackplane` implementation in `db_monitor/backplane/redis.py`
- [x] `create_backplane()` factory in `db_monitor/backplane/factory.py`
- [x] `ws_manager.py` delegates to the active backplane
- [x] `SchemaCacheBackplane` updated to use pluggable backplane
- [x] `WS_BACKPLANE_BACKEND` env var (default: `postgres`)
- [x] `REDIS_URL` env var (required when backend is `redis`)
- [x] Helm chart values for both backends
- [x] ConfigMap wiring for both backends
- [x] Tests for Redis backplane
