# Recovery And Validation Runbook

This runbook covers the operator workflow for checkpoint inspection, DLQ
listing and replay, and the recommended live validation order after a deploy.

## Preconditions

- The DB Monitor stack is running and reachable on `http://localhost:8000`.
- You have an admin API key in `id.secret` format.
- The admin key has been exchanged for a bearer token with:

```bash
curl -X POST \
  -H "X-API-Key: <id.secret>" \
  http://localhost:8000/auth/token
```

- Export the returned token for the commands below:

```bash
export DB_MONITOR_ACCESS_TOKEN="<bearer-token>"
```

## 1. Inspect Consumer Checkpoints

Use checkpoints to confirm the consumer is still advancing per topic and
partition after deploys, broker interruptions, or replay activity.

```bash
curl \
  -H "Authorization: Bearer $DB_MONITOR_ACCESS_TOKEN" \
  http://localhost:8000/admin/checkpoints
```

Healthy signs:

- `count` is non-zero for active topics.
- `updated_at` keeps moving forward during active ingestion.
- `kafka_offset` increases over time for active partitions.

Investigation triggers:

- Missing checkpoint rows for expected topics.
- `updated_at` stops moving while producers are active.
- Checkpoints advance for some partitions but not others.

## 2. List Pending DLQ Records

Use the admin DLQ listing to see consumer failures that were persisted for
recovery.

```bash
curl \
  -H "Authorization: Bearer $DB_MONITOR_ACCESS_TOKEN" \
  "http://localhost:8000/admin/dlq?limit=50"
```

Important fields:

- `id`: replay target identifier.
- `kafka_topic`, `kafka_partition`, `kafka_offset`: original Kafka position.
- `error_message`: most recent failure cause.
- `is_replayed`: whether the record has already been replayed.
- `replay_error`: last replay failure, if any.

To include historical replayed rows:

```bash
curl \
  -H "Authorization: Bearer $DB_MONITOR_ACCESS_TOKEN" \
  "http://localhost:8000/admin/dlq?limit=50&include_replayed=true"
```

## 3. Replay A DLQ Record

Replay a single persisted DLQ event back through the normal ingestion path.

```bash
curl -X POST \
  -H "Authorization: Bearer $DB_MONITOR_ACCESS_TOKEN" \
  http://localhost:8000/admin/dlq/<dlq_event_id>/replay
```

Expected outcomes:

- `status: replayed`: the event was inserted on replay.
- `status: duplicate`: the event was already present and replay stayed
  idempotent.
- HTTP `404`: the DLQ record no longer exists.

After replay:

- Re-run `GET /admin/dlq` to confirm the record now shows `is_replayed: true`.
- Re-run `GET /admin/checkpoints` if the replay was part of a larger recovery
  exercise.
- Check `/events` or `/changes` when you need to confirm the user-visible data
  result.

## 4. Replay A Batch Of DLQ Records

Replay multiple persisted DLQ rows in failed-at order.

```bash
curl -X POST \
  -H "Authorization: Bearer $DB_MONITOR_ACCESS_TOKEN" \
  "http://localhost:8000/admin/dlq/replay?limit=25"
```

Useful cases:

- recovering a backlog after a transient downstream outage
- replaying a small operator-reviewed batch instead of one row at a time

Recommended follow-up:

- review `failed_count` in the response
- re-check `GET /admin/dlq`
- confirm `/readyz` and `/admin/checkpoints` after the batch finishes

## 5. Live Validation Order After Deploy

Use this order after a production-style change:

1. Start or confirm the stack.

```bash
make monitor-up
```

2. Confirm readiness.

```bash
curl http://localhost:8000/readyz
```

3. Run the integration suite.

```bash
make monitor-test-integration
```

4. Run load validation after integration is green.

```bash
cd app
uv run python ../scripts/load_test.py \
  --workers 20 \
  --requests 100 \
  --targets events,stats,tables,checkpoints
```

5. Run the horizontal-scaling validation when the websocket path,
   backplane, or multi-replica deployment behavior changed.

```bash
make monitor-test-scale
```

Expected result:

- two local replicas start on separate ports
- one replica receives a websocket event after replay is triggered on the
  other replica
- the validator can self-provision a temporary admin key for local runs when
  `DB_MONITOR_ADMIN_API_KEY` is not set
- the script exits non-zero and prints replica log tails when cross-replica
  delivery fails

## 6. Escalation Guide

Escalate before replaying repeatedly when:

- the same DLQ record fails replay with the same `replay_error`
- checkpoint timestamps are stale and consumer lag is increasing
- readiness remains degraded after replay and ingestion recovery

In those cases, capture:

- `/readyz`
- `/admin/checkpoints`
- `/admin/dlq?include_replayed=true`
- relevant application logs from `make monitor-logs-app`

Then pause manual replay until the underlying ingestion or schema issue is
understood.