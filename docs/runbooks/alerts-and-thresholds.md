# Alerts And Thresholds Runbook

This runbook documents the alert thresholds currently configured for DB Monitor
and the first operator actions for each alert.

## Current Thresholds

- `APIHighLatency`: p95 API latency above `1.0s` for `5m`.
- `DatabasePoolUtilizationHigh`: DB pool utilization above `90%` for `5m`.
- `DatabasePoolOverflowActive`: DB pool overflow connections in use for `5m`.
- `ConsumerLagBacklog`: any Kafka partition lag above `1000` for `5m`.
- `ConsumerCommitStalled`: last successful Kafka commit older than `300s`
  for `5m` after the consumer has previously committed.
- `DeadLetterBacklogPending`: one or more persisted DLQ records pending for
  `5m`.
- `ConsumerCircuitBreakerOpen`: consumer circuit breaker open for `1m`.
- `FailedAuthAttemptsSpike`: failed authentication attempts above `5/min`
  sustained for `5m`.

## Threshold Rationale

The current API latency threshold is intentionally set above the observed mixed
endpoint validation baseline:

- mixed profile load run: `67.26 req/s`
- average latency: `295.67 ms`
- p95 latency: `375.37 ms`

This leaves room for ordinary variance while still surfacing a clear regression
before the API degrades into multi-second responses.

## First Response By Alert

### APIHighLatency

1. Check `/readyz` and `/health`.
2. Run the configured load profile again if the stack is otherwise healthy.
3. Inspect recent deploys, DB pool settings, and hot endpoints.

### DatabasePoolUtilizationHigh

1. Check `DB_POOL_SIZE` and `DB_MAX_OVERFLOW` in the active deployment.
2. Compare current latency against the known load baseline.
3. Inspect slow queries, stuck requests, or long-lived transactions.

### DatabasePoolOverflowActive

1. Treat this as sustained pool pressure, not a one-off spike.
2. Inspect current checked-out workload and long-running DB operations.
3. Raise pool capacity only after ruling out slow-query or leak behavior.

### ConsumerLagBacklog

1. Check `/readyz` for consumer lag and commit age.
2. Inspect Kafka Connect and broker health.
3. Confirm topics are receiving and being consumed normally.

### ConsumerCommitStalled

1. Check monitor-server logs.
2. Confirm PostgreSQL connectivity and write health.
3. Check whether the circuit breaker is open.

### DeadLetterBacklogPending

1. Inspect `/admin/dlq`.
2. Use the recovery steps in `docs/runbooks/recovery-and-validation.md`.
3. Replay only after the underlying failure cause is understood.

### ConsumerCircuitBreakerOpen

1. Treat as an active ingestion incident.
2. Check database errors, Kafka connectivity, and recent schema changes.
3. Confirm the breaker closes after the underlying failure path is removed.

### FailedAuthAttemptsSpike

1. Review auth logs and recent key rotation activity.
2. Confirm client tokens and API keys are still valid.
3. Escalate if the failure pattern looks abusive rather than accidental.