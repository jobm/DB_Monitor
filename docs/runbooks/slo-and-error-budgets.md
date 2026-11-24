# SLO Targets And Error Budgets

This runbook publishes the service-level objectives for each tenant cohort, the
metrics that evaluate them, and the alerts that fire when a cohort is burning
its error budget.

Policy values live in `TENANT_COHORT_SLO_POLICIES` and the rolling window in
`SLO_ROLLING_WINDOW_DAYS` (see
[Configuration](../configuration.md#tenant-cohort-slo-policy-settings)). The
active policy is published at `GET /admin/slo-policy`, which returns the window
length and the resolved cohort list. This page is the human-readable contract;
that endpoint is the machine-readable copy.

## Cohorts

A cohort is a set of tenants with the same availability and freshness
obligations. Cohorts are sized by source-database count (`max_sources`); the
operator picks the cohort for a cell at deploy time, not per request.

| Cohort | Max sources | Availability target | Error budget | Budget in 30d | Max commit age | Max lag | Max DLQ |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `small` | 10 | 99.9% | 0.1% | 43.2 min | 180 s | 500 | 0 |
| `medium` | 50 | 99.5% | 0.5% | 216 min | 300 s | 1,000 | 5 |
| `large` | 100 | 99.0% | 1.0% | 432 min | 600 s | 5,000 | 20 |

These are the built-in defaults in
`src/db_monitor/core/config/normalizers/slo.py`. A deployment overrides them
with `TENANT_COHORT_SLO_POLICIES`; this table must be updated in the same change
as any override that ships.

`error_budget_percent` is expected to equal `100 - availability_target`. The
config validator checks ranges but not that relationship, so a mismatched
override silently changes budget maths. Check it deliberately when overriding.

## Cohort Assignment

Each cell carries exactly one cohort label. This follows
[ADR-007](../adr/ADR-007-deployment-topology.md): a cell is provisioned for one
customer, so the cohort is a property of the deployment, not of an individual
event.

Prometheus adds the cohort at scrape time in `monitoring/prometheus/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'db_monitor'
    static_configs:
      - targets: ['monitor-server:8000']
        labels:
          cohort: small          # change per cell: small | medium | large
          customer_id: acme      # matches the provisioning observability labels
```

Rules below filter on `cohort`. A cell scraped without that label produces no
cohort series and no SLO alerts, so a missing label fails quiet rather than
loud — verify it during cell provisioning (see the validation section).

If one cell must host several tenants, they must all be in the same cohort;
per-tenant labels cannot differ within a single scrape target.

## Service-Level Indicators

| SLO | Definition | Source metric | Expression |
| --- | --- | --- | --- |
| Ingestion availability | Share of consumed events that are processed rather than failed | `db_monitor_events_processed_total`, `db_monitor_events_failed_total` | `processed / (processed + failed)` |
| Freshness | Share of time the consumer's last successful commit is within the cohort limit | `db_monitor_consumer_last_successful_commit_timestamp_seconds` | `time() - commit_ts <= max_commit_age_seconds` |
| Backlog | Share of time partition lag stays within the cohort limit | `db_monitor_consumer_lag` | `max by (cohort) (lag) <= max_consumer_lag` |
| Dead-letter | Persisted DLQ backlog stays within the cohort limit | `db_monitor_dlq_records_pending` | `pending <= max_dlq_messages` |

Duplicate-skipped events are deliberately excluded from the availability SLI:
`db_monitor_events_duplicate_skipped_total` counts at-least-once redelivery,
which is expected behaviour rather than unavailability. `events_consumed_total`
therefore cannot be used as the denominator; `processed + failed` is.

Requests that fail after all retries increment `events_failed_total` and, when
DLQ is enabled, are also persisted for replay. A DLQ replay that later succeeds
does not retroactively clear availability errors, so a burst of retriable
failures costs budget even when it self-heals. That is intentional: customers
saw delayed ingestion.

## Error Budgets

The rolling window is 30 days (43,200 minutes) unless `SLO_ROLLING_WINDOW_DAYS`
changes it. Budget is either time-in-violation (freshness, backlog, DLQ) or
failed events (availability).

Burn rate is budget consumed per unit of window, normalised so that a burn rate
of `1` exhausts the entire budget in exactly one window:

```text
burn_rate = observed_error_ratio / (error_budget_percent / 100)
```

The same formula applies to time-based SLOs, with the observed error ratio
being the fraction of the window spent outside the cohort limit.

| Burn rate | Budget consumed | Time to exhaustion | Response |
| --- | --- | --- | --- |
| 14.4× | 2% in 1 h | ~50 h | Page the on-call engineer |
| 6× | 5% in 6 h | ~5 days | Ticket, fix in the current cycle |
| 1× | Full budget | 30 days | Normal operation |

## Alerts

`monitoring/prometheus/alerts.yml` ships a `db_monitor_slo` group. It contains
recording rules that normalise the SLIs and, per cohort, burn-rate and
threshold alerts:

| Alert | Condition | Severity |
| --- | --- | --- |
| `IngestionAvailabilityBudgetBurnFast` | 14.4× burn over 1 h and 5 m | critical |
| `IngestionAvailabilityBudgetBurnSlow` | 6× burn over 6 h and 30 m | warning |
| `FreshnessBudgetBurnFast` | 14.4× of out-of-limit time over 1 h and 5 m | critical |
| `FreshnessBudgetBurnSlow` | 6× of out-of-limit time over 6 h and 30 m | warning |
| `ConsumerLagAboveCohortLimit` | lag above `max_consumer_lag` for 5 m | warning |
| `DeadLetterAboveCohortLimit` | DLQ backlog above `max_dlq_messages` for 5 m | warning |

Every alert carries `cohort`, `slo`, and `severity` labels, so Alertmanager can
route by cohort without parsing alert names:

```yaml
route:
  routes:
    - matchers: [severity="critical"]
      receiver: pagerduty
    - matchers: [cohort="small"]
      receiver: pagerduty
```

The existing global readiness alerts (`ConsumerLagBacklog`,
`DeadLetterBacklogPending`) remain in place and are keyed to
`READINESS_MAX_CONSUMER_LAG` / `READINESS_MAX_DLQ_MESSAGES`. They guard
`/readyz`; the cohort rules here guard the customer-facing objective. On a
correctly configured cell both fire together, and the cohort alert is the one
that names the budget consequence.

## Responding To A Burn Alert

1. Confirm the cell is scraped with the expected `cohort` label; a mislabelled
   cell produces alerts for the wrong budget.
2. Identify the affected tenant from the `service` label on the SLI series:
   `sum by (service) (rate(db_monitor_events_failed_total[5m]))`.
3. Check whether the failure is ingestion-wide or tenant-specific. Use
   [Alerts And Thresholds](alerts-and-thresholds.md) for the underlying
   condition (broker health, pool pressure, circuit breaker).
4. If the budget is exhausted, freeze non-essential changes for that cohort and
   follow the escalation in [Recovery And Validation](recovery-and-validation.md).
5. Record the incident against the cohort. Budget exhaustion is a planning
   signal: either the target is wrong for the cohort or the cell needs capacity.

## Validation

Check rule syntax with `promtool` before shipping a change:

```bash
podman run --rm \
  -v "$PWD/monitoring/prometheus:/rules:z" \
  --entrypoint promtool \
  docker.io/prom/prometheus:v2.53.0 \
  check rules /rules/alerts.yml
```

Confirm the published policy matches this page:

```bash
curl -s -H "X-API-Key: $DBM_ADMIN_KEY" \
  http://localhost:8000/admin/slo-policy | jq .
```

Confirm the Prometheus target carries the cohort label:

```bash
curl -s http://localhost:9090/api/v1/targets \
  | jq '.data.activeTargets[] | select(.labels.job=="db_monitor") | .labels'
```
