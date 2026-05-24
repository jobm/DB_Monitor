# DB Monitor Principal Readiness Review (2026-05-24)

## Scope

This review evaluates readiness for serving approximately 60 startups, each
with 10-100 source databases (about 600-6000 source DBs total), from a
principal software engineer perspective.

This assessment is based on the current architecture, runtime code paths,
operational docs, and existing validation tooling in the repository.

## Executive Verdict

**Overall readiness: Conditional for controlled pilot, not yet ready for broad
multi-tenant scale at the upper target range.**

- Ready now for:
  - small production cohorts
  - per-startup isolated deployments
  - lower to moderate event throughput
  - teams with active SRE support
- Not ready yet for:
  - a single shared control plane serving thousands of DBs
  - high-cardinality noisy-neighbor multi-tenancy
  - strict enterprise SLOs without additional scaling and governance work

## Current Architecture Strengths

1. Durable ingestion semantics are solid:
   - manual ack/commit after successful persistence
   - DLQ persistence plus replay APIs
   - circuit breaker and readiness gates
2. Operational maturity is above average for this stage:
   - health/readiness surfaces
   - checkpoint visibility
   - runbooks and alert thresholds
3. Flexible broker model:
   - Kafka + RabbitMQ
   - multi-cluster and partition pinning support
4. Security baseline is practical:
   - API key and JWT flows
   - secret-file support
   - bootstrap disabled by default in prod mode

## Scale and Performance Assessment

## Workload Lens

For 60 startups with 10-100 DBs each, meaningful capacity planning must assume
wide variance:

- Small tenant: few DBs, low change rate
- Medium tenant: dozens of DBs, bursty traffic
- Large tenant: high write-rate workloads with spikes

A realistic platform target should be modeled in events/second, not DB count.
DB count mostly increases metadata cardinality and operational surface area;
event rate drives resource pressure.

## Expected Behavior of Current Design

### Ingestion path

- The consumer task processes messages in a mostly sequential async loop per
  consumer task.
- Each event performs several DB interactions:
  - schema upsert/check
  - event insert
  - optional change extraction writes
  - checkpoint persistence
  - optional websocket/webhook fanout
- Batch mode exists, but event writes within a batch still execute per-event.

**Implication:** throughput scales, but not linearly, and tends to become
database-write-bound as event volume rises.

### Read API path

- `/events` currently uses `LIMIT/OFFSET` plus total count queries.
- At high data volume, deep offsets and full count scans can become expensive.

**Implication:** read performance will degrade under large history windows and
high-concurrency dashboard/API usage.

### Storage model

- Core tables (`events`, `column_changes`, `dead_letter_events`) are indexed.
- There is no built-in table partitioning/retention policy in current docs/code
  path.

**Implication:** long-lived production clusters may see index bloat, vacuum
pressure, and slower historical queries.

## Practical Readiness for 60 Startups

## If deployed as one shared cluster

**Risk is high** without additional controls. Main concerns:

1. No explicit tenant isolation model (quotas, noisy-neighbor controls,
   resource fairness).
2. Single monitor Postgres becoming central bottleneck.
3. Operational blast radius across tenants.

## If deployed per startup (or per cohort)

**Feasible for near-term rollout**, assuming:

1. clear per-tenant deployment boundaries,
2. autoscaling for consumer replicas,
3. strict operational SLOs and on-call workflows,
4. retention and capacity management introduced quickly.

## Principal Risks to Address Before Broad Rollout

1. Multi-tenancy and isolation
   - global admin/viewer roles but no first-class tenant boundary model
   - no tenant-level rate limits or quotas
2. Data growth and lifecycle
   - no explicit archival/retention strategy enforced for high-volume tables
3. Query scalability
   - offset pagination and total counts on large datasets
4. Ingestion throughput ceiling
   - per-event DB-heavy processing path; batching does not yet provide bulk DB
     write semantics
5. Backplane scaling behavior
   - websocket/schema invalidation use Postgres LISTEN/NOTIFY, which is useful
     but can become coordination-heavy at very high event fanout
6. Capacity governance
   - no documented per-tenant SLO/error budgets and admission controls

## Recommended Improvement Plan (Prioritized)

## Phase 1 (Immediate, 2-4 weeks)

1. Define a deployment topology decision:
   - Option A: per-startup isolated deployment (recommended now)
   - Option B: shared multi-tenant plane (requires extra controls below)
2. Introduce retention policies:
   - configurable TTL/archival for `events`, `column_changes`, DLQ history
   - scheduled purge/archival jobs with audit trails
3. Replace offset-heavy read patterns:
   - add cursor/keyset pagination for `/events` and `/changes`
   - make expensive total counts optional or approximate
4. Add capacity guardrails:
   - tenant/source-level ingestion rate limits
   - max tracked topics/tables per deployment profile

## Phase 2 (Near term, 4-8 weeks)

1. Increase ingestion efficiency:
   - add true bulk insert paths for event batches and change rows
   - reduce per-event metadata lookups with stronger in-memory caches and
     write-through invalidation
2. Hard multi-tenant controls (if shared plane):
   - tenant identity on all key entities
   - tenant-scoped authz policies
   - per-tenant quotas and budget-based throttling
3. Observability upgrades:
   - per-tenant/per-source SLI dashboards
   - queue depth, commit age, and lag by tenant/source group
   - alert routing by ownership boundary

## Phase 3 (Scale hardening, 8-12+ weeks)

1. Storage scaling strategy:
   - partition large event/history tables (time and/or tenant key)
   - tune index strategy for high-cardinality filters
2. Data architecture evolution:
   - optional hot/cold tiering for historical event data
   - optional analytical replica/read model for heavy query workloads
3. Websocket fanout architecture:
   - evaluate dedicated fanout bus for high-scale realtime clients
     (if LISTEN/NOTIFY pressure appears)

## Performance Outlook

Without the improvements above, performance is likely to be acceptable for
small-to-moderate deployments, but inconsistent at the high end (large tenant
count plus high event throughput plus heavy read traffic).

With the Phase 1 and Phase 2 improvements, the platform can support a
substantially larger production footprint with predictable behavior and clearer
operational boundaries.

## Suggested Rollout Strategy

1. Launch with 5-10 startups in isolated deployments.
2. Run a 2-4 week burn-in with strict SLO tracking:
   - ingestion lag
   - commit staleness
   - API p95/p99
   - DLQ growth and replay success
3. Expand to 20-30 startups only after SLO stability.
4. Reach 60 startups after retention, pagination, and quota controls are
   complete and validated under load.

## Go/No-Go Recommendation

**Go for limited production rollout** with explicit constraints and phased
onboarding.

**No-go for full 60-startup broad rollout in a shared plane** until tenant
isolation, retention lifecycle, and read/ingestion scaling changes are in
place.

## Actionable Next Checklist

1. Choose target deployment model (isolated vs shared multi-tenant).
2. Implement retention + archival policy and automation.
3. Implement cursor-based pagination and optional count behavior.
4. Add source/tenant ingestion quotas and throttling.
5. Add bulk-write ingestion optimizations.
6. Define and publish platform SLOs/error budgets per tenant cohort.
