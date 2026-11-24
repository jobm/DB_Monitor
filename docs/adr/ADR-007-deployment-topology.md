# ADR-007: Deployment Topology — Shared Control Plane, Isolated Customer Cells

**Status:** Accepted
**Date:** 2026-11-25
**Deciders:** Project leads
**Context:** [Pre-V1 Tenant Isolation Strategy](../pre-v1-tenant-isolation-strategy.md), [Deployment](../deployment.md#1-choose-your-deployment-model), [ADR-006](ADR-006-multi-tenancy-isolation.md)

---

## Context

DB Monitor serves two audiences with different isolation needs:

1. A single team running it as an audit-log and CDC service for its own
   databases.
2. A platform team offering it to multiple customers, where a cross-tenant
   data path is a security incident rather than a bug.

The runtime is already tenant-aware: API keys carry a nullable `tenant_id`
(ADR-006), events are partitioned by `service_name`, and quotas and SLO
policies can be scoped per tenant cohort. That made it tempting to run one
shared deployment for everyone. It also made the topology an implicit
assumption rather than a recorded decision: the provisioning API exists, but
nothing states whether it provisions cells or simply records intent.

## Decision

**Two models are supported, and one is the default for a managed offering.**

- **Model A — single-tenant deployment (self-hosted):** one instance, one
  monitor database, one broker namespace. Supported indefinitely.
- **Model B — shared control plane with isolated customer cells (default for
  multi-customer use):** each customer gets a dedicated DB Monitor runtime,
  monitor database (or server), broker namespace, and credential set. The
  control plane is shared; the data plane is not.

**A fully shared multi-tenant data plane is explicitly out of scope for v1.**
It stays deferred until the exit criteria below are met.

The control-plane API is **bookkeeping and orchestration state, not the
provisioner**. `POST /admin/customers/{customer_id}/provision-jobs` records an
8-step job (`register_customer` … `bootstrap_and_activate`); per-step status is
reported back through
`POST /admin/customers/{customer_id}/provision-jobs/{job_id}/steps/{step_name}`.
Actual infrastructure changes are executed by the deployment templates and
release pipeline, never by a request handler holding cloud credentials.

## Design

### What is shared

- Source code and build artifact.
- CI/CD pipeline, Helm chart, Terraform modules, and Bicep templates.
- Control-plane metadata: customer inventory, provisioning job state
  (`customer_provision_jobs`), lifecycle state, rollout status.
- Aggregate operator dashboards and alert routes.

### What is isolated per customer cell

| Resource | Isolation |
|----------|-----------|
| DB Monitor API + consumer | One deployment per cell |
| Monitor Postgres | Dedicated database minimum; dedicated server preferred |
| Kafka / RabbitMQ | Topic prefix or vhost per customer, unique consumer group |
| DLQ and replay | Per-cell records and operator actions |
| Retention archive | Separate container or path prefix |
| Secrets | API keys, JWT, webhook signing secret per cell |
| Endpoints | Customer-facing tokens scoped to the cell |

### Kubernetes layout

- Namespace `dbm-<customer_slug>`, one per cell.
- `db-monitor-api-<slug>` and `db-monitor-consumer-<slug>` deployments.
- Default-deny `NetworkPolicy`, egress limited to the broker, Postgres, and
  declared webhook destinations.
- Per-namespace `ResourceQuota`, per-pod requests/limits, and HPA bounds so one
  cell cannot starve another.

### Control-plane API surface

| Endpoint | Responsibility |
|----------|----------------|
| `POST /admin/customers/{id}/provision-jobs` | Create the job and its 8 pending steps |
| `GET /admin/customers/{id}/provision-jobs` | List jobs for one customer |
| `GET /admin/customers/{id}/provision-jobs/{job_id}` | Read one job |
| `POST …/steps/{step_name}?status=` | Advance a step and append an audit entry |
| `POST /admin/customers/{id}/suspend` / `resume` | Mark a cell inactive/active |
| `POST /admin/customers/{id}/upgrade?target_version=` | Register an upgrade target |
| `GET /admin/customers/{id}/lifecycle` | Read current lifecycle state |

All of these require a customer-scoped admin key and reject requests where the
`X-DBM-Customer-ID` header disagrees with the path. They record state; they do
not mutate customer infrastructure.

### Why API-key tenant scoping stays

ADR-006's tenant scoping is not made redundant by cells. It remains
defense-in-depth inside a cell, and it is what makes Model A safe to operate
for teams that legitimately want a shared deployment with per-team keys.

## Tradeoffs Considered

| Alternative | Reason not chosen |
|-------------|-------------------|
| **Single shared data plane with API-key scoping only** | One query bug or missed filter becomes a cross-customer leak; the pre-v1 codebase has not passed the security review that would justify it |
| **Shared database with row-level security as the primary boundary** | Needs RLS coverage on every table plus session-variable discipline in every request path; strong candidate post-v1 as defense-in-depth, not as the primary boundary now |
| **Database-per-tenant inside one shared app deployment** | Reduces per-customer cost but keeps a shared application process, so a crash, memory leak, or hot query still spans customers |
| **Fully separate infrastructure per customer, including CI/CD** | Duplicates the release pipeline and makes upgrades unmanageable |

**Cost accepted:** each cell pays for its own API/consumer pods and monitor
database, so unit cost is higher than a pooled deployment. That is the price of
a blast radius limited to one customer, and it is measurable later (see exit
criteria).

## Consequences

### Positive

- Isolation is structural: no application-level query is the only thing
  standing between two customers.
- Noisy neighbors are bounded by quotas per namespace.
- Emergency isolation is a single suspend or namespace pause.
- A bad release can be rolled out to one cohort (canary) before the rest.
- The control plane can evolve without redeploying every cell.

### Negative / accepted risks

- **Migration fan-out:** schema migrations must run per cell. The advisory lock
  in `migrations/core.py` protects one database, not a fleet; rollout tooling
  must sequence cells and report per-cell failures.
- **Upgrade skew:** cells can run different versions. Support must know which
  version a customer is on; `/admin/customers/{id}/lifecycle` records the
  target, not the running version.
- **Observability fragmentation:** metrics and alerts are per cell. Any
  aggregate view needs a customer label on every series, and incident payloads
  must carry the customer slug.
- **Control-plane drift:** because the API only records steps, a job can claim
  `deploy_monitor_services` completed while the cluster disagrees. The
  reconcile step is the pipeline's responsibility.
- **Onboarding latency:** 8 recorded steps plus real provisioning, targeting
  under 15 minutes fully automated.

### Migration path

- **A → B:** stand up the control plane, then create one cell per existing
  tenant and cut traffic over per customer. No data model change is required;
  `service_name` and `tenant_id` already carry the identity.
- **B → shared plane:** only after the exit criteria below; would make
  Postgres RLS the primary boundary and require a data-plane migration, which
  is why it is a separate ADR when it happens.

## Exit Criteria To Revisit

Re-open this decision only when **all** hold:

1. Onboarding and upgrades are fully automated and reconciled.
2. Per-cell cost is measured and predictable.
3. Per-customer SLO attainment is stable across multiple release cycles.
4. Isolation controls have passed security review and penetration testing.

## References

- [`docs/pre-v1-tenant-isolation-strategy.md`](../pre-v1-tenant-isolation-strategy.md)
- [`docs/deployment.md`](../deployment.md)
- [ADR-006: Multi-Tenancy Isolation](ADR-006-multi-tenancy-isolation.md)
- Provisioning: `src/db_monitor/routes/ops/provisioning/`
- Lifecycle: `src/db_monitor/routes/ops/lifecycle.py`
