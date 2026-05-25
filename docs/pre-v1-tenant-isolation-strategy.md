# Pre-V1 Tenant Isolation Strategy

This document defines a practical pre-v1 deployment model that allows
DB Monitor to launch quickly while preserving strict customer isolation and
reducing noisy-neighbor risk.

## Decision Summary

Use a **shared control plane** with **isolated customer cells**.

- Shared control plane: onboarding API/workflows, release pipeline, templates,
  monitoring aggregation, and operator tooling.
- Isolated customer cell: one runtime instance of DB Monitor plus isolated
  state and credentials per customer.

This is not a fully shared multi-tenant data plane.

## Why This Is The Right Pre-V1 Choice

1. It enables fast go-to-market with one codebase and one standard template.
2. It avoids high-risk cross-tenant data paths in application code.
3. It naturally limits noisy-neighbor blast radius to one customer cell.
4. It keeps the path open for later optimization once usage patterns are known.

## Isolation Boundaries (What Is Shared vs Isolated)

Shared across customers:

- source code and build artifact
- CI/CD pipeline and deployment templates
- operator dashboards and alerts at aggregate level
- control-plane metadata (customer inventory, rollout status)

Isolated per customer cell:

- DB Monitor application deployment
- monitor Postgres database (prefer dedicated server or dedicated DB + creds)
- consumer group and topic/queue namespace
- DLQ records and replay operations
- retention archive storage path/container
- API keys, JWT secrets, webhook secrets
- customer-facing endpoints/tokens

## Recommended Azure/Kubernetes Layout

## Control Plane

- One control-plane service (internal) for customer lifecycle actions:
  - create customer cell
  - rotate secrets
  - trigger upgrade per customer
  - suspend/resume cell
- One template set (Bicep/Terraform/Helm) used for every customer cell.

## Customer Cell (Per Customer)

- Kubernetes namespace per customer: `dbm-<customer_slug>`
- Deployment(s):
  - `db-monitor-api-<customer_slug>`
  - `db-monitor-consumer-<customer_slug>`
- Dedicated secret scope for each namespace.
- NetworkPolicy:
  - deny-all default
  - allow only required egress (broker, Postgres, webhook destinations)
  - allow ingress only from approved gateway paths
- Resource controls:
  - namespace `ResourceQuota`
  - per-pod requests/limits
  - HPA bounds per customer

## Data Plane Isolation

- Postgres:
  - pre-v1 preferred: separate Postgres instance per customer tier
  - acceptable fallback: one Postgres server with one DB per customer and
    unique least-privilege DB user per DB
- Messaging:
  - Kafka topic prefix per customer and unique consumer group
  - RabbitMQ vhost or queue namespace per customer
- Storage:
  - separate blob container/path prefix per customer for archives and exports

## Security Baseline

Required pre-v1 controls:

1. unique signing secrets and API keys per customer
2. strict customer-scoped credentials (no shared admin credentials)
3. operator APIs require customer-id for every mutating operation
4. no cross-namespace service account permissions
5. encrypted secrets at rest and secret rotation workflow

## Minimum Customer Onboarding Flow

1. Register customer record in control plane.
2. Generate customer slug and namespace.
3. Provision Postgres target and credentials for the customer.
4. Provision customer-scoped broker namespace (topic prefix/vhost/queues).
5. Create namespace secrets and config from standard template.
6. Deploy DB Monitor API + consumer into customer namespace.
7. Run migrations and health/readiness checks.
8. Bootstrap admin key, deliver credentials, and mark customer as active.

Expected onboarding SLO target for pre-v1: < 15 minutes automated.

## Operational Guardrails For Pre-V1

1. rollout policy: canary one customer, then small cohort
2. per-customer SLO policy published through `/admin/slo-policy`
3. alerting includes customer identity in every incident payload
4. emergency isolation: suspend one customer cell without impacting others

## What To Defer Until Post-V1

- true shared multi-tenant data plane with strong tenant-scoped authz
- large-scale pooled worker scheduling across customers
- shared database with row-level security as primary tenant boundary

## Exit Criteria To Revisit Architecture

Re-evaluate a deeper shared-plane model only when all are true:

1. onboarding and upgrades are fully automated
2. customer cell cost profile is measured and predictable
3. per-customer SLO attainment is stable over multiple release cycles
4. data isolation controls pass security review and penetration testing
