# ADR-006: Multi-Tenancy Isolation via API Key Tenant Scope

**Status:** Implemented
**Date:** 2026-07-05
**Deciders:** Project leads
**Context:** [Auth and session settings](../configuration.md#auth-and-session-settings)

---

## Decision

Tenant isolation is enforced at the **API key level** using nullable `tenant_id` on the `ApiKey` model:

- **NULL tenant_id** — Admin key with unrestricted access to all tenants' data.
- **Non-NULL tenant_id** — Scoped key restricted to a single tenant's data.

## Design

### Data Model

```text
api_keys
├── id, key_hash, owner_name, role, is_active, expires_at, ...
└── tenant_id VARCHAR(128) NULL
```

The `tenant_id` column on `api_keys` is the single source of truth for tenant scoping. No per-table tenant columns are added.

### Enforcement Layers

| Layer | Mechanism |
|-------|-----------|
| **Auth dependency** | `get_tenant_filter()` returns the API key's `tenant_id` (or `None` for admin keys) |
| **Route handler** | Route-level 403 check: if `tenant_filter` is set and the user-supplied `service_name` differs, access is denied |
| **Query filter** | Events and changes queries use `effective_service_name = tenant_filter or service_name` to scope results |
| **Table listing** | In-memory filter on `service_name` for the `/tables` endpoints |

### Flow

```
Client request (with API key)
  → auth resolves the key → gets tenant_id
  → if tenant_id is set → apply as forced service_name filter
  → if admin (tenant_id is NULL) → allow any service_name
  → data queries limit results by effective service_name
```

### Tenant Identifier

The **service_name** field on events, monitored_tables, and dead_letter_events serves as the natural tenant identifier. This avoids modifying the event data model — the same `service_name` that already partitions CDC event data is used for tenant isolation.

## Tradeoffs Considered

| Alternative | Reason Not Chosen |
|-------------|-------------------|
| **Separate tenant table + FK** | Adds complexity without benefit — `service_name` already exists as a partition key on all data tables |
| **Schema-level isolation (separate DB per tenant)** | Impractical for shared-monitor deployment; eliminates shared-admin visibility |
| **Row-level security (RLS) policies** | Valuable as defense-in-depth but adds operational complexity; app-level enforcement is sufficient for v1 |
| **JWT token claims for tenant** | API keys are the primary auth mechanism; adding tenant to JWT would be secondary |

## Security Properties

1. **Leak prevention**: A tenant-scoped key cannot list, query, or access data from other tenants even with a forged `service_name` parameter.
2. **Admin bypass**: Admin keys (NULL tenant_id) can access all tenants for operational support.
3. **Default deny**: A newly created key has no tenant scope unless explicitly assigned during provisioning.

## Future Considerations

- **Postgres RLS**: Can be added as defense-in-depth by setting session-level `app.tenant_id` from the auth dependency and creating RLS policies on `events`, `column_changes`, and `monitored_tables`.
- **Per-tenant quotas**: Already supported via `INGESTION_TENANT_DEFAULT_EVENTS_PER_WINDOW` and `INGESTION_TENANT_QUOTAS` env vars (OS-5).
- **Per-tenant API key management**: Admin routes for listing/revoking keys scoped to a specific tenant could be added as a phase-2 enhancement.
