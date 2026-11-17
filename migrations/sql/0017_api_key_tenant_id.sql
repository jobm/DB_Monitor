-- Migration: 0017_api_key_tenant_id
-- Adds tenant_id column to api_keys for multi-tenant isolation.
-- NULL tenant_id = admin key with access to all tenants.
-- Non-NULL tenant_id = scoped key restricted to that tenant.

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(128) NULL;

CREATE INDEX IF NOT EXISTS ix_api_keys_tenant_id ON api_keys (tenant_id);
