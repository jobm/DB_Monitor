-- Migration: 0012_api_audit_log_spill
-- Creates the durable spill table for persisted audit log failures.

CREATE TABLE IF NOT EXISTS api_audit_log_spill (
    id SERIAL PRIMARY KEY,
    entry JSONB NOT NULL,
    error_message TEXT NULL,
    spilled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_api_audit_log_spill_spilled_at ON api_audit_log_spill (spilled_at);
