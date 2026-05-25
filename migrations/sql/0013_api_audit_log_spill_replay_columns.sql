-- Migration: 0013_api_audit_log_spill_replay_columns
-- Adds replay metadata to api_audit_log_spill

ALTER TABLE api_audit_log_spill
    ADD COLUMN IF NOT EXISTS replayed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS replayed_by VARCHAR(128) NULL,
    ADD COLUMN IF NOT EXISTS replayed_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS ix_api_audit_log_spill_replayed_at ON api_audit_log_spill (replayed_at);
