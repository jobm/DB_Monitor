-- Migration: 0016_api_audit_log_spill_unreplayed_index
-- Adds a partial index for fast unreplayed spill lookups and
-- a configurable retention cleanup targetting old replayed rows.

CREATE INDEX IF NOT EXISTS ix_api_audit_log_spill_unreplayed
    ON api_audit_log_spill (spilled_at)
    WHERE replayed = FALSE;
