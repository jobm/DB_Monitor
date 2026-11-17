-- Migration: 0018_high_cardinality_indexes
-- Adds targeted indexes for high-cardinality filter patterns identified
-- during the storage scaling review (RR-1).
--
-- Motivations:
--   events: Tenant-scoped time-range queries (service_name + event_time
--   range) are common after OS-7, but the existing cursor index has
--   event_type in column 2, blocking efficient index scans.
--
--   events: Tenant-scoped source_table_id lookups need the index to
--   start with service_name so the tenant filter is applied first.
--
--   column_changes: The existing ix_changes_table_col_time lacks id,
--   causing keyset pagination on column-scoped queries to fall back to
--   sequential scans.
--
--   column_changes: Row-identity equality + ordering queries are not
--   covered by the GIN index (designed for JSONB containment).

CREATE INDEX IF NOT EXISTS ix_events_service_time_capture
    ON events (service_name, event_time DESC, capture_time DESC, id DESC);

CREATE INDEX IF NOT EXISTS ix_events_service_source_capture
    ON events (service_name, source_table_id, capture_time DESC, id DESC);

CREATE INDEX IF NOT EXISTS ix_changes_table_column_time_id
    ON column_changes (table_id, column_id, changed_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS ix_changes_table_identity_time
    ON column_changes (table_id, row_identity, changed_at DESC, id DESC);
