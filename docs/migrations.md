# Database Migrations

DB Monitor uses a **custom Python-based migration framework** (no Alembic) to
manage schema changes. Every DDL operation is a versioned, tracked migration
step that can be applied, validated, and reviewed.

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `db-monitor-migrate apply` | Apply all pending migrations |
| `db-monitor-migrate validate` | Check that all migrations are applied and schema matches models |
| `make monitor-migrate` | Shorthand for `uv run python -m db_monitor.cli migrate apply` |

---

## How It Works

### Migration Tracking Table

The framework uses a `schema_migrations` table to track which migrations have
been applied:

```sql
CREATE TABLE schema_migrations (
    version    VARCHAR(64) PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Advisory Lock

Migrations run under a PostgreSQL **advisory lock** (`lock_id = 7273646`,
mnemonic: "dbmonitor" on a phone keypad) to prevent concurrent migration
runs. If a second process tries to apply migrations while the lock is held,
it receives a clear `RuntimeError` and must retry.

### Migration Lifecycle

1. Start with a fresh (or existing) database.
2. Run `db-monitor-migrate apply`.
3. The framework:
   - Creates the `schema_migrations` tracking table if missing.
   - Acquires the advisory lock (`pg_try_advisory_lock`).
   - Queries `schema_migrations` for already-applied versions.
   - For each **pending** migration (in version order), calls its `apply()`
     function and records the version.
   - Releases the advisory lock.
4. On subsequent runs, only **new** migrations are applied.

### Validation (`db-monitor-migrate validate`)

Runs two checks:

1. **Pending-migration check** — ensures every migration in the `MIGRATIONS`
   tuple has a corresponding row in `schema_migrations`.
2. **Schema-drift check** — queries `information_schema` and compares every
   table and column against the SQLAlchemy ORM model definitions
   (`Base.metadata`). Reports any drift: missing tables, missing columns,
   type mismatches, nullable mismatches, and unexpected tables.

---

## Creating a New Migration

### Step 1: Write the apply function

Create a new async function in the appropriate module under
`src/db_monitor/core/migrations/`:

```python
# In src/db_monitor/core/migrations/schema.py  (or auth.py, infra.py, etc.)

from collections.abc import Callable
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def _apply_my_new_feature(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    \"\"\"Add my_new_feature support to the schema.\"\"\"
    del session_factory
    statements = [
        "ALTER TABLE my_table ADD COLUMN IF NOT EXISTS new_col VARCHAR(64)",
        "CREATE INDEX IF NOT EXISTS ix_my_table_new_col ON my_table (new_col)",
    ]
    for statement in statements:
        await connection.execute(text(statement))
```

**Rules:**

- The function signature **must** be
  `async def (connection: AsyncConnection, session_factory: Callable) -> None`.
  Use `del session_factory` if the migration only needs DDL.
- Use `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`,
  and `CREATE INDEX IF NOT EXISTS` so all steps are **idempotent**.
- Every DDL statement must be executable within a single transaction.
- Do **not** use `Base.metadata.create_all` — all DDL must be explicit and
  versioned.

### Step 2: Register the migration

Add a new entry to the `MIGRATIONS` tuple in
`src/db_monitor/core/migrations/__init__.py`:

```python
from db_monitor.core.migrations.schema import _apply_my_new_feature

MIGRATIONS: tuple[Migration, ...] = (
    # ... existing migrations ...
    Migration(
        version="0016_my_new_feature",
        description="Add my_new_feature support",
        apply=_apply_my_new_feature,
    ),
)
```

And update `CURRENT_SCHEMA_VERSION` if this is the new latest migration
(which it will be unless you're inserting an interleaved fix).

### Step 3: Update the `__all__` export (if applicable)

If your function is referenced externally (e.g., from tests or the CLI), add
it to `__all__` in `__init__.py`.

### Step 4: Run and verify

```bash
# Apply the migration
db-monitor-migrate apply

# Validate no drift
db-monitor-migrate validate
```

---

## Migration Naming Convention

Use the format: `NNNN_description_with_underscores`

- `NNNN` — zero-padded four-digit sequence: `0001`, `0002`, …, `0015`, `0016`.
- `description_with_underscores` — short, snake_case description of the change.

Examples:

| Version | Description |
|---------|-------------|
| `0001_base_schema` | Create the baseline DB Monitor schema |
| `0013_event_partitioning` | Convert events and column_changes to time-range partitioned tables |
| `0015_events_unique_constraint` | Replace unique index with unique constraint on partitioned events |

---

## Review Checklist for Migration PRs

- [ ] **Idempotent DDL** — all statements use `IF NOT EXISTS` guards.
- [ ] **Version bump** — version is the next number in `NNNN` sequence.
- [ ] **No `create_all`** — no call to `Base.metadata.create_all` anywhere.
- [ ] **Tests pass** — `make monitor-lint` and `make monitor-test-core` pass.
- [ ] **Validate passes** — `db-monitor-migrate validate` reports zero drift.
- [ ] **Rollback considered** — if the migration could fail mid-way, the
      statements should be re-orderable or the change should be additive
      (adding columns/indexes, not removing them).
- [ ] **Documentation** — if the migration changes a public contract or
      configuration, update `docs/configuration.md` or `docs/api-docs.md`.

---

## Migration File Layout

All migration files live under `src/db_monitor/core/migrations/`:

| File | Purpose |
|------|---------|
| `__init__.py` | `MIGRATIONS` tuple, re-exports, `CURRENT_SCHEMA_VERSION` |
| `core.py` | Framework: `Migration` dataclass, lock coordination, `apply_migrations()`, `validate_migrations()`, drift detection |
| `schema.py` | Base-schema DDL, event ingestion columns, backfill, offset widening, row identity, unique constraint |
| `auth.py` | API key lifecycle, customer JWT secrets, control-plane state |
| `infra.py` | Recovery tables, cursor indexes, quota windows, audit spill |
| `partitioning.py` | Event partitioning conversion |
| `partition_ops.py` | Monthly partition creation and drop-old-partition helpers |
| `legacy.py` | Legacy Kafka event migration helpers |
| `recovery_migration.py` | Broker-neutral recovery metadata |

---

## Schema Drift Detection

The `validate_migrations()` function in `core.py` detects:

- **Missing tables** — tables present in `Base.metadata` but absent from the
  database.
- **Missing columns** — columns present in `Base.metadata` but absent from a
  table's actual definition.
- **Type mismatches** — column types that differ between the ORM and the live
  schema.
- **Nullable mismatches** — columns whose nullable flag differs.
- **Unexpected tables** — tables in the `public` schema that are not tracked by
  the ORM (excluding system tables like `schema_migrations`,
  `spatial_ref_sys`).

Drift issues are reported as a `RuntimeError` with a summary of every
difference found.

---

## Startup Integration

The application startup (`extensions.py` → `init_db()`) uses the
`db_schema_mode` setting to decide whether to **apply** or **validate**
migrations:

- `apply` (default in dev) — runs `apply_migrations()` to catch up.
- `validate` (default in prod) — runs `validate_migrations()` and raises if
  anything is out of date.
- `skip` — does nothing (for read-replica or ephemeral environments).
