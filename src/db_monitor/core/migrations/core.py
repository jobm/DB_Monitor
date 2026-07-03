"""Migration framework, dataclass, advisory-lock coordination."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)

SCHEMA_MIGRATIONS_TABLE = "schema_migrations"


@dataclass(frozen=True)
class Migration:
    """Single tracked migration step."""

    version: str
    description: str
    apply: Callable[[AsyncConnection, Callable], Any]


_MIGRATION_LOCK_ID = 7273646  # "dbmonitor" on a phone keypad


# ── PostgreSQL type normalisation map ──────────────────────────────────
# Maps SQLAlchemy type repr prefixes to information_schema equivalents.
_SQLA_TO_PG_TYPE: dict[str, str] = {
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "SMALLINT": "smallint",
    "VARCHAR": "character varying",
    "TEXT": "text",
    "BOOLEAN": "boolean",
    "JSONB": "jsonb",
    "TIMESTAMP": "timestamp with time zone",
    "DATE": "date",
    "NUMERIC": "numeric",
    "FLOAT": "double precision",
    "REAL": "real",
    "UUID": "uuid",
    "SERIAL": "integer",  # SERIAL is syntactic sugar; info_schema reports integer
    "BYTEA": "bytea",
}


def _normalise_sqla_type(raw: str) -> str:
    """Normalise a SQLAlchemy ``str(type)`` into the form reported by
    ``information_schema.columns.data_type``."""
    upper = raw.upper().split("(")[0].split()[0]
    return _SQLA_TO_PG_TYPE.get(upper, raw.lower())


# ── Internal helpers ──────────────────────────────────────────────────


async def _ensure_migration_table(connection: AsyncConnection) -> None:
    """Create the migration tracking table when it is missing."""
    await connection.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
                version VARCHAR(64) PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )


async def _get_applied_versions(connection: AsyncConnection) -> set[str]:
    """Return the set of tracked migration versions already applied."""
    result = await connection.execute(
        text(f"SELECT version FROM {SCHEMA_MIGRATIONS_TABLE} ORDER BY version")
    )
    return {row[0] for row in result.fetchall()}


async def _record_migration(
    connection: AsyncConnection,
    migration: Migration,
) -> None:
    """Persist a successful migration application in the tracking table."""
    await connection.execute(
        text(
            f"""
            INSERT INTO {SCHEMA_MIGRATIONS_TABLE} (version, description)
            VALUES (:version, :description)
            ON CONFLICT (version) DO NOTHING
            """
        ),
        {
            "version": migration.version,
            "description": migration.description,
        },
    )


async def _acquire_migration_lock(
    connection: AsyncConnection,
) -> bool:
    """Try to acquire the migration advisory lock. Returns True on success."""
    result = await connection.execute(
        text("SELECT pg_try_advisory_lock(:lock_id)"),
        {"lock_id": _MIGRATION_LOCK_ID},
    )
    return bool(result.scalar())


async def _release_migration_lock(
    connection: AsyncConnection,
) -> None:
    """Release the migration advisory lock."""
    await connection.execute(
        text("SELECT pg_advisory_unlock(:lock_id)"),
        {"lock_id": _MIGRATION_LOCK_ID},
    )


# ── Schema drift detection ────────────────────────────────────────────

_TABLES_IGNORED_FOR_DRIFT = {
    "schema_migrations",
    "spatial_ref_sys",
    "geography_columns",
    "geometry_columns",
}


async def _detect_schema_drift(
    connection: AsyncConnection,
) -> list[str]:
    """Compare the live database schema against the ORM model definitions
    and return a list of human-readable drift issues.

    Checks cover:
    - Missing tables
    - Missing columns on existing tables
    - Column type mismatches
    - Nullable mismatches
    - Unexpected tables (non-ORM, non-system)
    """
    # Lazy import to avoid circular dependency at module level.
    from db_monitor.core.models import Base

    issues: list[str] = []

    # ── Gather expected schema from SQLAlchemy metadata ──────────────
    expected_tables: dict[str, dict[str, dict[str, Any]]] = {}
    for table_name, table in Base.metadata.tables.items():
        cols: dict[str, dict[str, Any]] = {}
        for col in table.columns:
            cols[col.name] = {
                "type": _normalise_sqla_type(str(col.type)),
                "nullable": col.nullable,
            }
        expected_tables[table_name] = cols

    # ── Query actual tables from information_schema ──────────────────
    result = await connection.execute(
        text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            """
        )
    )
    actual_table_names: set[str] = {row[0] for row in result.fetchall()}

    # ── Check for missing tables ─────────────────────────────────────
    for tname in expected_tables:
        if tname not in actual_table_names:
            issues.append(f"Missing table: {tname}")

    # ── Check for unexpected tables ──────────────────────────────────
    expected_table_names = set(expected_tables)
    for tname in sorted(actual_table_names):
        if tname not in expected_table_names and tname not in _TABLES_IGNORED_FOR_DRIFT:
            issues.append(f"Unexpected table: {tname}")

    # ── Check columns for each expected table ────────────────────────
    for tname, expected_cols in expected_tables.items():
        if tname not in actual_table_names:
            continue

        col_result = await connection.execute(
            text(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :tname
                """
            ),
            {"tname": tname},
        )
        actual_cols: dict[str, dict[str, Any]] = {}
        for row in col_result.fetchall():
            actual_cols[row[0]] = {
                "type": row[1],
                "nullable": row[2] == "YES",
                # actual default is not checked for drift
            }

        for col_name, col_info in expected_cols.items():
            if col_name not in actual_cols:
                issues.append(f"Missing column: {tname}.{col_name}")
                continue

            actual_type = actual_cols[col_name]["type"]
            expected_type = col_info["type"]

            # Type comparison (allow common aliases)
            if actual_type != expected_type:
                # SERIAL columns report as integer in info_schema
                if expected_type == "integer" and actual_type in (
                    "integer",
                    "bigint",
                    "smallint",
                ):
                    pass  # compatible integer types
                elif expected_type == "character varying" and actual_type.startswith(
                    "character varying"
                ):
                    pass  # compatible varchar with different length
                else:
                    issues.append(
                        f"Type mismatch: {tname}.{col_name}: "
                        f"expected {expected_type}, got {actual_type}"
                    )

            # Nullable check
            expected_nullable = col_info["nullable"]
            actual_nullable = actual_cols[col_name]["nullable"]
            if expected_nullable != actual_nullable:
                issues.append(
                    f"Nullable mismatch: {tname}.{col_name}: "
                    f"expected nullable={expected_nullable}, "
                    f"got nullable={actual_nullable}"
                )

    return issues


# ── Public API ────────────────────────────────────────────────────────


async def get_pending_migrations(
    engine: AsyncEngine,
    migrations: tuple[Migration, ...],
) -> list[Migration]:
    """Return the ordered list of migrations that have not been applied."""
    async with engine.begin() as connection:
        await _ensure_migration_table(connection)
        applied_versions = await _get_applied_versions(connection)

    return [
        migration
        for migration in migrations
        if migration.version not in applied_versions
    ]


async def apply_migrations(
    session_factory: Callable,
    engine: AsyncEngine,
    migrations: tuple[Migration, ...],
) -> list[str]:
    """Apply all pending migrations and return their versions.

    Acquires a PostgreSQL advisory lock before running any migration
    steps. When the lock is held by another process this function logs a
    clear message and raises ``RuntimeError`` so callers can retry or
    exit cleanly.
    """
    applied_versions: list[str] = []

    async with engine.connect() as connection:
        await _ensure_migration_table(connection)
        await connection.commit()

        if not await _acquire_migration_lock(connection):
            raise RuntimeError(
                "Migration lock is held by another process. "
                "Only one actor can run schema changes at a time. "
                "Retry once the other migration completes."
            )
        await connection.commit()

        try:
            async with connection.begin():
                pending = [
                    m
                    for m in migrations
                    if m.version
                    not in await _get_applied_versions(connection)
                ]

                for migration in pending:
                    await migration.apply(connection, session_factory)
                    await _record_migration(connection, migration)
                    applied_versions.append(migration.version)
        finally:
            await _release_migration_lock(connection)

    return applied_versions


async def validate_migrations(
    engine: AsyncEngine,
    migrations: tuple[Migration, ...],
) -> None:
    """Raise when the database is missing required tracked migrations
    or when the live schema has drifted from the ORM model definitions.

    Runs two validation passes:
    1. Checks that all migration versions are tracked in the database.
    2. Queries ``information_schema`` and compares every table and column
       against ``Base.metadata``, reporting any drift.
    """
    pending = await get_pending_migrations(engine, migrations)
    if pending:
        pending_versions = ", ".join(
            migration.version for migration in pending
        )
        raise RuntimeError(
            "Database schema is not up to date. "
            "Run the migration command before starting the app. "
            f"Pending migrations: {pending_versions}"
        )

    async with engine.connect() as connection:
        drift_issues = await _detect_schema_drift(connection)

    if drift_issues:
        summary = "\n  - ".join([""] + drift_issues)
        raise RuntimeError(
            f"Schema drift detected ({len(drift_issues)} issues):{summary}"
        )
