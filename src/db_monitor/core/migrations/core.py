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
    """Raise when the database is missing required tracked migrations."""
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
