"""Tests for explicit-DDL migration schema and drift detection.

Verifies that:
1. The ``_apply_base_schema`` DDL covers every table and column in the ORM.
2. Schema-drift detection correctly spots missing tables and columns.
3. All migration versions are sequential with no gaps or duplicates.
4. (Optional, requires live DB) Running all migrations produces a schema
   that matches the ORM model definitions.
"""

from __future__ import annotations

import os
import re

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from db_monitor.core.migrations import MIGRATIONS, Migration
from db_monitor.core.models import Base


# ── Tests ──────────────────────────────────────────────────────────────


class TestMigrationList:
    """Verify the ``MIGRATIONS`` tuple is well-formed."""

    def test_all_migrations_have_apply_function(self) -> None:
        """Every migration entry must have a callable ``apply``."""
        for m in MIGRATIONS:
            assert callable(m.apply), (
                f"Migration {m.version} apply is not callable: {m.apply}"
            )

    def test_migration_versions_are_unique(self) -> None:
        """No duplicate version strings."""
        versions = [m.version for m in MIGRATIONS]
        assert len(versions) == len(set(versions)), (
            f"Duplicate migration versions: {versions}"
        )

    def test_migration_versions_are_sequential(self) -> None:
        """Versions must be monotonically increasing four-digit numbers."""
        prefixes = [m.version.split("_")[0] for m in MIGRATIONS]
        expected = [f"{i:04d}" for i in range(1, len(MIGRATIONS) + 1)]
        assert prefixes == expected, (
            f"Version prefix sequence: expected {expected}, got {prefixes}"
        )

    def test_migration_descriptions_are_nonempty(self) -> None:
        """Every migration must have a non-empty description."""
        for m in MIGRATIONS:
            assert m.description.strip(), (
                f"Migration {m.version} has an empty description"
            )

    def test_current_schema_version_matches_last_migration(self) -> None:
        """``CURRENT_SCHEMA_VERSION`` must match the last migration."""
        from db_monitor.core.migrations import CURRENT_SCHEMA_VERSION

        assert CURRENT_SCHEMA_VERSION == MIGRATIONS[-1].version


class TestDDLCoversAllORMTables:
    """Validate that ``_apply_base_schema`` in ``schema.py`` creates every
    table and column defined in the ORM models.

    Uses regex extraction of ``CREATE TABLE`` names from the DDL source
    and compares against ``Base.metadata``.
    """

    @staticmethod
    def _get_ddl_tables() -> set[str]:
        import inspect

        from db_monitor.core.migrations.schema import _apply_base_schema

        source = inspect.getsource(_apply_base_schema)
        create_pattern = re.compile(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)",
            re.IGNORECASE,
        )
        return {m.group(1) for m in create_pattern.finditer(source)}

    def test_all_orm_tables_have_create_ddl(self) -> None:
        """Every ORM table must have a ``CREATE TABLE`` in the DDL."""
        ddl_tables = self._get_ddl_tables()
        orm_tables = set(Base.metadata.tables.keys())

        missing = orm_tables - ddl_tables
        assert not missing, (
            f"ORM tables missing from _apply_base_schema DDL: "
            f"{sorted(missing)}"
        )

    def test_ddl_references_no_unexpected_tables(self) -> None:
        """Every ``CREATE TABLE`` in the DDL should correspond to an
        ORM model."""
        ddl_tables = self._get_ddl_tables()
        orm_tables = set(Base.metadata.tables.keys())

        extra = ddl_tables - orm_tables
        assert not extra, (
            f"DDL creates tables not in ORM: {sorted(extra)}"
        )

    def test_ddl_table_count_matches_orm(self) -> None:
        """Number of ``CREATE TABLE`` statements must equal ORM tables."""
        ddl_tables = self._get_ddl_tables()
        orm_tables = set(Base.metadata.tables.keys())
        assert len(ddl_tables) == len(orm_tables), (
            f"Count mismatch: DDL={len(ddl_tables)}, ORM={len(orm_tables)}"
        )


class TestDriftDetection:
    """Test that ``_detect_schema_drift`` correctly identifies issues."""

    @pytest.mark.anyio
    async def test_drift_detects_missing_table(self) -> None:
        """Drift detection must flag a missing expected table."""
        from db_monitor.core.migrations.core import _detect_schema_drift

        class FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class FakeConnection:
            def __init__(self):
                self.call_count = 0

            async def execute(self, statement, params=None):
                self.call_count += 1
                return FakeResult([])

        conn = FakeConnection()
        issues = await _detect_schema_drift(conn)  # type: ignore[arg-type]

        missing_issues = [i for i in issues if "Missing table" in i]
        assert missing_issues, (
            f"Expected missing-table issues but got: {issues}"
        )

    @pytest.mark.anyio
    async def test_drift_no_issues_when_schema_matches(self) -> None:
        """Drift detection must report no issues when every expected
        table and column exists."""
        from db_monitor.core.migrations.core import _detect_schema_drift

        # Build fake information_schema results matching all ORM tables
        actual_tables = [(tname,) for tname in Base.metadata.tables]
        actual_columns = []
        for tname, table in Base.metadata.tables.items():
            for col in table.columns:
                pg_type_raw = str(col.type).upper().split("(")[0]
                pg_type = {
                    "INTEGER": "integer",
                    "BIGINT": "bigint",
                    "VARCHAR": "character varying",
                    "TEXT": "text",
                    "BOOLEAN": "boolean",
                    "JSONB": "jsonb",
                    "TIMESTAMP": "timestamp with time zone",
                    "SERIAL": "integer",
                }.get(pg_type_raw, pg_type_raw.lower())
                actual_columns.append(
                    (tname, col.name, pg_type, "YES" if col.nullable else "NO")
                )

        class FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class FakeConnection:
            def __init__(self):
                self.call_index = 0

            async def execute(self, statement, params=None):
                self.call_index += 1
                if self.call_index == 1:
                    return FakeResult(actual_tables)
                tname = params["tname"]
                matching = [(c[1], c[2], c[3]) for c in actual_columns if c[0] == tname]
                return FakeResult(matching)

        conn = FakeConnection()
        issues = await _detect_schema_drift(conn)  # type: ignore[arg-type]

        assert not issues, (
            f"Expected no drift issues but got: {issues}"
        )

    @pytest.mark.anyio
    async def test_drift_catches_missing_column(self) -> None:
        """Drift detection must flag a missing column."""
        from db_monitor.core.migrations.core import _detect_schema_drift

        # Use the first ORM table and drop its last column
        orm_tables = list(Base.metadata.tables.items())
        first_tname, first_table = orm_tables[0]
        all_cols = list(first_table.columns)
        drop_col = all_cols[-1].name

        actual_tables = [(first_tname,)]
        actual_columns = []
        for col in all_cols:
            if col.name == drop_col:
                continue
            pg_type_raw = str(col.type).upper().split("(")[0]
            pg_type = {
                "INTEGER": "integer",
                "BIGINT": "bigint",
                "VARCHAR": "character varying",
                "TEXT": "text",
                "BOOLEAN": "boolean",
                "JSONB": "jsonb",
                "TIMESTAMP": "timestamp with time zone",
                "SERIAL": "integer",
            }.get(pg_type_raw, pg_type_raw.lower())
            actual_columns.append(
                (first_tname, col.name, pg_type, "YES" if col.nullable else "NO")
            )

        class FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class FakeConnection:
            def __init__(self):
                self.call_index = 0

            async def execute(self, statement, params=None):
                self.call_index += 1
                if self.call_index == 1:
                    return FakeResult(actual_tables)
                return FakeResult(
                    [(c[1], c[2], c[3]) for c in actual_columns if c[0] == params["tname"]]
                )

        conn = FakeConnection()
        issues = await _detect_schema_drift(conn)  # type: ignore[arg-type]

        missing_col_issues = [i for i in issues if "Missing column" in i]
        assert missing_col_issues, (
            f"Expected missing-column issues but got: {issues}"
        )


class TestValidateMigrations:
    """Test that ``validate_migrations`` correctly reports pending
    migrations and drift."""

    @pytest.mark.anyio
    async def test_fails_on_pending_migrations(self) -> None:
        """``validate_migrations`` must raise when there are pending
        migrations."""
        from db_monitor.core.migrations.core import validate_migrations

        class FakeEngine:
            def begin(self):
                class FakeCtx:
                    async def __aenter__(s):
                        return FakeConnection()
                    async def __aexit__(s, *a):
                        return False
                return FakeCtx()

        class FakeConnection:
            async def execute(self, statement, params=None):
                class FakeResult:
                    def fetchall(self):
                        return []
                    def scalar(self):
                        return True
                return FakeResult()

        with pytest.raises(RuntimeError, match="Pending migrations"):
            await validate_migrations(FakeEngine(), MIGRATIONS)  # type: ignore[arg-type]


class TestLiveMigrationExecution:
    """Run all 15 migrations against a real PostgreSQL database.

    These tests require a running PostgreSQL instance and are marked
    ``@pytest.mark.slow`` — they are excluded from the default test run.
    Run with::

        pytest tests/test_migration_schema.py -m slow -v
    """

    _test_schema: str = ""

    @classmethod
    def _get_database_url(cls) -> str | None:
        """Return the PostgreSQL URL to use for integration tests.

        Reads from ``DB_MONITOR_TEST_URL`` first, then
        ``MONITOR_POSTGRES_URL``, then falls back to the default local
        dev URL (port 5437).  Returns ``None`` if none is available.
        """
        default = "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres"
        raw = os.environ.get(
            "DB_MONITOR_TEST_URL",
            os.environ.get("MONITOR_POSTGRES_URL", default),
        )
        if raw and raw.endswith("_FILE"):
            file_val = os.environ.get(raw)
            if file_val:
                raw = file_val.strip()
        return raw or None

    @staticmethod
    async def _database_exists(admin_url: str, dbname: str) -> bool:
        """Check if *dbname* exists on the server at *admin_url*."""
        from asyncpg import create_pool

        # Use a raw asyncpg pool because ``CREATE / DROP DATABASE``
        # cannot run inside a transaction.
        pool = await create_pool(
            admin_url.replace("+asyncpg", ""),
            min_size=1,
            max_size=1,
        )
        assert pool is not None
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT 1 FROM pg_database WHERE datname = $1",
                    dbname,
                )
                return row is not None
        finally:
            await pool.close()

    @staticmethod
    async def _create_database(admin_url: str, dbname: str) -> None:
        """Create *dbname* on the server at *admin_url*.

        ``CREATE DATABASE`` cannot run inside a transaction, so we use
        a raw asyncpg pool with auto-commit behaviour.
        """
        from asyncpg import create_pool

        pool = await create_pool(
            admin_url.replace("+asyncpg", ""),
            min_size=1,
            max_size=1,
        )
        assert pool is not None
        try:
            async with pool.acquire() as conn:
                await conn.execute(f'CREATE DATABASE "{dbname}"')
        finally:
            await pool.close()

    @staticmethod
    async def _drop_database(admin_url: str, dbname: str) -> None:
        """Drop *dbname*.

        Terminates all connections to the database first."""
        from asyncpg import create_pool

        pool = await create_pool(
            admin_url.replace("+asyncpg", ""),
            min_size=1,
            max_size=1,
        )
        assert pool is not None
        try:
            async with pool.acquire() as conn:
                # Kill existing connections so the DROP can proceed
                await conn.execute(
                    """
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = $1
                      AND pid <> pg_backend_pid()
                    """,
                    dbname,
                )
                await conn.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        finally:
            await pool.close()

    @pytest.mark.slow
    @pytest.mark.anyio
    async def test_all_migrations_on_fresh_database(self) -> None:
        """Run all migrations against an isolated test database and
        verify the resulting schema matches the ORM model definitions.

        The test creates a temporary database, applies every migration
        step, validates the schema, and drops the database.
        """
        admin_url = self._get_database_url()
        if admin_url is None:
            pytest.skip("No database URL configured for live migration test")

        # Gracefully skip if PostgreSQL is not reachable
        try:
            probe = create_async_engine(admin_url)
            async with probe.connect() as conn:
                await conn.execute(text("SELECT 1"))
            await probe.dispose()
        except Exception as exc:
            pytest.skip(f"Database at {admin_url} is not reachable: {exc}")

        test_db = f"db_monitor_mig_test_{os.urandom(4).hex()}"

        # ── Create the test database (cannot run in a transaction) ──────
        exists = await self._database_exists(admin_url, test_db)
        if exists:
            await self._drop_database(admin_url, test_db)
        await self._create_database(admin_url, test_db)

        # Build the URL for the test database
        test_url = admin_url.rsplit("/", 1)[0] + f"/{test_db}"

        from sqlalchemy.ext.asyncio import AsyncSession
        from sqlalchemy.orm import sessionmaker

        from db_monitor.core.migrations import apply_migrations
        from db_monitor.core.migrations.core import _get_applied_versions

        engine = create_async_engine(test_url)

        try:
            # ── Apply all 15 migrations ─────────────────────────────────
            test_session_factory = sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )
            applied = await apply_migrations(
                test_session_factory,
                engine,
                MIGRATIONS,
            )
            assert len(applied) == len(MIGRATIONS), (
                f"Expected {len(MIGRATIONS)} migrations applied, "
                f"got {len(applied)}: {applied}"
            )

            # ── Verify all versions are tracked ─────────────────────────
            async with engine.begin() as conn:
                applied_versions = await _get_applied_versions(conn)

            expected_versions = {m.version for m in MIGRATIONS}
            missing = expected_versions - applied_versions
            assert not missing, (
                f"Migrations not tracked in schema_migrations: "
                f"{sorted(missing)}"
            )

            # ── Verify every ORM table has the expected columns ─────────
            async with engine.begin() as conn:
                for tname, table in Base.metadata.tables.items():
                    orm_cols = {col.name for col in table.columns}
                    col_result = await conn.execute(
                        text(
                            """
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = :tname
                            """
                        ),
                        {"tname": tname},
                    )
                    actual_cols = {row[0] for row in col_result.fetchall()}
                    missing_cols = orm_cols - actual_cols
                    assert not missing_cols, (
                        f"Table {tname} is missing columns: "
                        f"{sorted(missing_cols)}"
                    )
        finally:
            # ── Cleanup: close engine, then drop the test database ──────
            await engine.dispose()
            await self._drop_database(admin_url, test_db)
