"""Auth and customer lifecycle migration steps — API keys, JWT secrets, control-plane state."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def _apply_api_key_lifecycle_columns(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Add API key expiration and revocation lifecycle columns."""
    del session_factory
    statements = [
        "ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ",
        "ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ",
        (
            "CREATE INDEX IF NOT EXISTS ix_api_keys_expires_at "
            "ON api_keys (expires_at)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS ix_api_keys_revoked_at "
            "ON api_keys (revoked_at)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))


async def _apply_customer_jwt_secret_state(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create durable customer JWT secret lifecycle storage."""
    del session_factory
    await connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS customer_jwt_secret_state (
                customer_id VARCHAR(128) PRIMARY KEY,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )


async def _apply_customer_control_plane_state(
    connection: AsyncConnection,
    session_factory: Callable,
) -> None:
    """Create durable customer lifecycle and job state tables."""
    del session_factory
    statements = [
        """
        CREATE TABLE IF NOT EXISTS customer_lifecycle_state (
            customer_id VARCHAR(128) PRIMARY KEY,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_provision_jobs (
            job_id VARCHAR(64) PRIMARY KEY,
            customer_id VARCHAR(128) NOT NULL,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        (
            "CREATE INDEX IF NOT EXISTS "
            "ix_customer_provision_jobs_customer_id "
            "ON customer_provision_jobs (customer_id)"
        ),
    ]
    for statement in statements:
        await connection.execute(text(statement))
