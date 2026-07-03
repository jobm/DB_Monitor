"""PostgreSQL LISTEN/NOTIFY based schema-cache invalidation across replicas."""

from __future__ import annotations

import asyncio
import json
import logging
from uuid import uuid4

import asyncpg

from db_monitor.core.config import settings
from db_monitor.extensions import engine
from sqlalchemy import text

logger = logging.getLogger(__name__)

SCHEMA_CACHE_BACKPLANE_DSN = settings.db.postgres_url.replace(
    "postgresql+asyncpg://",
    "postgresql://",
    1,
)


class SchemaCacheBackplane:
    """Propagate schema-cache invalidations across clustered replicas."""

    def __init__(self) -> None:
        self._instance_id = uuid4().hex
        self._listener_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def publish_invalidation(
        self,
        service_name: str,
        table_name: str,
    ) -> None:
        """Publish a schema-cache invalidation to other replicas."""
        if not settings.auth.ws_backplane_enabled:
            return

        payload = json.dumps(
            {
                "origin_instance_id": self._instance_id,
                "service_name": service_name,
                "table_name": table_name,
            }
        )
        async with engine.begin() as connection:
            await connection.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {
                    "channel": settings.auth.ws_backplane_channel + "_schema",
                    "payload": payload,
                },
            )

    async def start_listener(self) -> asyncio.Task | None:
        """Start the schema-cache backplane listener if clustering is on."""
        if not settings.auth.ws_backplane_enabled:
            return None
        if self._listener_task and not self._listener_task.done():
            return self._listener_task

        self._stop_event = asyncio.Event()
        self._listener_task = asyncio.create_task(
            self._run_listener(),
            name="schema_cache_backplane_listener",
        )
        return self._listener_task

    async def _run_listener(self) -> None:
        while not self._stop_event.is_set():
            connection: asyncpg.Connection | None = None
            try:
                connection = await asyncpg.connect(
                    SCHEMA_CACHE_BACKPLANE_DSN
                )
                await connection.add_listener(
                    settings.auth.ws_backplane_channel + "_schema",
                    self._handle_notification,
                )
                logger.info(
                    "Schema cache backplane listener subscribed to %s",
                    settings.auth.ws_backplane_channel + "_schema",
                )
                await self._stop_event.wait()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Schema cache backplane listener error: %s",
                    exc,
                )
                await asyncio.sleep(5.0)
            finally:
                if connection is not None:
                    try:
                        await connection.remove_listener(
                            settings.auth.ws_backplane_channel + "_schema",
                            self._handle_notification,
                        )
                    except Exception:
                        pass
                    await connection.close()

    def _handle_notification(
        self,
        _connection: asyncpg.Connection,
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        asyncio.create_task(self._deliver_notification(payload))

    async def _deliver_notification(self, payload: str) -> None:
        try:
            envelope = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("Ignoring invalid schema-cache backplane payload")
            return

        if envelope.get("origin_instance_id") == self._instance_id:
            return

        service_name = str(envelope.get("service_name") or "").strip()
        table_name = str(envelope.get("table_name") or "").strip()
        if not service_name or not table_name:
            return

        # Import here to avoid circular dependency:
        # cache -> discovery -> cache
        from db_monitor.schema.discovery import _invalidate_registered_schema_caches

        _invalidate_registered_schema_caches(service_name, table_name)


schema_cache_backplane = SchemaCacheBackplane()
