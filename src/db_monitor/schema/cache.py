"""Cross-replica schema-cache invalidation via pluggable backplane.

Uses the same backplane infrastructure as WebSocket fanout (Postgres
LISTEN/NOTIFY or Redis Pub/Sub) on a dedicated channel (``<ws_channel>_schema``).
"""

from __future__ import annotations

import asyncio
import json
import logging
from uuid import uuid4

from db_monitor.backplane import create_backplane
from db_monitor.backplane.base import BackplaneProtocol
from db_monitor.core.config import settings

logger = logging.getLogger(__name__)


class SchemaCacheBackplane:
    """Propagate schema-cache invalidations across clustered replicas."""

    def __init__(self) -> None:
        self._instance_id = uuid4().hex
        self._backplane: BackplaneProtocol | None = None
        self._listener_task: asyncio.Task | None = None
        self._backplane_enabled = settings.auth.ws_backplane_enabled

    async def publish_invalidation(
        self,
        service_name: str,
        table_name: str,
    ) -> None:
        """Publish a schema-cache invalidation to other replicas."""
        if not self._backplane_enabled or self._backplane is None:
            return

        payload = json.dumps(
            {
                "origin_instance_id": self._instance_id,
                "service_name": service_name,
                "table_name": table_name,
            }
        )
        channel = settings.auth.ws_backplane_channel + "_schema"
        await self._backplane.publish(payload, channel=channel)

    async def start_listener(self) -> asyncio.Task | None:
        """Start the schema-cache backplane listener if clustering is on."""
        if not self._backplane_enabled:
            return None
        if self._listener_task and not self._listener_task.done():
            return self._listener_task

        channel = settings.auth.ws_backplane_channel + "_schema"
        self._backplane = create_backplane(channel=channel)
        await self._backplane.start(
            notification_callback=self._handle_notification,
        )

        self._listener_task = asyncio.create_task(
            self._wait_for_backplane(),
            name="schema_cache_backplane_watcher",
        )
        return self._listener_task

    async def _wait_for_backplane(self) -> None:
        """Keep the lifespan manager aware while the backplane is connected."""
        while self._backplane is not None and self._backplane.is_connected:
            await asyncio.sleep(1)

    async def _handle_notification(self, payload: str) -> None:
        """Handle an incoming backplane notification."""
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
