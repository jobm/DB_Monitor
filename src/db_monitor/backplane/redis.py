"""Redis Pub/Sub backplane implementation.

The Redis backplane uses Redis Pub/Sub for cross-replica communication.
It is the recommended backend for high-scale deployments (2000+ ev/s).

Advantages over Postgres LISTEN/NOTIFY:

- Persistent connection management with auto-reconnect
- Lower latency than Postgres notifications under load
- No shared dependency on the monitor database for fanout
- Supports multiple channels natively with channel globbing
- Can be scaled independently of the database

Configuration::

    WS_BACKPLANE_BACKEND=redis
    REDIS_URL=redis://redis:6379/0
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis as AsyncRedis


logger = logging.getLogger(__name__)


class RedisBackplane:
    """Redis Pub/Sub cross-replica backplane."""

    def __init__(self, channel: str, redis_url: str) -> None:
        self._channel = channel
        self._redis_url = redis_url
        self._notification_callback: Callable[[str], Awaitable[None]] | None = None
        self._listener_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._redis: AsyncRedis | None = None
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def start(
        self,
        notification_callback: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        """Start the backplane listener and subscribe to the channel."""
        self._notification_callback = notification_callback
        if self._listener_task and not self._listener_task.done():
            return
        self._stop_event = asyncio.Event()
        self._listener_task = asyncio.create_task(
            self._run_listener(),
            name=f"redis_backplane_{self._channel}",
        )

    async def stop(self) -> None:
        """Stop the backplane listener and close Redis connections."""
        if self._listener_task is None:
            return
        self._stop_event.set()
        self._listener_task.cancel()
        try:
            await self._listener_task
        except asyncio.CancelledError:
            pass
        self._listener_task = None
        self._connected = False
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def publish(self, payload: str, channel: str | None = None) -> None:
        """Publish a payload to the Redis Pub/Sub channel."""
        ch = channel or self._channel
        redis = await self._get_connection()
        await redis.publish(ch, payload)

    async def _get_connection(self) -> AsyncRedis:
        """Get or create a Redis connection."""
        if self._redis is None:
            self._redis = AsyncRedis.from_url(
                self._redis_url,
                decode_responses=True,
                socket_keepalive=True,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
            )
        return self._redis

    async def _run_listener(self) -> None:
        while not self._stop_event.is_set():
            try:
                redis = await self._get_connection()
                # Verify connectivity
                await redis.ping()
                self._connected = True

                async with redis.pubsub() as pubsub:
                    await pubsub.subscribe(self._channel)
                    logger.info(
                        "Redis backplane subscribed to %s",
                        self._channel,
                    )

                    async for message in pubsub.listen():
                        if self._stop_event.is_set():
                            break
                        if message["type"] != "message":
                            continue
                        payload = message.get("data", "")
                        if isinstance(payload, bytes):
                            payload = payload.decode("utf-8")
                        if (
                            self._notification_callback is not None
                            and payload
                        ):
                            await self._notification_callback(payload)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Redis backplane listener error: %s",
                    exc,
                )
                self._connected = False
                self._redis = None
                await asyncio.sleep(5.0)

    async def __aenter__(self) -> "RedisBackplane":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()
