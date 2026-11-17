"""Backplane factory — creates the active backplane based on settings."""

from __future__ import annotations

from db_monitor.core.config import settings
from db_monitor.backplane.base import BackplaneProtocol


def create_backplane(
    channel: str | None = None,
) -> BackplaneProtocol:
    """Create the active backplane based on ``WS_BACKPLANE_BACKEND``.

    Returns a :class:`BackplaneProtocol` implementation.

    Args:
        channel: The pub/sub channel name. Defaults to
            ``settings.auth.ws_backplane_channel``.

    Returns:
        A :class:`PostgresBackplane` or :class:`RedisBackplane` instance.

    Raises:
        ValueError: If ``WS_BACKPLANE_BACKEND`` is set to an unsupported value
            or if ``redis`` is selected but ``REDIS_URL`` is not configured.
    """
    if channel is None:
        channel = settings.auth.ws_backplane_channel

    backend = settings.backplane.backend
    if backend == "postgres":
        from db_monitor.backplane.postgres import PostgresBackplane

        return PostgresBackplane(channel)

    if backend == "redis":
        redis_url = settings.backplane.redis_url
        if not redis_url:
            raise ValueError(
                "REDIS_URL must be set when WS_BACKPLANE_BACKEND=redis"
            )
        from db_monitor.backplane.redis import RedisBackplane

        return RedisBackplane(channel, redis_url)

    raise ValueError(
        f"Unsupported WS_BACKPLANE_BACKEND: {backend!r}. "
        f"Expected 'postgres' or 'redis'."
    )
