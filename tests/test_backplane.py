"""Tests for the pluggable backplane implementations.

Tests cover:
- PostgresBackplane construction and interface compliance
- RedisBackplane construction and interface compliance
- Backplane factory (create_backplane)
- WebSocketManager backplane integration
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from db_monitor.backplane import BackplaneProtocol, create_backplane
from db_monitor.backplane.postgres import PostgresBackplane
from db_monitor.backplane.redis import RedisBackplane


class TestBackplaneProtocol:
    """Verify that backplane implementations satisfy the protocol."""

    def test_postgres_is_protocol(self) -> None:
        bp = PostgresBackplane("test_channel")
        assert isinstance(bp, BackplaneProtocol)

    def test_redis_is_protocol(self) -> None:
        bp = RedisBackplane("test_channel", "redis://localhost:6379/0")
        assert isinstance(bp, BackplaneProtocol)


class TestPostgresBackplane:
    """Unit tests for PostgresBackplane."""

    def test_init(self) -> None:
        bp = PostgresBackplane("test_channel")
        assert bp._channel == "test_channel"
        assert bp.is_connected is False
        assert bp._listener_task is None


class TestRedisBackplane:
    """Unit tests for RedisBackplane."""

    def test_init(self) -> None:
        bp = RedisBackplane("test_channel", "redis://localhost:6379/0")
        assert bp._channel == "test_channel"
        assert bp._redis_url == "redis://localhost:6379/0"
        assert bp.is_connected is False
        assert bp._listener_task is None

    @patch("db_monitor.backplane.redis.AsyncRedis")
    async def test_publish(self, mock_redis_class: AsyncMock) -> None:
        mock_redis: AsyncMock = AsyncMock()
        mock_redis_class.from_url.return_value = mock_redis

        bp = RedisBackplane("test_channel", "redis://localhost:6379/0")
        await bp.publish('{"hello": "world"}')

        mock_redis.publish.assert_awaited_once_with(
            "test_channel",
            '{"hello": "world"}',
        )

    @patch("db_monitor.backplane.redis.AsyncRedis")
    async def test_publish_custom_channel(self, mock_redis_class: AsyncMock) -> None:
        mock_redis: AsyncMock = AsyncMock()
        mock_redis_class.from_url.return_value = mock_redis

        bp = RedisBackplane("test_channel", "redis://localhost:6379/0")
        await bp.publish("payload", channel="custom_channel")

        mock_redis.publish.assert_awaited_once_with("custom_channel", "payload")

    @patch("db_monitor.backplane.redis.AsyncRedis")
    async def test_start_stop(self, mock_redis_class: AsyncMock) -> None:
        mock_redis: AsyncMock = AsyncMock()
        mock_redis_class.from_url.return_value = mock_redis

        bp = RedisBackplane("test_channel", "redis://localhost:6379/0")

        callback = AsyncMock()
        await bp.start(notification_callback=callback)
        assert bp._listener_task is not None
        assert not bp._listener_task.done()

        await bp.stop()
        assert bp._listener_task is None
        assert bp.is_connected is False
        # Stop clears _redis, so aclose is called on the stored reference
        assert bp._redis is None

    @patch("db_monitor.backplane.redis.AsyncRedis")
    async def test_start_creates_listener_task(self, mock_redis_class: AsyncMock) -> None:
        mock_redis: AsyncMock = AsyncMock()
        mock_redis_class.from_url.return_value = mock_redis

        bp = RedisBackplane("test_channel", "redis://localhost:6379/0")
        assert bp._listener_task is None

        await bp.start()
        assert bp._listener_task is not None
        assert not bp._listener_task.done()

        await bp.stop()


class TestCreateBackplane:
    """Tests for the backplane factory."""

    @patch("db_monitor.backplane.factory.settings")
    def test_create_postgres_default(self, mock_settings) -> None:
        mock_settings.backplane.backend = "postgres"
        mock_settings.auth.ws_backplane_channel = "db_monitor_ws_events"

        bp = create_backplane()
        assert isinstance(bp, PostgresBackplane)
        assert bp._channel == "db_monitor_ws_events"

    @patch("db_monitor.backplane.factory.settings")
    def test_create_postgres_custom_channel(self, mock_settings) -> None:
        mock_settings.backplane.backend = "postgres"

        bp = create_backplane(channel="custom_ch")
        assert isinstance(bp, PostgresBackplane)
        assert bp._channel == "custom_ch"

    @patch("db_monitor.backplane.factory.settings")
    def test_create_redis(self, mock_settings) -> None:
        mock_settings.backplane.backend = "redis"
        mock_settings.backplane.redis_url = "redis://redis:6379/0"
        mock_settings.auth.ws_backplane_channel = "db_monitor_ws_events"

        bp = create_backplane()
        assert isinstance(bp, RedisBackplane)
        assert bp._channel == "db_monitor_ws_events"
        assert bp._redis_url == "redis://redis:6379/0"

    @patch("db_monitor.backplane.factory.settings")
    def test_create_redis_missing_url(self, mock_settings) -> None:
        mock_settings.backplane.backend = "redis"
        mock_settings.backplane.redis_url = None

        with pytest.raises(ValueError, match="REDIS_URL"):
            create_backplane()

    @patch("db_monitor.backplane.factory.settings")
    def test_create_unsupported_backend(self, mock_settings) -> None:
        mock_settings.backplane.backend = "nats"

        with pytest.raises(ValueError, match="nats"):
            create_backplane()


class TestWebSocketManagerBackplane:
    """Tests for WebSocketManager backplane integration."""

    @patch("db_monitor.ws_manager.settings")
    def test_backplane_backend_initially_none(self, mock_settings) -> None:
        from db_monitor.ws_manager import WebSocketManager

        mock_settings.auth.ws_backplane_enabled = True
        mgr = WebSocketManager()
        assert mgr.backplane_backend is None

    @patch("db_monitor.ws_manager.settings")
    async def test_backplane_disabled(self, mock_settings) -> None:
        from db_monitor.ws_manager import WebSocketManager

        mock_settings.auth.ws_backplane_enabled = False
        mgr = WebSocketManager()
        assert mgr._backplane_enabled is False
        result = await mgr.start_backplane_listener()
        assert result is None
