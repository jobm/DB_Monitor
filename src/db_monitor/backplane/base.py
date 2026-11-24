"""Abstract backplane protocol and event envelope types."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable


@runtime_checkable
class BackplaneProtocol(Protocol):
    """Pluggable cross-replica pub/sub backplane.

    Implementations must support publish, start, stop, and accept a
    notification callback that is invoked for each received message.
    """

    async def start(
        self,
        notification_callback: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        """Start the backplane listener and register the notification callback."""
        ...

    async def stop(self) -> None:
        """Stop the backplane listener and release resources."""
        ...

    async def publish(self, payload: str, channel: str | None = None) -> None:
        """Publish a JSON payload to the backplane channel."""
        ...

    @property
    def is_connected(self) -> bool:
        """Return True if the backplane is connected and running."""
        ...
