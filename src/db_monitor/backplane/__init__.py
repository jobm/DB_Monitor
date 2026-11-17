"""Pluggable cross-replica backplane for WebSocket fanout.

Provides Postgres LISTEN/NOTIFY and Redis Pub/Sub backends for
broadcasting events across horizontally scaled replicas.

Usage::

    from db_monitor.backplane import create_backplane

    backplane = create_backplane()
    await backplane.start()
    await backplane.publish({"event_id": 42})
    # ... backplane delivers to _handle_notification callback
    await backplane.stop()
"""

from db_monitor.backplane.base import BackplaneProtocol
from db_monitor.backplane.factory import create_backplane

__all__ = [
    "BackplaneProtocol",
    "create_backplane",
]
