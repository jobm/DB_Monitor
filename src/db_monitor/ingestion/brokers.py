"""Canonical ingestion brokers exports.

This module now points directly at runtime broker adapters without routing
through deprecated app/ingestion wrapper modules.
"""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_message_brokers = importlib.import_module("message_brokers")

BrokerMessage = _message_brokers.BrokerMessage
DeadLetterPublisher = _message_brokers.DeadLetterPublisher
build_message_consumer = _message_brokers.build_message_consumer
build_dead_letter_publisher = _message_brokers.build_dead_letter_publisher

__all__ = [
    "BrokerMessage",
    "DeadLetterPublisher",
    "build_message_consumer",
    "build_dead_letter_publisher",
]
