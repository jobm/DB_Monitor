"""Typed dataclass definitions for all application configuration domains."""

from db_monitor.core.config.dataclasses.broker import BrokerSettings
from db_monitor.core.config.dataclasses.core import AppSettings
from db_monitor.core.config.dataclasses.auth_settings import AuthSettings
from db_monitor.core.config.dataclasses.backplane import BackplaneSettings
from db_monitor.core.config.dataclasses.db import DatabaseSettings
from db_monitor.core.config.dataclasses.ingestion import IngestionSettings
from db_monitor.core.config.dataclasses.observability import ObservabilitySettings
from db_monitor.core.config.dataclasses.retention import RetentionSettings
from db_monitor.core.config.dataclasses.webhook import WebhookSettings

__all__ = [
    "AppSettings",
    "AuthSettings",
    "BackplaneSettings",
    "BrokerSettings",
    "DatabaseSettings",
    "IngestionSettings",
    "ObservabilitySettings",
    "RetentionSettings",
    "WebhookSettings",
]
