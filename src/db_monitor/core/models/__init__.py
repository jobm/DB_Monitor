"""SQLAlchemy models for DB Monitor Server — domain-split sub-package.

All models are re-exported here so that existing imports from
``db_monitor.core.models`` continue to work unchanged.
"""

from db_monitor.core.models.auth import (
    ApiKey,
    CustomerJWTSecretState,
    CustomerLifecycleState,
    CustomerProvisionJob,
)
from db_monitor.core.models.base import Base
from db_monitor.core.models.consumer import ConsumerCheckpoint, DeadLetterEvent
from db_monitor.core.models.events import ColumnChange, KafkaEvent, KafkaEventLegacy
from db_monitor.core.models.ops import ApiAuditLog, ApiAuditLogSpill, IngestionQuotaWindow
from db_monitor.core.models.schema import MonitoredColumn, MonitoredTable

__all__ = [
    "ApiAuditLog",
    "ApiAuditLogSpill",
    "ApiKey",
    "Base",
    "ColumnChange",
    "ConsumerCheckpoint",
    "CustomerJWTSecretState",
    "CustomerLifecycleState",
    "CustomerProvisionJob",
    "DeadLetterEvent",
    "IngestionQuotaWindow",
    "KafkaEvent",
    "KafkaEventLegacy",
    "MonitoredColumn",
    "MonitoredTable",
]
