"""Event filtering, validation, and transformation pipeline."""

import logging
import os
from typing import Any, Optional

from models import KafkaEvent

logger = logging.getLogger(__name__)


class EventPipelineConfig:
    """Configuration for the event pipeline.

    Loads settings from environment variables for:
    - FILTER_ALLOWED_EVENT_TYPES (comma-separated, e.g. "insert,update")
    - FILTER_IGNORED_EVENT_TYPES (comma-separated, defaults to "unparsed")
    - FILTER_ALLOWED_TABLES (comma-separated, e.g. "public.orders")
    - FILTER_IGNORED_TABLES (comma-separated)
    - VALIDATION_REQUIRED_FIELDS (comma-separated keys that must exist in event_data)
    - TRANSFORM_MASK_FIELDS (comma-separated, fields to mask with ***MASKED***)
    """

    def __init__(self):
        self.allowed_event_types = self._parse_list(
            os.getenv("FILTER_ALLOWED_EVENT_TYPES")
        )
        self.ignored_event_types = self._parse_list(
            os.getenv("FILTER_IGNORED_EVENT_TYPES", "unparsed")
        )
        self.allowed_tables = self._parse_list(os.getenv("FILTER_ALLOWED_TABLES"))
        self.ignored_tables = self._parse_list(os.getenv("FILTER_IGNORED_TABLES"))

        self.required_fields = self._parse_list(os.getenv("VALIDATION_REQUIRED_FIELDS"))
        self.mask_fields = self._parse_list(
            os.getenv("TRANSFORM_MASK_FIELDS", "password,secret,token,credit_card")
        )

    def _parse_list(self, val: Optional[str], default: list[str] = None) -> list[str]:
        if not val:
            return default or []
        return [v.strip() for v in val.split(",") if v.strip()]


class EventPipeline:
    """Handles filtering, validation, and transformation of KafkaEvents."""

    def __init__(self, config: Optional[EventPipelineConfig] = None):
        self.config = config or EventPipelineConfig()

    def should_process(self, event: KafkaEvent) -> bool:
        """Determines if the event passes filtering and validation rules."""

        # 1. Event Type Filtering
        if event.event_type:
            if (
                self.config.allowed_event_types
                and event.event_type not in self.config.allowed_event_types
            ):
                return False
            if event.event_type in self.config.ignored_event_types:
                return False

        # 2. Table Filtering
        if event.event_data and isinstance(event.event_data, dict):
            source = event.event_data.get("source")
            if not isinstance(source, dict):
                source = event.event_data.get("payload", {}).get("source")
            if isinstance(source, dict):
                db = source.get("db", "")
                table = source.get("table", "")
                if table:
                    full_table = f"{db}.{table}" if db else table

                    if self.config.allowed_tables:
                        if (
                            table not in self.config.allowed_tables
                            and full_table not in self.config.allowed_tables
                        ):
                            return False

                    if (
                        table in self.config.ignored_tables
                        or full_table in self.config.ignored_tables
                    ):
                        return False

        # 3. Schema-based Validation (Required Fields)
        if self.config.required_fields:
            if not event.event_data or not isinstance(event.event_data, dict):
                logger.warning("Event missing event_data, failing validation.")
                return False

            for field in self.config.required_fields:
                if field not in event.event_data:
                    logger.warning(f"Event missing required field: {field}")
                    return False

        return True

    def transform(self, event: KafkaEvent) -> KafkaEvent:
        """Applies transformation rules like data masking. Modifies inplace (mostly)."""
        if (
            not self.config.mask_fields
            or not event.event_data
            or not isinstance(event.event_data, dict)
        ):
            return event

        # Recursive apply mask to dicts
        self._mask_dict(event.event_data)

        return event

    def _mask_dict(self, data: Any):
        if not isinstance(data, dict):
            return

        for key, value in data.items():
            if key in self.config.mask_fields:
                data[key] = "***MASKED***"
            elif isinstance(value, dict):
                self._mask_dict(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._mask_dict(item)


# Default global pipeline instance
event_pipeline = EventPipeline()
