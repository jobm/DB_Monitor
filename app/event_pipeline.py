"""Event filtering, validation, and transformation pipeline."""

import importlib
import logging
import os
from collections.abc import Callable
from typing import Any, Optional

from core.models import KafkaEvent
from source_metadata import extract_source_coordinates

logger = logging.getLogger(__name__)

EventProcessor = Callable[[KafkaEvent], KafkaEvent | None]


class EventPipelineConfig:
    """Configuration for the event pipeline.

    Loads settings from environment variables for:
    - FILTER_ALLOWED_EVENT_TYPES (comma-separated, e.g. "insert,update")
    - FILTER_IGNORED_EVENT_TYPES (comma-separated, defaults to "unparsed")
    - FILTER_ALLOWED_TABLES (comma-separated, e.g. "public.orders")
    - FILTER_IGNORED_TABLES (comma-separated)
        - VALIDATION_REQUIRED_FIELDS
            (comma-separated keys that must exist in event_data)
    - TRANSFORM_MASK_FIELDS (comma-separated, fields to mask with ***MASKED***)
        - CUSTOM_EVENT_PROCESSORS
            (comma-separated import paths such as
            "my_module:processor,my_package.processors.other_processor")
    """

    def __init__(self):
        self.allowed_event_types = self._parse_list(
            os.getenv("FILTER_ALLOWED_EVENT_TYPES")
        )
        self.ignored_event_types = self._parse_list(
            os.getenv("FILTER_IGNORED_EVENT_TYPES", "unparsed")
        )
        self.allowed_tables = self._parse_list(
            os.getenv("FILTER_ALLOWED_TABLES")
        )
        self.ignored_tables = self._parse_list(
            os.getenv("FILTER_IGNORED_TABLES")
        )

        self.required_fields = self._parse_list(
            os.getenv("VALIDATION_REQUIRED_FIELDS")
        )
        self.mask_fields = self._parse_list(
            os.getenv(
                "TRANSFORM_MASK_FIELDS", "password,secret,token,credit_card"
            )
        )
        self.custom_processors = self._load_custom_processors(
            os.getenv("CUSTOM_EVENT_PROCESSORS")
        )

    def _parse_list(
        self, val: Optional[str], default: list[str] = None
    ) -> list[str]:
        if not val:
            return default or []
        return [v.strip() for v in val.split(",") if v.strip()]

    def _load_custom_processors(
        self,
        value: Optional[str],
    ) -> list[EventProcessor]:
        """Load custom event-processor callables from import paths."""
        return [
            self._load_processor_reference(reference)
            for reference in self._parse_list(value)
        ]

    def _load_processor_reference(self, reference: str) -> EventProcessor:
        """Resolve a single processor import path into a callable."""
        module_name, attribute_name = self._split_processor_reference(
            reference
        )

        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise ValueError(
                "Invalid CUSTOM_EVENT_PROCESSORS entry "
                f"'{reference}'. Could not import module '{module_name}'."
            ) from exc

        processor = getattr(module, attribute_name, None)
        if not callable(processor):
            raise ValueError(
                "Invalid CUSTOM_EVENT_PROCESSORS entry "
                f"'{reference}'. Expected a callable processor reference."
            )

        return processor

    def _split_processor_reference(self, reference: str) -> tuple[str, str]:
        """Split `module:function` or `module.function` references."""
        if ":" in reference:
            module_name, attribute_name = reference.split(":", 1)
        elif "." in reference:
            module_name, attribute_name = reference.rsplit(".", 1)
        else:
            raise ValueError(
                "Invalid CUSTOM_EVENT_PROCESSORS entry "
                f"'{reference}'. Use 'module:function' or 'module.function'."
            )

        if not module_name or not attribute_name:
            raise ValueError(
                "Invalid CUSTOM_EVENT_PROCESSORS entry "
                f"'{reference}'. Use 'module:function' or 'module.function'."
            )

        return module_name, attribute_name


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
            source_coordinates = extract_source_coordinates(event.event_data)
            if source_coordinates is not None:
                candidate_tables = source_coordinates.table_filter_names()

                if self.config.allowed_tables:
                    if not any(
                        candidate in self.config.allowed_tables
                        for candidate in candidate_tables
                    ):
                        return False

                if any(
                    candidate in self.config.ignored_tables
                    for candidate in candidate_tables
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
        """Apply transformation rules like data masking.

        This mutates the event in place in most cases.
        """
        event = self._apply_custom_processors(event)
        if (
            not self.config.mask_fields
            or not event.event_data
            or not isinstance(event.event_data, dict)
        ):
            return event

        # Recursive apply mask to dicts
        self._mask_dict(event.event_data)

        return event

    def _apply_custom_processors(self, event: KafkaEvent) -> KafkaEvent:
        """Run configured custom processors in order."""
        current_event = event
        for processor in self.config.custom_processors:
            processed_event = processor(current_event)
            if processed_event is not None:
                current_event = processed_event
        return current_event

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
