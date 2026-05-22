from __future__ import annotations

from datetime import datetime, timezone
from types import ModuleType

import pytest

import event_pipeline as event_pipeline_module
from models import KafkaEvent


def _make_event(event_data: dict) -> KafkaEvent:
    """Build a minimal event object for pipeline tests."""
    return KafkaEvent(
        event_type="UPDATE",
        event_time=datetime.now(timezone.utc),
        user_id=None,
        service_name="orderdb",
        kafka_topic="orderdb.public.orders",
        kafka_partition=0,
        kafka_offset=1,
        event_data=event_data,
        raw_payload="{}",
        operation="UPDATE",
    )


def test_transform_runs_custom_processors_before_masking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Derived fields from a custom processor should persist through masking."""
    processor_module = ModuleType("test_custom_event_processors")

    def derive_summary(event: KafkaEvent) -> KafkaEvent:
        payload = event.event_data.setdefault("payload", {})
        after = payload.setdefault("after", {})
        after["summary"] = f"order-{after.get('id')}"
        return event

    processor_module.derive_summary = derive_summary
    monkeypatch.setitem(
        __import__("sys").modules,
        "test_custom_event_processors",
        processor_module,
    )
    monkeypatch.setenv(
        "CUSTOM_EVENT_PROCESSORS",
        "test_custom_event_processors:derive_summary",
    )
    monkeypatch.setenv("TRANSFORM_MASK_FIELDS", "password")

    pipeline = event_pipeline_module.EventPipeline(
        event_pipeline_module.EventPipelineConfig()
    )
    event = _make_event(
        {"payload": {"after": {"id": 42, "password": "secret"}}}
    )

    transformed = pipeline.transform(event)

    assert transformed.event_data["payload"]["after"]["summary"] == "order-42"
    assert transformed.event_data["payload"]["after"]["password"] == "***MASKED***"


def test_config_rejects_invalid_custom_processor_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Misconfigured processor paths should fail with actionable guidance."""
    monkeypatch.setenv("CUSTOM_EVENT_PROCESSORS", "missing_processor")

    with pytest.raises(
        ValueError,
        match="CUSTOM_EVENT_PROCESSORS.*module:function.*module.function",
    ):
        event_pipeline_module.EventPipelineConfig()