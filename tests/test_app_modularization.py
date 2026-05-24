from __future__ import annotations

import importlib


def test_app_package_facades_are_importable() -> None:
    """The legacy app namespace should expose modular facades."""
    api_factory = importlib.import_module("app.api.factory")
    api_router = importlib.import_module("app.api.router")
    core_config = importlib.import_module("app.core.config")
    core_db = importlib.import_module("app.core.db")
    core_lifecycle = importlib.import_module("app.core.lifecycle")
    ingestion_brokers = importlib.import_module("app.ingestion.brokers")
    ingestion_consumer = importlib.import_module("app.ingestion.consumer")
    ingestion_pipeline = importlib.import_module("app.ingestion.pipeline")
    ingestion_runtime = importlib.import_module(
        "app.ingestion.runtime_state"
    )
    ingestion_schema = importlib.import_module("app.ingestion.schema")

    assert callable(api_factory.create_application)
    assert hasattr(api_router, "router")
    assert hasattr(core_config, "KAFKA_BROKER")
    assert hasattr(core_db, "AsyncSessionLocal")
    assert hasattr(core_lifecycle, "lifecycle_manager")
    assert hasattr(ingestion_brokers, "build_message_consumer")
    assert hasattr(ingestion_consumer, "consumer_task")
    assert hasattr(ingestion_consumer, "replay_dead_letter_event_record")
    assert hasattr(ingestion_pipeline, "event_pipeline")
    assert hasattr(ingestion_runtime, "consumer_runtime")
    assert hasattr(ingestion_schema, "schema_cache_backplane")
