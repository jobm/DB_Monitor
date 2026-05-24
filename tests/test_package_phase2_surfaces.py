from __future__ import annotations

import db_monitor.auth as package_auth
import db_monitor.ingestion.brokers as package_ingestion_brokers
import db_monitor.ingestion.changes as package_ingestion_changes
import db_monitor.ingestion.consumer as package_ingestion_consumer
import db_monitor.ingestion.pipeline as package_ingestion_pipeline
import db_monitor.ingestion.runtime_state as package_ingestion_runtime
import db_monitor.ingestion.schema as package_ingestion_schema
import db_monitor.repositories.events as package_repository_events
import db_monitor.routes as package_routes


def test_package_auth_surface_exposes_authenticate_credentials() -> None:
    """Canonical auth namespace should expose credential validation helper."""
    assert callable(package_auth.authenticate_credentials)


def test_package_ingestion_surfaces_expose_expected_runtime_helpers() -> None:
    """Canonical ingestion namespace should expose key runtime helpers."""
    assert callable(package_ingestion_consumer.consumer_task)
    assert hasattr(package_ingestion_schema, "schema_cache_backplane")
    assert hasattr(package_ingestion_changes, "ChangeProcessor")
    assert hasattr(package_ingestion_pipeline, "event_pipeline")
    assert hasattr(package_ingestion_runtime, "consumer_runtimes")
    assert callable(package_ingestion_brokers.build_message_consumer)


def test_package_repositories_surface_exposes_events_repository() -> None:
    """Canonical repositories namespace should expose EventsRepository."""
    assert hasattr(package_repository_events, "EventsRepository")
    assert hasattr(package_repository_events, "EventQueryFilters")


def test_package_routes_surface_exposes_router() -> None:
    """Canonical routes namespace should expose FastAPI router composition."""
    assert hasattr(package_routes, "router")
