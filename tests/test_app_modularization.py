from __future__ import annotations

import importlib


def test_app_package_facades_are_importable() -> None:
    """The canonical and legacy namespace should expose modular facades."""
    # Modules migrated to db_monitor (no longer in app/)
    dm_factory = importlib.import_module("db_monitor.app_factory")
    dm_metrics = importlib.import_module("db_monitor.metrics")
    dm_extensions = importlib.import_module("db_monitor.extensions")
    dm_ws = importlib.import_module("db_monitor.ws_manager")
    dm_retention = importlib.import_module("db_monitor.retention")

    assert callable(dm_factory.create_application)
    assert hasattr(dm_metrics, "events_consumed_total")
    assert hasattr(dm_extensions, "AsyncSessionLocal")
    assert hasattr(dm_ws, "ws_manager")
    assert hasattr(dm_retention, "run_retention_cleanup_once")

    # Modules migrated to db_monitor (Phase 1b)
    dm_lifecycle = importlib.import_module("db_monitor.lifecycle_manager")
    dm_auth = importlib.import_module("db_monitor.auth")
    dm_audit_log = importlib.import_module("db_monitor.audit_log")
    dm_core_models = importlib.import_module("db_monitor.core.models")
    dm_core_responses = importlib.import_module("db_monitor.core.responses")

    assert hasattr(dm_lifecycle, "lifecycle_manager")
    assert hasattr(dm_lifecycle, "ApplicationLifecycleManager")
    assert hasattr(dm_auth, "require_admin_role")
    assert hasattr(dm_audit_log, "audit_log_writer")
    assert hasattr(dm_core_models, "ApiKey")
    assert hasattr(dm_core_responses, "HealthResponse")

    # Modules migrated to db_monitor (Phase 1c — ingestion)
    dm_ingestion_brokers = importlib.import_module(
        "db_monitor.ingestion.brokers"
    )
    dm_ingestion_changes = importlib.import_module(
        "db_monitor.ingestion.changes"
    )
    dm_ingestion_pipeline = importlib.import_module(
        "db_monitor.ingestion.pipeline"
    )
    dm_ingestion_schema = importlib.import_module(
        "db_monitor.ingestion.schema"
    )
    dm_ingestion_quota = importlib.import_module(
        "db_monitor.ingestion.quota"
    )
    dm_webhooks = importlib.import_module("db_monitor.webhooks")

    assert hasattr(dm_ingestion_brokers, "build_message_consumer")
    assert hasattr(dm_ingestion_changes, "ChangeProcessor")
    assert hasattr(dm_ingestion_pipeline, "event_pipeline")
    assert hasattr(dm_ingestion_schema, "SchemaDiscovery")
    assert hasattr(dm_ingestion_schema, "schema_cache_backplane")
    assert hasattr(dm_ingestion_quota, "quota_limiter")
    assert hasattr(dm_webhooks, "webhook_notifier")

    # Modules migrated to db_monitor (Phase 1e — routes and repositories)
    dm_routes = importlib.import_module("db_monitor.routes")
    dm_routes_auth = importlib.import_module("db_monitor.routes.auth")
    dm_routes_data = importlib.import_module("db_monitor.routes.data")
    dm_routes_ops = importlib.import_module("db_monitor.routes.ops")
    dm_routes_utils = importlib.import_module("db_monitor.routes.utils")
    dm_repositories = importlib.import_module("db_monitor.repositories")
    dm_repo_events = importlib.import_module(
        "db_monitor.repositories.events"
    )
    dm_repo_replay = importlib.import_module(
        "db_monitor.repositories.audit_log_replay"
    )
    dm_repo_spill = importlib.import_module(
        "db_monitor.repositories.audit_log_spill"
    )
    dm_lifespan = importlib.import_module("db_monitor.lifespan")

    assert hasattr(dm_routes, "router")
    assert hasattr(dm_routes_auth, "router")
    assert hasattr(dm_routes_data, "router")
    assert hasattr(dm_routes_ops, "router")
    assert hasattr(dm_routes_utils, "parse_timestamp")
    assert hasattr(dm_repositories, "EventQueryFilters")
    assert hasattr(dm_repo_events, "EventsRepository")
    assert hasattr(dm_repo_replay, "replay_spill_entries")
    assert hasattr(dm_repo_spill, "list_audit_log_spill_snapshot")
    assert callable(dm_lifespan.managed_lifespan)

    # Modules migrated to db_monitor (Phase 1d — consumer package and runtime)
    dm_consumer = importlib.import_module("db_monitor.consumer")
    dm_consumer_runtime = importlib.import_module(
        "db_monitor.ingestion.runtime_state"
    )
    dm_consumer_recovery = importlib.import_module(
        "db_monitor.ingestion.recovery"
    )

    assert hasattr(dm_consumer, "consumer_task")
    assert hasattr(dm_consumer, "get_consumer_health")
    assert hasattr(dm_consumer_runtime, "CircuitBreaker")
    assert hasattr(dm_consumer_runtime, "get_consumer_health")
    assert hasattr(dm_consumer_recovery, "list_consumer_checkpoints_snapshot")

    # Canonical module locations (config now at db_monitor.core.config)
    dm_core_config = importlib.import_module("db_monitor.core.config")
    dm_extensions = importlib.import_module("db_monitor.extensions")
    dm_lifecycle = importlib.import_module("db_monitor.lifecycle_manager")

    assert hasattr(dm_core_config, "settings")
    assert dm_core_config.settings.env
    assert hasattr(dm_extensions, "AsyncSessionLocal")
    assert hasattr(dm_lifecycle, "lifecycle_manager")
