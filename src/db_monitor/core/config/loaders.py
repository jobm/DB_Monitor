"""Backward-compatible re-exports from the db_monitor.core.config package.

The normalization and loading helpers previously duplicated here have been
moved into ``db_monitor.core.config.normalizers``.
"""

from db_monitor.core.config import _get_settings, settings, AppSettings  # noqa: F401
from db_monitor.core.config.dataclasses import (  # noqa: F401
    AuthSettings,
    BrokerSettings,
    DatabaseSettings,
    IngestionSettings,
    ObservabilitySettings,
    RetentionSettings,
    WebhookSettings,
)
from db_monitor.core.config.normalizers import (  # noqa: F401
    default_allow_bootstrap,
    default_kafka_cluster,
    default_rabbitmq_cluster,
    default_tenant_cohort_slo_policies,
    load_broker_clusters,
    load_ingestion_quota_overrides,
    load_kafka_clusters,
    load_tenant_cohort_slo_policies,
    normalize_bootstrap_servers,
    normalize_broker_kind,
    normalize_cluster_topics,
    normalize_partition_list,
    normalize_postgres_url,
    normalize_rabbitmq_queue_names,
    normalize_topic_partitions,
)
