"""Environment-variable normalization and cluster-configuration loaders.

All functions are re-exported so existing imports from
``db_monitor.core.config.normalizers`` continue to work unchanged.
"""

from db_monitor.core.config.normalizers.clusters import (
    default_kafka_cluster,
    default_rabbitmq_cluster,
    load_broker_clusters,
    load_kafka_clusters,
    normalize_bootstrap_servers,
    normalize_broker_kind,
    normalize_cluster_topics,
    normalize_partition_list,
    normalize_rabbitmq_queue_names,
    normalize_topic_partitions,
)
from db_monitor.core.config.normalizers.quotas import (
    load_ingestion_quota_overrides,
)
from db_monitor.core.config.normalizers.slo import (
    default_tenant_cohort_slo_policies,
    load_tenant_cohort_slo_policies,
)
from db_monitor.core.config.normalizers.urls import (
    default_allow_bootstrap,
    normalize_postgres_url,
)

__all__ = [
    "default_allow_bootstrap",
    "default_kafka_cluster",
    "default_rabbitmq_cluster",
    "default_tenant_cohort_slo_policies",
    "load_broker_clusters",
    "load_kafka_clusters",
    "load_ingestion_quota_overrides",
    "load_tenant_cohort_slo_policies",
    "normalize_bootstrap_servers",
    "normalize_broker_kind",
    "normalize_cluster_topics",
    "normalize_partition_list",
    "normalize_postgres_url",
    "normalize_rabbitmq_queue_names",
    "normalize_topic_partitions",
]
