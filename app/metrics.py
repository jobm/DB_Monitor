"""Prometheus metrics for DB Monitor."""

from prometheus_client import Counter, Histogram, Gauge

events_consumed_total = Counter(
    "db_monitor_events_consumed_total",
    "Total number of events consumed from Kafka",
    ["service", "operation"]
)

events_processed_total = Counter(
    "db_monitor_events_processed_total",
    "Total number of events processed successfully",
    ["service", "operation"]
)

events_failed_total = Counter(
    "db_monitor_events_failed_total",
    "Total number of events that failed processing",
    ["service", "error_type"]
)

db_write_duration_seconds = Histogram(
    "db_monitor_db_write_duration_seconds",
    "Time spent writing events to database",
    ["operation"]
)

kafka_commit_duration_seconds = Histogram(
    "db_monitor_kafka_commit_duration_seconds",
    "Time spent committing Kafka offsets"
)

api_request_duration_seconds = Histogram(
    "db_monitor_api_request_duration_seconds",
    "API request duration in seconds",
    ["endpoint", "method"]
)

audit_log_flush_duration_seconds = Histogram(
    "db_monitor_audit_log_flush_duration_seconds",
    "Time spent flushing buffered audit log records"
)

audit_log_queue_size = Gauge(
    "db_monitor_audit_log_queue_size",
    "Number of buffered audit log records waiting to be persisted"
)

audit_log_records_dropped_total = Counter(
    "db_monitor_audit_log_records_dropped_total",
    "Total number of audit log records dropped because the queue was full"
)

failed_auth_attempts_total = Counter(
    "db_monitor_failed_auth_attempts_total",
    "Total number of failed authentication attempts",
    ["surface", "credential_type"]
)

active_connections = Gauge(
    "db_monitor_active_connections",
    "Number of active database connections"
)

db_pool_size = Gauge(
    "db_monitor_db_pool_size",
    "Configured database pool size"
)

db_pool_checked_in_connections = Gauge(
    "db_monitor_db_pool_checked_in_connections",
    "Number of checked-in database connections currently in the pool"
)

db_pool_overflow_connections = Gauge(
    "db_monitor_db_pool_overflow_connections",
    "Number of database connections created beyond the base pool size"
)

db_pool_utilization_ratio = Gauge(
    "db_monitor_db_pool_utilization_ratio",
    "Ratio of checked-out database connections to total pool capacity"
)

consumer_lag = Gauge(
    "db_monitor_consumer_lag",
    "Kafka consumer lag",
    ["topic", "partition"]
)

dlq_messages_total = Counter(
    "db_monitor_dlq_messages_total",
    "Total number of messages forwarded to the dead-letter queue"
)

dlq_records_pending = Gauge(
    "db_monitor_dlq_records_pending",
    "Number of persisted dead-letter records waiting for replay"
)

consumer_last_successful_commit_timestamp_seconds = Gauge(
    "db_monitor_consumer_last_successful_commit_timestamp_seconds",
    "Unix timestamp of the most recent successful Kafka commit"
)

tables_discovered_total = Gauge(
    "db_monitor_tables_discovered_total",
    "Total number of tables discovered"
)

columns_discovered_total = Gauge(
    "db_monitor_columns_discovered_total",
    "Total number of columns discovered"
)

circuit_breaker_state = Gauge(
    "db_monitor_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["breaker"]
)
