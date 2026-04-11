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

active_connections = Gauge(
    "db_monitor_active_connections",
    "Number of active database connections"
)

consumer_lag = Gauge(
    "db_monitor_consumer_lag",
    "Kafka consumer lag",
    ["topic", "partition"]
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
