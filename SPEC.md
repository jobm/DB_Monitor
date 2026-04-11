# DB Monitor - Technical Specification

**Version:** 1.0  
**Date:** 2026-02-27  
**Status:** Draft for Review

---

## 1. Executive Summary

**Purpose:** Real-time database audit and change tracking system for microservices and multi-DB environments.

**Current State:** Foundation exists (FastAPI + Kafka + Debezium + PostgreSQL). Functional for basic event capture but missing critical capabilities for audit/compliance use cases.

**Target State:** Production-ready audit system with table/column visibility, temporal change tracking, and visualization capabilities.

---

## 2. Gap Analysis

| Capability | Current | Required | Priority |
|------------|---------|----------|----------|
| Schema registry (what tables/columns exist) | ❌ | ✅ | P0 |
| Column-level change tracking | ❌ | ✅ | P0 |
| Temporal queries (value at time T) | ❌ | ✅ | P0 |
| Multi-topic/multi-service support | ⚠️ | ✅ | P0 |
| Horizontal scaling | ❌ | ✅ | P1 |
| Dead-letter queue (DLQ) | ❌ | ✅ | P1 |
| Observability (metrics, tracing) | ❌ | ✅ | P1 |
| Batch processing | ❌ | ✅ | P1 |
| Visualization/API filtering | ⚠️ | ✅ | P2 |

---

## 3. Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Source Databases                         │
│   (Order, Catalog, Shipping, + any number of services)         │
└─────────────────────────┬───────────────────────────────────────┘
                          │ Debezium CDC
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka (Message Bus)                         │
│  Topics: {service}.{db}.{table} - one per monitored table      │
└─────────────────────────┬───────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ Consumer      │  │ Consumer      │  │ Consumer      │  ← Horizontal
│ Instance 1    │  │ Instance 2    │  │ Instance N    │    Scale-out
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Monitor Database                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ schema_catalog│  │ column_changes│  │ events       │         │
│  │ (tables)     │  │ (history)     │  │ (raw events) │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI API                               │
│  /tables, /tables/{name}/columns, /changes, /events             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Model

### 4.1 Schema Catalog (NEW)

```python
class MonitoredTable(Base):
    __tablename__ = "monitored_tables"
    
    id = Column(Integer, primary_key=True)
    service_name = Column(String(128), nullable=False)  # e.g., "order", "catalog"
    database_name = Column(String(128), nullable=False) # e.g., "orderdb"
    table_name = Column(String(128), nullable=False)    # e.g., "orders"
    topic_name = Column(String(256), nullable=False)    # Kafka topic
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (
        Index("ix_tables_service_db_table", "service_name", "database_name", "table_name", unique=True),
    )


class MonitoredColumn(Base):
    __tablename__ = "monitored_columns"
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey("monitored_tables.id"), nullable=False)
    column_name = Column(String(128), nullable=False)
    data_type = Column(String(64), nullable=False)
    is_primary_key = Column(Boolean, default=False)
    is_nullable = Column(Boolean, default=True)
    audit_enabled = Column(Boolean, default=True)  # Track changes?
    
    __table_args__ = (
        Index("ix_columns_table_id", "table_id"),
    )
```

### 4.2 Column Change History (NEW)

```python
class ColumnChange(Base):
    __tablename__ = "column_changes"
    
    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    table_id = Column(Integer, ForeignKey("monitored_tables.id"), nullable=False)
    column_id = Column(Integer, ForeignKey("monitored_columns.id"), nullable=False)
    
    operation = Column(String(16), nullable=False)  # INSERT, UPDATE, DELETE
    old_value = Column(JSON, nullable=True)          # Previous value
    new_value = Column(JSON, nullable=True)          # New value
    
    changed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    
    __table_args__ = (
        Index("ix_changes_table_col_time", "table_id", "column_id", "changed_at"),
    )
```

### 4.3 Existing Events (RETAIN + extend)

Keep `KafkaEvent` table, add:
- `source_table_id` - FK to `MonitoredTable`
- `operation` - INSERT/UPDATE/DELETE

---

## 5. API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /tables` | List all monitored tables |
| `GET /tables/{service}/{table}` | Get table schema (columns, types) |
| `GET /tables/{service}/{table}/columns` | Column details |
| `GET /changes` | Query column changes with filters |
| `GET /changes/{table}/{column}` | History of changes for specific column |
| `GET /changes/{table}/{column}/at?timestamp=T` | Value at specific time |
| `GET /events` | Raw events (retain existing) |
| `GET /events/stats` | Aggregated statistics |

---

## 6. Prioritized Tasks

### Phase 1: Core Schema & Discovery (Week 1-2)

| # | Task | Description |
|---|------|-------------|
| 1.1 | **Schema Catalog Model** | Add `MonitoredTable`, `MonitoredColumn` models |
| 1.2 | **Auto-discovery** | Parse Debezium messages to extract schema, auto-register tables/columns |
| 1.3 | **Discovery API** | Implement `/tables` endpoints |
| 1.4 | **Schema Indexing** | Add proper DB indexes for catalog queries |

### Phase 2: Change Tracking (Week 2-3)

| # | Task | Description |
|---|------|-------------|
| 2.1 | **Column Change Model** | Add `ColumnChange` model with indexes |
| 2.2 | **Change Processor** | Extract column-level deltas from Debezium payloads |
| 2.3 | **Temporal Queries** | Implement `/changes` with time-based filtering |
| 2.4 | **Point-in-time Lookup** | "What was value at T?" query logic |

### Phase 3: Resilience & Scale (Week 3-4)

| # | Task | Description |
|---|------|-------------|
| 3.1 | **Multi-topic Consumer** | Subscribe to all monitored topics dynamically |
| 3.2 | **Horizontal Scaling** | Consumer group for parallel processing |
| 3.3 | **Dead-letter Queue** | Kafka DLQ topic for failed messages |
| 3.4 | **Circuit Breaker** | Prevent cascade failures on DB/Kafka issues |
| 3.5 | **Batch Processing** | Bulk inserts for high-throughput scenarios |

### Phase 4: Observability (Week 4-5)

| # | Task | Description |
|---|------|-------------|
| 4.1 | **Prometheus Metrics** | Request latency, event throughput, consumer lag |
| 4.2 | **Structured Logging** | JSON logs with correlation IDs |
| 4.3 | **Health Checks** | Deep health (DB, Kafka connectivity) |
| 4.4 | **Tracing** | OpenTelemetry integration |

### Phase 5: Visualization & UX (Week 5-6)

| # | Task | Description |
|---|------|-------------|
| 5.1 | **API Filtering** | Pagination, sorting, field selection |
| 5.2 | **Date Range Queries** | Filter by time range |
| 5.3 | **Aggregation Stats** | Count by table, operation type, time |
| 5.4 | **Simple Dashboard** | (Optional) Basic UI or Grafana datasource |

---

## 7. Infrastructure Recommendations

### 7.1 Resilience

| Tool | Purpose | When to Add |
|------|---------|-------------|
| **Dead-letter Queue (Kafka)** | Failed message handling | Phase 3 |
| **Circuit Breaker** (Python `circuitbreaker`) | DB/Kafka failure protection | Phase 3 |
| **Retry with Backoff** | Already implemented ✅ | - |
| **Graceful Shutdown** | Already implemented ✅ | - |
| **Health Checks** | Already implemented (basic) | Enhance in Phase 4 |

### 7.2 Scalability

| Tool | Purpose | When to Add |
|------|---------|-------------|
| **Kafka Consumer Groups** | Horizontal scale | Phase 3 (minimal change) |
| **Connection Pooling** | Already using asyncpg ✅ | - |
| **Batch Inserts** | Throughput optimization | Phase 3 |
| **Read Replicas** | Query scaling | If API latency high |
| **Redis Cache** | Frequent queries | Phase 2+ if needed |

### 7.3 Observability

| Tool | Purpose | When to Add |
|------|---------|-------------|
| **Prometheus + Metrics & dashboards | Phase 4 Grafana** | |
| **OpenTelemetry** | Distributed tracing | Phase 4 |
| **ELK Stack** | Log aggregation | Phase 4 |

### 7.4 Optional Enhancements (Later)

| Tool | Use Case |
|------|----------|
| **Apache Iceberg / Delta Lake** | Time-travel queries on raw data |
| **ClickHouse** | High-speed analytics on audit data |
| **Grafana** | Pre-built dashboards |

---

## 8. Immediate Next Steps

1. **Start Phase 1.1** - Create schema catalog models
2. **Decide** - Auto-discover tables from Kafka or configure via YAML/env?
3. **Decide** - Keep PostgreSQL or consider ClickHouse for change history?

---

## 9. Success Criteria

- [x] Can list all monitored tables across all services
- [x] Can see column definitions (name, type, PK)
- [x] Can query "what changed in table X between T1 and T2"
- [x] Can query "what was value of column Y at time T"
- [x] System handles 10K+ events/second without degradation (batch processing)
- [x] Failed messages go to DLQ, not lost
- [x] Horizontal scaling works (multiple consumer instances via consumer group)

---

## Implementation Status

### Completed (v1.0)

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1: Schema & Discovery | ✅ | Schema discovery from Debezium, /tables API |
| Phase 2: Change Tracking | ✅ | ColumnChange model, change processor, temporal queries |
| Phase 3: Resilience & Scale | ✅ | Multi-topic, DLQ, circuit breaker, batch processing |
| Phase 4: Observability | ✅ | Prometheus metrics, structured logging |
| Phase 5: API & UX | ✅ | Pagination, filtering, stats endpoint |
