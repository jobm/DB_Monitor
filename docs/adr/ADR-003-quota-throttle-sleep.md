# ADR-003: Ingestion Quota Throttle via Internal Sleep Loop

**Status:** Implemented  
**Date:** 2026-06-23  
**Deciders:** Project leads  
**Context:** [Ingestion quota configuration](../../docs/configuration.md#ingestion-quota-and-throttling-settings)

---

## Context

DB Monitor supports configurable ingestion quotas to protect downstream storage
and API latency during traffic spikes. When a source or tenant exceeds its
per-window event budget, the system must decide what to do.

The two obvious strategies are:

1. **Drop mode:** Acknowledge and discard the event without writing to the DB.
2. **Throttle mode:** Delay processing until the quota window resets, then
   allow the event through.

For throttle mode, the question was *how* to delay: internally (sleep the
consumer task) or externally (push back on the broker by pausing partition
consumption).

## Decision

Use an internal sleep loop for throttle-mode quota enforcement, with
per-chunk yields to allow event-loop interleaving.

### Why not broker pushback?

- **Kafka `pause()`/`resume()`:** Pausing a partition stops *all* consumption
  on that partition, not just over-quota sources. A single over-quota source
  would stall other sources sharing the same partition.
- **RabbitMQ consumer prefetch reduction:** Similar problem — throttling one
  source affects others on the same channel.
- **Broker-specific complexity:** Each broker has a different pushback
  mechanism. An internal sleep loop is broker-agnostic.
- **External coordination:** Pushback would require per-source quota state to
  be communicated back to the broker, which adds latency and complexity.

### Why not a simple `asyncio.sleep(window_remaining)`?

- A naive `sleep(remaining_seconds)` would stall the entire consumer task,
  blocking all sources. The sleep loop divides the wait into small chunks
  (bounded by `INGESTION_QUOTA_MAX_THROTTLE_SECONDS`) so the consumer can
  process events from other sources between chunks.

### Throttle loop design

```text
Event arrives → Quota exceeded?
  No  → Process immediately
  Yes → Enter sleep loop:
         while window_remaining > 0:
           chunk = min(window_remaining, max_throttle_seconds)
           await asyncio.sleep(chunk)
           window_remaining -= chunk
         Retry quota check → Process if allowed now
```

### Rejected alternatives

- **Drop mode as the only option:** Usable for fail-fast but unacceptable for
  users who want to absorb spikes without data loss.
- **Rate-limiter library (e.g., `aiolimiter`):** Would add a dependency and
  still need coordination across replicas.
- **Dedicated quota service:** Over-engineered for the current scale; can be
  introduced later if cross-replica coordination becomes necessary.

## Consequences

**Positive:**

- Simple, broker-agnostic throttle mechanism.
- Per-source granularity — one source being throttled does not block others.
- Predictable resource usage — the sleep loop is pure async, no threads.

**Negative:**

- The consumer task's event loop is occupied during sleep chunks; very long
  throttle windows could delay processing of non-throttled events.
- No cross-replica coordination — each replica maintains its own quota window
  counter, so a rapid failover could briefly double the effective window.

**Mitigations:**

- `INGESTION_QUOTA_MAX_THROTTLE_SECONDS` bounds the sleep chunk size.
- Quota windows use `(window_start, current_count)` stored in DB, so a
  restart resets the window to the current time — bounded burst only.

## Compliance

- [x] Sleep-loop throttle implementation in `src/db_monitor/ingestion/quota.py`
- [x] Configurable `INGESTION_QUOTA_MAX_THROTTLE_SECONDS`
- [x] Property-based tests verify monotonic quota decisions and bounded sleep
- [x] Drop mode as alternative for fail-fast deployments
