"""Compatibility wrapper for consumer runtime state."""

from __future__ import annotations

import consumer.runtime_state as _runtime_state

CircuitBreaker = _runtime_state.CircuitBreaker
ConsumerStopRequested = _runtime_state.ConsumerStopRequested
cluster_circuit_breakers = _runtime_state.cluster_circuit_breakers
consumer_runtime = _runtime_state.consumer_runtime
consumer_runtimes = _runtime_state.consumer_runtimes
get_consumer_health = _runtime_state.get_consumer_health
