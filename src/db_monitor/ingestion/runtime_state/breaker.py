"""Circuit breaker for consumer resilience."""

from __future__ import annotations

import asyncio
import logging

from db_monitor.metrics import circuit_breaker_state

logger = logging.getLogger(__name__)

CONSUMER_METRIC_NAME = "broker_consumer"


class ConsumerStopRequested(RuntimeError):
    """Raised when the consumer must stop to avoid losing messages."""


class CircuitBreaker:
    """Track repeated failures and gate downstream execution attempts."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        metric_name: str = CONSUMER_METRIC_NAME,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"
        self.metric_name = metric_name
        self._update_metric()

    def record_failure(self) -> None:
        """Record a failed operation and open the breaker on threshold."""
        self.failure_count += 1
        self.last_failure_time = asyncio.get_event_loop().time()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            self._update_metric()
            logger.warning("Circuit breaker opened due to repeated failures")

    def record_success(self) -> None:
        """Reset breaker state after a successful operation."""
        self.failure_count = 0
        self.state = "closed"
        self._update_metric()

    def can_execute(self) -> bool:
        """Return whether processing is currently allowed."""
        if self.state == "closed":
            return True
        if (
            asyncio.get_event_loop().time() - self.last_failure_time
            > self.recovery_timeout
        ):
            self.state = "half-open"
            self._update_metric()
            logger.info("Circuit breaker half-open, allowing test request")
            return True
        return False

    def _update_metric(self) -> None:
        """Publish breaker state transitions to metrics."""
        state_value = {
            "closed": 0,
            "open": 1,
            "half-open": 2,
        }.get(self.state, 0)
        circuit_breaker_state.labels(breaker=self.metric_name).set(
            state_value
        )


circuit_breaker = CircuitBreaker()
cluster_circuit_breakers: dict[str, CircuitBreaker] = {
    "default": circuit_breaker,
}


def _get_circuit_breaker(cluster_name: str) -> CircuitBreaker:
    """Return the circuit breaker assigned to a cluster consumer."""
    breaker = cluster_circuit_breakers.get(cluster_name)
    if breaker is None:
        breaker = CircuitBreaker(
            metric_name=f"{CONSUMER_METRIC_NAME}:{cluster_name}"
        )
        cluster_circuit_breakers[cluster_name] = breaker
    return breaker
