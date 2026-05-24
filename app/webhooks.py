"""Outbound webhook delivery for persisted CDC events."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import urllib.request

from core.config import (
    WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS,
    WEBHOOK_CIRCUIT_BREAKER_THRESHOLD,
    WEBHOOK_MAX_RETRIES,
    WEBHOOK_RETRY_BACKOFF_SECONDS,
    WEBHOOK_SHARED_SECRET,
    WEBHOOK_TIMEOUT_SECONDS,
    WEBHOOK_URLS,
)
from metrics import (
    webhook_circuit_breaker_state,
    webhook_delivery_attempts_total,
)

logger = logging.getLogger(__name__)


class WebhookNotifier:
    """Send best-effort webhook notifications for new events."""

    def __init__(
        self,
        urls: list[str],
        timeout_seconds: float,
        shared_secret: str | None = None,
        max_retries: int = WEBHOOK_MAX_RETRIES,
        retry_backoff_seconds: float = WEBHOOK_RETRY_BACKOFF_SECONDS,
        circuit_breaker_threshold: int = WEBHOOK_CIRCUIT_BREAKER_THRESHOLD,
        circuit_breaker_recovery_seconds: float = (
            WEBHOOK_CIRCUIT_BREAKER_RECOVERY_SECONDS
        ),
    ) -> None:
        self._urls = list(urls)
        self._timeout_seconds = timeout_seconds
        self._shared_secret = shared_secret
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._circuit_breaker_threshold = circuit_breaker_threshold
        self._circuit_breaker_recovery_seconds = (
            circuit_breaker_recovery_seconds
        )
        self._failure_counts = {url: 0 for url in self._urls}
        self._circuit_opened_at = {url: None for url in self._urls}
        for url in self._urls:
            webhook_circuit_breaker_state.labels(url=url).set(0)

    async def send_message(
        self,
        payload: dict[str, object],
    ) -> list[dict[str, str]]:
        """Deliver one event payload to all configured webhook targets."""
        if not self._urls:
            return []

        payload_body = json.dumps(payload, separators=(",", ":")).encode(
            "utf-8"
        )
        delivery_results = await asyncio.gather(
            *[
                asyncio.to_thread(
                    self._deliver_with_retries,
                    url,
                    payload_body,
                )
                for url in self._urls
            ]
        )
        return [result for result in delivery_results if result is not None]

    def _circuit_is_open(self, url: str) -> bool:
        opened_at = self._circuit_opened_at.get(url)
        if opened_at is None:
            return False

        elapsed = time.monotonic() - opened_at
        if elapsed >= self._circuit_breaker_recovery_seconds:
            self._circuit_opened_at[url] = None
            webhook_circuit_breaker_state.labels(url=url).set(0)
            return False
        return True

    def _record_success(self, url: str) -> None:
        self._failure_counts[url] = 0
        self._circuit_opened_at[url] = None
        webhook_circuit_breaker_state.labels(url=url).set(0)

    def _record_failure(self, url: str) -> None:
        failure_count = int(self._failure_counts.get(url, 0)) + 1
        self._failure_counts[url] = failure_count
        if failure_count >= self._circuit_breaker_threshold:
            self._circuit_opened_at[url] = time.monotonic()
            webhook_circuit_breaker_state.labels(url=url).set(1)

    def _deliver_with_retries(
        self,
        url: str,
        payload_body: bytes,
    ) -> dict[str, str] | None:
        if self._circuit_is_open(url):
            webhook_delivery_attempts_total.labels(
                url=url,
                result="short_circuited",
            ).inc()
            logger.warning(
                "Webhook delivery to %s skipped because the circuit is open",
                url,
            )
            return {
                "url": url,
                "error": "circuit open",
            }

        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                webhook_delivery_attempts_total.labels(
                    url=url,
                    result="attempt",
                ).inc()
                self._post_payload(url, payload_body)
                self._record_success(url)
                webhook_delivery_attempts_total.labels(
                    url=url,
                    result="success",
                ).inc()
                return None
            except Exception as exc:
                last_exc = exc
                self._record_failure(url)
                webhook_delivery_attempts_total.labels(
                    url=url,
                    result="failure",
                ).inc()
                if attempt >= self._max_retries:
                    break
                time.sleep(self._retry_backoff_seconds * (2**attempt))

        logger.warning(
            "Webhook delivery to %s failed after %d attempts: %s",
            url,
            self._max_retries + 1,
            last_exc,
        )
        return {
            "url": url,
            "error": str(last_exc) if last_exc is not None else "unknown",
        }

    def _build_headers(self, payload_body: bytes) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "db-monitor-webhook/1.0",
        }
        if self._shared_secret:
            signature = hmac.new(
                self._shared_secret.encode("utf-8"),
                payload_body,
                hashlib.sha256,
            ).hexdigest()
            headers["X-DB-Monitor-Signature"] = f"sha256={signature}"
        return headers

    def _post_payload(self, url: str, payload_body: bytes) -> None:
        request = urllib.request.Request(
            url,
            data=payload_body,
            headers=self._build_headers(payload_body),
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._timeout_seconds,
            ) as response:
                status_code = response.getcode()
        except Exception as exc:
            raise RuntimeError(
                f"Webhook delivery to {url} failed: {exc}"
            ) from exc

        if status_code >= 400:
            raise RuntimeError(
                f"Webhook delivery to {url} returned {status_code}"
            )


webhook_notifier = WebhookNotifier(
    urls=WEBHOOK_URLS,
    timeout_seconds=WEBHOOK_TIMEOUT_SECONDS,
    shared_secret=WEBHOOK_SHARED_SECRET,
)
