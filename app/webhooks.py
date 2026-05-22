"""Outbound webhook delivery for persisted CDC events."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import urllib.request

from config import (
    WEBHOOK_SHARED_SECRET,
    WEBHOOK_TIMEOUT_SECONDS,
    WEBHOOK_URLS,
)

logger = logging.getLogger(__name__)


class WebhookNotifier:
    """Send best-effort webhook notifications for new events."""

    def __init__(
        self,
        urls: list[str],
        timeout_seconds: float,
        shared_secret: str | None = None,
    ) -> None:
        self._urls = list(urls)
        self._timeout_seconds = timeout_seconds
        self._shared_secret = shared_secret

    async def send_message(self, payload: dict[str, object]) -> None:
        """Deliver one event payload to all configured webhook targets."""
        if not self._urls:
            return

        payload_body = json.dumps(payload, separators=(",", ":")).encode(
            "utf-8"
        )
        await asyncio.gather(
            *[
                asyncio.to_thread(self._post_payload, url, payload_body)
                for url in self._urls
            ],
            return_exceptions=True,
        )

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
            logger.warning("Webhook delivery to %s failed: %s", url, exc)
            return

        if status_code >= 400:
            logger.warning(
                "Webhook delivery to %s returned %s",
                url,
                status_code,
            )


webhook_notifier = WebhookNotifier(
    urls=WEBHOOK_URLS,
    timeout_seconds=WEBHOOK_TIMEOUT_SECONDS,
    shared_secret=WEBHOOK_SHARED_SECRET,
)
