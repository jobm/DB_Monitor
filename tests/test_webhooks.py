from __future__ import annotations

import hashlib
import hmac
import json

import pytest

import webhooks
from webhooks import WebhookNotifier


@pytest.mark.anyio
async def test_webhook_notifier_posts_signed_payloads(monkeypatch) -> None:
    captured: list[tuple[str, bytes, dict[str, str]]] = []

    def fake_post_payload(
        self,
        url: str,
        payload_body: bytes,
    ) -> None:
        captured.append(
            (url, payload_body, dict(self._build_headers(payload_body)))
        )

    def fake_last_headers(self, payload_body: bytes) -> dict[str, str]:
        signature = hmac.new(
            b"top-secret",
            payload_body,
            hashlib.sha256,
        ).hexdigest()
        return {
            "Content-Type": "application/json",
            "User-Agent": "db-monitor-webhook/1.0",
            "X-DB-Monitor-Signature": f"sha256={signature}",
        }

    monkeypatch.setattr(WebhookNotifier, "_post_payload", fake_post_payload)
    monkeypatch.setattr(WebhookNotifier, "_build_headers", fake_last_headers)

    notifier = WebhookNotifier(
        urls=["https://hooks.example/a", "https://hooks.example/b"],
        timeout_seconds=5.0,
        shared_secret="top-secret",
    )

    await notifier.send_message({"type": "new_event", "event": {"id": 5}})

    assert [url for url, _body, _headers in captured] == [
        "https://hooks.example/a",
        "https://hooks.example/b",
    ]
    assert json.loads(captured[0][1].decode("utf-8")) == {
        "type": "new_event",
        "event": {"id": 5},
    }
    assert captured[0][2]["X-DB-Monitor-Signature"].startswith("sha256=")


@pytest.mark.anyio
async def test_webhook_notifier_noops_without_urls() -> None:
    notifier = WebhookNotifier(
        urls=[],
        timeout_seconds=5.0,
        shared_secret=None,
    )

    await notifier.send_message({"type": "new_event"})


@pytest.mark.anyio
async def test_webhook_notifier_retries_until_success(monkeypatch) -> None:
    attempts: list[str] = []

    def fake_post_payload(self, url: str, payload_body: bytes) -> None:
        del self
        del payload_body
        attempts.append(url)
        if len(attempts) < 3:
            raise RuntimeError("temporary failure")

    monkeypatch.setattr(WebhookNotifier, "_post_payload", fake_post_payload)
    monkeypatch.setattr(webhooks.time, "sleep", lambda _delay: None)

    notifier = WebhookNotifier(
        urls=["https://hooks.example/retry"],
        timeout_seconds=5.0,
        max_retries=2,
        retry_backoff_seconds=0.01,
        circuit_breaker_threshold=5,
        circuit_breaker_recovery_seconds=30.0,
    )

    await notifier.send_message({"type": "new_event"})

    assert len(attempts) == 3
    assert notifier._failure_counts["https://hooks.example/retry"] == 0


@pytest.mark.anyio
async def test_webhook_notifier_opens_circuit_after_repeated_failures(
    monkeypatch,
) -> None:
    attempts: list[str] = []

    def fake_post_payload(self, url: str, payload_body: bytes) -> None:
        del self
        del payload_body
        attempts.append(url)
        raise RuntimeError("permanent failure")

    monkeypatch.setattr(WebhookNotifier, "_post_payload", fake_post_payload)
    monkeypatch.setattr(webhooks.time, "sleep", lambda _delay: None)

    notifier = WebhookNotifier(
        urls=["https://hooks.example/circuit"],
        timeout_seconds=5.0,
        max_retries=0,
        retry_backoff_seconds=0.01,
        circuit_breaker_threshold=2,
        circuit_breaker_recovery_seconds=30.0,
    )

    await notifier.send_message({"type": "new_event"})
    await notifier.send_message({"type": "new_event"})
    await notifier.send_message({"type": "new_event"})

    assert len(attempts) == 2
    assert (
        notifier._circuit_opened_at["https://hooks.example/circuit"]
        is not None
    )
