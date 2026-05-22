from __future__ import annotations

import hashlib
import hmac
import json

import pytest

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
