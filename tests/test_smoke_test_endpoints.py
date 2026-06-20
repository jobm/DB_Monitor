"""Unit tests for the ``smoke_test_endpoints`` CLI script."""

from __future__ import annotations

import http.server
import json
import threading
import time

import scripts.smoke_test_endpoints as smoke_test_endpoints


class _JsonHandler(http.server.BaseHTTPRequestHandler):
    """Minimal handler that returns a JSON health payload."""

    status_code: int = 200
    payload: dict[str, str] = {"status": "healthy"}

    def do_GET(self) -> None:  # noqa: N802
        body = json.dumps(self.payload).encode()
        self.send_response(self.status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002,D102
        pass  # suppress noisy request logs


class _TextHandler(http.server.BaseHTTPRequestHandler):
    """Minimal handler that returns a plain-text body."""

    status_code: int = 200
    body: str = "# HELP uptime Uptime\n# TYPE uptime gauge\nuptime 42\n"

    def do_GET(self) -> None:  # noqa: N802
        raw = self.body.encode()
        self.send_response(self.status_code)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002,D102
        pass


class _SlowHandler(http.server.BaseHTTPRequestHandler):
    """Handler that accepts connections but never writes a response."""

    def do_GET(self) -> None:  # noqa: N802
        time.sleep(2)  # well above the 0.3s test timeout

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002,D102
        pass


def _start_json_server(
    status_code: int = 200,
    payload: dict[str, str] | None = None,
) -> tuple[http.server.HTTPServer, int]:
    """Spin up a tiny JSON HTTP server and return ``(server, port)``."""
    _JsonHandler.status_code = status_code
    _JsonHandler.payload = payload or {"status": "healthy"}
    server = http.server.HTTPServer(("127.0.0.1", 0), _JsonHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def _start_text_server(
    status_code: int = 200,
    body: str | None = None,
) -> tuple[http.server.HTTPServer, int]:
    """Spin up a tiny text HTTP server and return ``(server, port)``."""
    _TextHandler.status_code = status_code
    _TextHandler.body = body or "# HELP uptime 42\n"
    server = http.server.HTTPServer(("127.0.0.1", 0), _TextHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


# ── smoke_test_endpoint unit tests ───────────────────────────────


def test_smoke_test_endpoint_json_ok() -> None:
    server, port = _start_json_server()
    try:
        ok, detail = smoke_test_endpoints.smoke_test_endpoint(
            f"http://127.0.0.1:{port}/health",
        )
        assert ok is True
        assert detail == "healthy"
    finally:
        server.shutdown()


def test_smoke_test_endpoint_json_non_200() -> None:
    server, port = _start_json_server(status_code=503)
    try:
        ok, detail = smoke_test_endpoints.smoke_test_endpoint(
            f"http://127.0.0.1:{port}/health",
        )
        assert ok is False
        assert "503" in detail
    finally:
        server.shutdown()


def test_smoke_test_endpoint_text_ok() -> None:
    server, port = _start_text_server()
    try:
        ok, detail = smoke_test_endpoints.smoke_test_endpoint(
            f"http://127.0.0.1:{port}/metrics",
            expect_json=False,
        )
        assert ok is True
        assert "text/plain" in detail
    finally:
        server.shutdown()


def test_smoke_test_endpoint_text_non_200() -> None:
    server, port = _start_text_server(status_code=500)
    try:
        ok, detail = smoke_test_endpoints.smoke_test_endpoint(
            f"http://127.0.0.1:{port}/metrics",
            expect_json=False,
        )
        assert ok is False
        assert "500" in detail
    finally:
        server.shutdown()


def test_smoke_test_endpoint_malformed_json() -> None:
    server, port = _start_text_server(body="not json at all")
    try:
        ok, detail = smoke_test_endpoints.smoke_test_endpoint(
            f"http://127.0.0.1:{port}/health",
            expect_json=True,
        )
        assert ok is False
        assert "Expecting value" in detail or "JSONDecodeError" in detail
    finally:
        server.shutdown()


def test_smoke_test_endpoint_empty_body() -> None:
    server, port = _start_text_server(body="")
    try:
        ok, detail = smoke_test_endpoints.smoke_test_endpoint(
            f"http://127.0.0.1:{port}/health",
            expect_json=True,
        )
        assert ok is False
        assert "Expecting value" in detail or "JSONDecodeError" in detail
    finally:
        server.shutdown()


def test_smoke_test_endpoint_timeout() -> None:
    """Server accepts connection but never sends a response."""
    server = http.server.HTTPServer(
        ("127.0.0.1", 0), _SlowHandler,
    )
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        ok, detail = smoke_test_endpoints.smoke_test_endpoint(
            f"http://127.0.0.1:{port}/health",
            timeout=0.3,
        )
        assert ok is False
        assert "timed out" in detail.lower() or "timeout" in detail.lower()
    finally:
        server.shutdown()


def test_smoke_test_endpoint_connection_refused() -> None:
    ok, detail = smoke_test_endpoints.smoke_test_endpoint(
        "http://127.0.0.1:19999/health",
        timeout=1.0,
    )
    assert ok is False
    assert "Connection refused" in detail or "Errno" in detail


# ── main() CLI tests ────────────────────────────────────────────


def test_main_returns_zero_when_all_endpoints_ok() -> None:
    server, port = _start_json_server()
    try:
        rc = smoke_test_endpoints.main(
            [f"http://127.0.0.1:{port}", "/health"],
        )
        assert rc == 0
    finally:
        server.shutdown()


def test_main_returns_one_when_any_endpoint_fails() -> None:
    rc = smoke_test_endpoints.main(
        ["http://127.0.0.1:19999", "/health"],
    )
    assert rc == 1


def test_main_returns_zero_for_multiple_ok_endpoints() -> None:
    server, port = _start_json_server()
    try:
        rc = smoke_test_endpoints.main(
            [f"http://127.0.0.1:{port}", "/health", "/livez"],
        )
        assert rc == 0
    finally:
        server.shutdown()


def test_main_returns_zero_for_text_endpoint() -> None:
    server, port = _start_text_server()
    try:
        rc = smoke_test_endpoints.main(
            [
                f"http://127.0.0.1:{port}",
                "--text-endpoints",
                "/metrics",
            ],
        )
        assert rc == 0
    finally:
        server.shutdown()


def test_main_returns_one_when_text_endpoint_fails() -> None:
    rc = smoke_test_endpoints.main(
        ["http://127.0.0.1:19999", "--text-endpoints", "/metrics"],
    )
    assert rc == 1


def test_main_mixed_json_and_text_endpoints() -> None:
    json_server, json_port = _start_json_server()
    text_server, text_port = _start_text_server()
    try:
        # Use the JSON server for both; /metrics on a JSON server still
        # returns HTTP 200 so the text check passes.
        rc = smoke_test_endpoints.main(
            [
                f"http://127.0.0.1:{json_port}",
                "/health",
                "--text-endpoints",
                "/metrics",
            ],
        )
        assert rc == 0
    finally:
        json_server.shutdown()
        text_server.shutdown()
