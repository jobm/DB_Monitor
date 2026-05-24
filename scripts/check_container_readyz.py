"""Validate monitor-server readiness endpoint semantics for CI smoke tests."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request


def main() -> int:
    """Poll /readyz until it returns a valid readiness payload."""
    url = "http://localhost:8000/readyz"
    deadline = time.time() + 240
    last_error: Exception | None = None
    attempt = 0

    print(f"Checking readiness endpoint semantics at {url}")

    while time.time() < deadline:
        attempt += 1
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                payload = json.loads(response.read().decode())
                print(
                    f"Attempt {attempt}: HTTP {response.status} "
                    f"status={payload.get('status')}"
                )
                if payload.get("status") in {"ready", "not_ready"}:
                    return 0
        except urllib.error.HTTPError as exc:
            if exc.code == 503:
                payload = json.loads(exc.read().decode())
                print(
                    f"Attempt {attempt}: HTTP {exc.code} "
                    f"status={payload.get('status')}"
                )
                if payload.get("status") == "not_ready":
                    return 0
            last_error = exc
        except Exception as exc:
            print(f"Attempt {attempt}: request failed: {exc}")
            last_error = exc

        time.sleep(2)

    raise SystemExit(
        f"Timed out waiting for readiness endpoint semantics at {url}: "
        f"{last_error}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
