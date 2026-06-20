"""Smoke-test a list of HTTP endpoints and report their status.

Exit code 0 when every endpoint returns HTTP 200, 1 otherwise.

Usage::

    python scripts/smoke_test_endpoints.py http://localhost:8000 /health /livez
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request


def smoke_test_endpoint(
    url: str,
    timeout: float = 5.0,
    expect_json: bool = True,
) -> tuple[bool, str]:
    """Probe a single URL and return ``(ok, detail)``.

    *ok* is ``True`` when the endpoint returns HTTP 200.  When
    *expect_json* is ``True`` (the default) the response body is
    parsed as JSON and the ``status`` field is reported as *detail*;
    otherwise the Content-Type header is reported instead.
    """
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read().decode()
            if resp.status != 200:
                return False, f"HTTP {resp.status}"
            if expect_json:
                body = json.loads(raw)
                return True, body.get("status", "unknown")
            content_type = resp.headers.get("Content-Type", "unknown")
            return True, f"HTTP 200 ({content_type})"
    except Exception as exc:
        return False, str(exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "base_url",
        help="Base URL of the running application (e.g. http://localhost:8000).",
    )
    parser.add_argument(
        "endpoints",
        nargs="*",
        help="JSON endpoints to probe (e.g. /health /livez).",
    )
    parser.add_argument(
        "--text-endpoints",
        nargs="*",
        default=[],
        help=(
            "Non-JSON endpoints to probe for HTTP 200 only "
            "(e.g. /metrics)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-request timeout in seconds (default: 5).",
    )
    args = parser.parse_args(argv)

    failed: list[str] = []

    for path in args.endpoints:
        url = args.base_url.rstrip("/") + path
        ok, detail = smoke_test_endpoint(
            url, timeout=args.timeout, expect_json=True,
        )
        if ok:
            print(f"OK   {path}: {detail}")
        else:
            print(f"FAIL {path}: {detail}")
            failed.append(path)

    for path in args.text_endpoints:
        url = args.base_url.rstrip("/") + path
        ok, detail = smoke_test_endpoint(
            url, timeout=args.timeout, expect_json=False,
        )
        if ok:
            print(f"OK   {path}: {detail}")
        else:
            print(f"FAIL {path}: {detail}")
            failed.append(path)

    if failed:
        print(
            f"\nSmoke test failed for: {', '.join(failed)}",
            file=sys.stderr,
        )
        return 1

    print("\nAll smoke endpoints OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
