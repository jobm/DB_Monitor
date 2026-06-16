#!/usr/bin/env python3
"""Check whether a TCP port is free on a given host.

Exit code 0 when the port is free, 1 when it is already in use.

Usage::

    python scripts/check_port_free.py 8000
    python scripts/check_port_free.py 8000 --host 127.0.0.1
    python scripts/check_port_free.py 8000 --timeout 3
"""

from __future__ import annotations

import argparse
import socket
import sys


def check_port(host: str, port: int, timeout: float) -> bool:
    """Return True if *port* is free (nothing is listening)."""
    with socket.socket() as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) != 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify that a TCP port is not already in use.",
    )
    parser.add_argument(
        "port",
        type=int,
        help="TCP port number to check.",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Hostname or IP to probe (default: localhost).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=2.0,
        help="Socket connect timeout in seconds (default: 2).",
    )
    args = parser.parse_args(argv)

    if check_port(args.host, args.port, args.timeout):
        print(f"Port {args.port} is free — OK.")
        return 0

    print(
        f"ERROR: Port {args.port} is already in use on {args.host}. "
        "Stop the conflicting process before continuing.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
