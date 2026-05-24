"""Allow `python -m db_monitor` execution."""

from __future__ import annotations

from db_monitor.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
