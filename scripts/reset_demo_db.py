"""Reset DB Monitor demo data so each run starts from a clean slate.

Usage (local Compose):
    DEMO_MODE=1 python scripts/reset_demo_db.py --yes
    # or: make monitor-demo-reset

Usage (Kind, piped into the API pod):
    kubectl ... exec -i deploy/...-db-monitor-api -- \
      env DEMO_MODE=1 python - < scripts/reset_demo_db.py -- --yes

Guarded by DEMO_MODE=1 (or --force) to prevent accidental data loss.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from sqlalchemy import text

from db_monitor.extensions import AsyncSessionLocal


DEMO_TABLES = [
    "monitored_columns",
    "monitored_tables",
    "column_changes",
    "events",
    "dead_letter_events",
    "consumer_checkpoints",
]


async def reset() -> None:
    async with AsyncSessionLocal() as session:
        for table in DEMO_TABLES:
            await session.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        await session.commit()
    print(f"reset {len(DEMO_TABLES)} tables: {', '.join(DEMO_TABLES)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset DB Monitor demo data.")
    parser.add_argument("--yes", action="store_true", help="Confirm the reset.")
    parser.add_argument("--force", action="store_true", help="Bypass DEMO_MODE guard.")
    args = parser.parse_args()

    if not (args.force or os.environ.get("DEMO_MODE") == "1"):
        print("Refusing: set DEMO_MODE=1 or pass --force.", file=sys.stderr)
        return 2
    if not args.yes:
        print("Refusing: pass --yes to confirm.", file=sys.stderr)
        return 2

    asyncio.run(reset())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
