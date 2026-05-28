#!/usr/bin/env python3
"""CLI helper to replay spilled audit log entries into the main audit table."""

import argparse
import asyncio
import logging
from typing import Optional

from db_monitor.repositories.audit_log_replay import replay_spill_entries

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_ids(ids_str: Optional[str]) -> Optional[list[int]]:
    if not ids_str:
        return None
    return [int(x.strip()) for x in ids_str.split(",") if x.strip()]


async def main_async(
    limit: int,
    ids: Optional[list[int]],
    actor: Optional[str],
) -> None:
    result = await replay_spill_entries(limit=limit, ids=ids, actor=actor)
    logger.info("Replay result: %s", result)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay spilled audit log entries",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max entries to replay",
    )
    parser.add_argument(
        "--ids",
        type=str,
        default=None,
        help="Comma-separated spill ids to replay",
    )
    parser.add_argument(
        "--actor",
        type=str,
        default=None,
        help="Actor name to record for the replay",
    )
    args = parser.parse_args()
    ids = parse_ids(args.ids)
    asyncio.run(main_async(args.limit, ids=ids, actor=args.actor))


if __name__ == "__main__":
    main()
