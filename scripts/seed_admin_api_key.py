from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys

from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from auth import (  # noqa: E402
    build_api_key_expiration,
    generate_new_api_key,
    get_api_key_hash,
)
from extensions import AsyncSessionLocal  # noqa: E402
from models import ApiKey  # noqa: E402


async def create_admin_api_key(owner_name: str, ttl_days: int) -> str:
    """Create a short-lived admin API key and return it in client format."""
    raw_key = generate_new_api_key()
    key_hash = get_api_key_hash(raw_key)
    now = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        # Deactivate any currently active admin keys for this owner so this
        # command always returns exactly one fresh, usable key.
        existing_result = await session.execute(
            select(ApiKey).where(
                ApiKey.owner_name == owner_name,
                ApiKey.is_active.is_(True),
                ApiKey.role == "admin",
            )
        )
        for existing in existing_result.scalars().all():
            existing.is_active = False
            if existing.revoked_at is None:
                existing.revoked_at = now

        api_key = ApiKey(
            key_hash=key_hash,
            owner_name=owner_name,
            role="admin",
            is_active=True,
            expires_at=build_api_key_expiration(ttl_days),
        )
        session.add(api_key)
        await session.flush()
        composite_key = f"{api_key.id}.{raw_key}"
        await session.commit()

    return composite_key


def main() -> int:
    """Create a CI/local admin API key and print it to stdout."""
    parser = argparse.ArgumentParser(
        description="Create a short-lived admin API key for automation.",
    )
    parser.add_argument(
        "--owner-name",
        default="ci-smoke",
        help="Owner name recorded for the generated key.",
    )
    parser.add_argument(
        "--ttl-days",
        type=int,
        default=1,
        help="Expiration window for the generated key.",
    )
    args = parser.parse_args()

    print(asyncio.run(create_admin_api_key(args.owner_name, args.ttl_days)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
