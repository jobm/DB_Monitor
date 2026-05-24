"""Helpers for bridging legacy app imports to the src package namespace."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_src_namespace_path() -> None:
    """Ensure repository src path is importable for db_monitor namespace."""
    src_root = Path(__file__).resolve().parents[1] / "src"
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)
