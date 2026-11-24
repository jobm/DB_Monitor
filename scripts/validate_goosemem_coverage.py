#!/usr/bin/env python3
"""Verify every module under src/db_monitor is referenced by .goosemem.

Coverage is block-scoped: a module counts as referenced only if a single
block either contains its ``package/file.py`` path, or contains both its
package path and its filename.  That mirrors how Goose actually uses the
file -- only the retrieved block is in context, so a reference split across
two blocks is not a reference.

``__init__.py`` is exempt: it marks a package, and the package directory is
already required to be named.  ``__main__.py`` is not exempt.

Exit codes:
    2 -- .goosemem or the source tree is missing
    1 -- modules are not referenced
    0 -- every non-``__init__`` module is referenced
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

DEFAULT_MEM = Path(".goosemem")
DEFAULT_SRC = Path("src/db_monitor")
# A block starts at its '#tag #tag ...' line; the separator rules around it
# are formatting, so coverage keys off the tag line instead.
TAG_LINE = re.compile(r"^#[a-z0-9]")
TOKEN_CHARS = "A-Za-z0-9_"


def boundary_pattern(text: str) -> re.Pattern[str]:
    """Match ``text`` delimited by non-identifier characters."""
    return re.compile(
        rf"(?<![{TOKEN_CHARS}]){re.escape(text)}(?![{TOKEN_CHARS}])"
    )


def load_blocks(mem_path: Path) -> list[str]:
    """Split .goosemem into blocks, each including its tag header."""
    text = mem_path.read_text(encoding="utf-8")
    blocks: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        if TAG_LINE.match(line):
            if current:
                blocks.append("\n".join(current))
            current = [line]
        elif current:
            current.append(line)
    if current:
        blocks.append("\n".join(current))
    return blocks


def iter_modules(src_dir: Path) -> list[Path]:
    """Return Python modules under ``src_dir``, excluding caches."""
    skip = {"__pycache__", ".venv", "site-packages"}
    modules = []
    for path in src_dir.rglob("*.py"):
        if skip.intersection(path.parts):
            continue
        modules.append(path)
    return sorted(modules)


def referenced(block: str, rel: Path) -> bool:
    """Return True if ``block`` names this module."""
    if boundary_pattern(rel.as_posix()).search(block):
        return True
    parent = rel.parent.as_posix()
    if parent == ".":
        return False
    return bool(
        boundary_pattern(parent).search(block)
        and boundary_pattern(rel.name).search(block)
    )


def check(mem_path: Path, src_dir: Path, verbose: bool) -> int:
    if not mem_path.is_file():
        print(f"ERROR: {mem_path} not found", file=sys.stderr)
        return 2
    if not src_dir.is_dir():
        print(
            f"ERROR: {src_dir} not found (run from the repository root)",
            file=sys.stderr,
        )
        return 2

    blocks = load_blocks(mem_path)
    modules = iter_modules(src_dir)
    checked = 0
    exempt = 0
    gaps: list[Path] = []

    for path in modules:
        rel = path.relative_to(src_dir)
        if rel.name == "__init__.py":
            exempt += 1
            continue
        checked += 1
        if not any(referenced(block, rel) for block in blocks):
            gaps.append(rel)

    dirs = {rel.parent for rel in
            (p.relative_to(src_dir) for p in modules)}
    covered_dirs = sum(
        1
        for rel_dir in dirs
        if rel_dir == Path(".")
        or any(boundary_pattern(rel_dir.as_posix()).search(b)
               for b in blocks)
    )

    print(f"=== .goosemem module coverage ({src_dir}) ===")
    print()
    for gap in gaps:
        print(f"  GAP: {src_dir}/{gap.as_posix()}")
    if gaps:
        print()

    print("--- Stats ---")
    print(f"  Blocks:          {len(blocks)}")
    print(f"  Lines:           {len(mem_path.read_text().splitlines())}")
    print(f"  Bytes:           {mem_path.stat().st_size}")
    print(f"  Modules:         {checked - len(gaps)}/{checked} referenced")
    print(f"  __init__ files:  {exempt} exempt (package markers)")
    print(f"  Directories:     {covered_dirs}/{len(dirs)} covered")
    print()

    if gaps:
        count = len(gaps)
        noun = "module" if count == 1 else "modules"
        print(f"FAIL: {count} {noun} not referenced inside a single block.")
        print("Add it to the FILES line of the block owning its package.")
        return 1

    print("PASS: every module is referenced by its owning block.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check .goosemem module coverage."
    )
    parser.add_argument("--mem", type=Path, default=DEFAULT_MEM)
    parser.add_argument("--src", type=Path, default=DEFAULT_SRC)
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="reserved"
    )
    args = parser.parse_args()
    return check(args.mem, args.src, args.verbose)


if __name__ == "__main__":
    raise SystemExit(main())
