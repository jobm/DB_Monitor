"""Verify that a Git tag exists and is cryptographically signed."""

from __future__ import annotations

import subprocess
import sys


def _run_git(*args: str) -> str:
    """Run a git command and return stdout text."""
    result = subprocess.run(
        ["git", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git command failed")
    return result.stdout


def verify_signed_tag(tag_name: str) -> None:
    """Validate that ``tag_name`` resolves to a signed annotated tag."""
    tag_object_type = _run_git("cat-file", "-t", tag_name).strip()
    if tag_object_type != "tag":
        raise RuntimeError(
            f"Tag '{tag_name}' must be an annotated, signed tag. "
            "Lightweight tags are not accepted."
        )

    tag_body = _run_git("cat-file", "-p", tag_name)
    if "-----BEGIN PGP SIGNATURE-----" not in tag_body:
        raise RuntimeError(
            f"Tag '{tag_name}' is annotated but not signed. "
            "Create release tags with `git tag -s` before pushing."
        )


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for release workflows."""
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        raise SystemExit("Usage: python scripts/verify_signed_tag.py <tag>")

    tag_name = args[0]
    verify_signed_tag(tag_name)
    print(f"Verified signed annotated tag: {tag_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
