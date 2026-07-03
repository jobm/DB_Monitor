"""Top-level package for DB Monitor.

This package is the canonical namespace for distribution-focused imports.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

# Static fallback version — used when importlib.metadata is unavailable
# (e.g., editable installs, local checkouts). Updated during the release
# process.
__version__: str = "0.0.1"

try:
    # Prefer the installed-package version when available
    __version__ = version("db-monitor")
except PackageNotFoundError:
    pass


__all__ = ["__version__"]
