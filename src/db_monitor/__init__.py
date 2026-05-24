"""Top-level package for DB Monitor.

This package is the canonical namespace for distribution-focused imports.
During the current migration phase, runtime behavior is delegated to legacy
`app/` modules to preserve compatibility.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version("db-monitor")
except PackageNotFoundError:
    # Editable/local source checkouts may not have installed metadata.
    __version__ = "0.0.0"


__all__ = ["__version__"]
