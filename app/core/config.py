"""Compatibility wrapper for application configuration."""

from __future__ import annotations

import config as _config


def __getattr__(name: str):
	"""Proxy attribute access to the legacy config module."""
	return getattr(_config, name)


def __dir__() -> list[str]:
	"""Expose proxied module attributes for introspection tools."""
	return sorted(set(globals()) | set(dir(_config)))
