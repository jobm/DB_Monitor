"""Compatibility wrapper for database engine and session helpers."""

from __future__ import annotations

import extensions as _extensions

AsyncSessionLocal = _extensions.AsyncSessionLocal
build_engine = _extensions.build_engine
engine = _extensions.engine
init_db = _extensions.init_db
