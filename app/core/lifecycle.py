"""Compatibility wrapper for lifecycle management."""

from __future__ import annotations

import lifecycle_manager as _lifecycle_manager

ApplicationLifecycleManager = _lifecycle_manager.ApplicationLifecycleManager
GracefulShutdownManager = _lifecycle_manager.GracefulShutdownManager
lifecycle_manager = _lifecycle_manager.lifecycle_manager
