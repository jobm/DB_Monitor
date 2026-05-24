"""Canonical ingestion pipeline exports."""

from __future__ import annotations

import importlib

from db_monitor.legacy_runtime import ensure_legacy_path

ensure_legacy_path()

_event_parser = importlib.import_module("event_parser")
_event_pipeline = importlib.import_module("event_pipeline")

parse_event_payload = _event_parser.parse_event_payload
EventPipeline = _event_pipeline.EventPipeline
EventPipelineConfig = _event_pipeline.EventPipelineConfig
event_pipeline = _event_pipeline.event_pipeline

__all__ = [
    "EventPipeline",
    "EventPipelineConfig",
    "event_pipeline",
    "parse_event_payload",
]
