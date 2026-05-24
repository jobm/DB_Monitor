"""Compatibility wrapper for event filtering and transformation."""

from __future__ import annotations

import event_pipeline as _event_pipeline

EventPipeline = _event_pipeline.EventPipeline
EventPipelineConfig = _event_pipeline.EventPipelineConfig
event_pipeline = _event_pipeline.event_pipeline
