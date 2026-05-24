"""Compatibility wrapper for API response models."""

from __future__ import annotations

import response_models as _response_models

AccessTokenExchangeResponse = _response_models.AccessTokenExchangeResponse
AppInfoResponse = _response_models.AppInfoResponse
ChangesResponse = _response_models.ChangesResponse
EventsResponse = _response_models.EventsResponse
HealthResponse = _response_models.HealthResponse
PointInTimeValueResponse = _response_models.PointInTimeValueResponse
ReadinessResponse = _response_models.ReadinessResponse
TableColumnsResponse = _response_models.TableColumnsResponse
TableListResponse = _response_models.TableListResponse
WebSocketTokenExchangeResponse = (
    _response_models.WebSocketTokenExchangeResponse
)
