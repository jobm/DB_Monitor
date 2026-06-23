"""OpenTelemetry setup helpers for DB Monitor."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator


_instrumented = False


def _parse_otlp_headers(raw_headers: str | None) -> dict[str, str] | None:
    if not raw_headers:
        return None

    headers: dict[str, str] = {}
    for pair in raw_headers.split(","):
        key, separator, value = pair.partition("=")
        normalized_key = key.strip()
        normalized_value = value.strip()
        if not separator or not normalized_key or not normalized_value:
            raise ValueError(
                "Invalid OTEL_EXPORTER_OTLP_HEADERS format. "
                "Use comma-separated key=value pairs."
            )
        headers[normalized_key] = normalized_value
    return headers


def _set_span_attributes(span: Any, attributes: dict[str, object]) -> None:
    for key, value in attributes.items():
        if value is None:
            continue
        span.set_attribute(key, value)


def initialize_tracing(
    *,
    app: Any,
    engine: Any,
    service_name: str,
    exporter: str,
    otlp_endpoint: str | None = None,
    otlp_headers: str | None = None,
) -> None:
    """Initialize OpenTelemetry tracing and instrument the app once."""
    global _instrumented

    if _instrumented:
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import (
        SQLAlchemyInstrumentor,
    )
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
    )

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    if exporter == "console":
        span_exporter = ConsoleSpanExporter()
    else:
        span_exporter = OTLPSpanExporter(
            endpoint=otlp_endpoint,
            headers=_parse_otlp_headers(otlp_headers),
        )
    provider.add_span_processor(BatchSpanProcessor(span_exporter))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    _instrumented = True


def current_trace_context() -> dict[str, str]:
    """Return trace identifiers for the current span when available."""
    try:
        from opentelemetry.trace import get_current_span
    except ImportError:
        return {}

    span = get_current_span()
    context = span.get_span_context()
    if not context.is_valid:
        return {}
    return {
        "trace_id": format(context.trace_id, "032x"),
        "span_id": format(context.span_id, "016x"),
    }


@contextmanager
def start_span(
    name: str,
    attributes: dict[str, object] | None = None,
) -> Iterator[Any | None]:
    """Start a best-effort tracing span for background workflows."""
    try:
        from opentelemetry import trace
    except ImportError:
        yield None
        return

    tracer = trace.get_tracer("db_monitor")
    with tracer.start_as_current_span(name) as span:
        if attributes:
            _set_span_attributes(span, attributes)
        yield span
