"""Property-based tests for ``event_parser.py``.

Uses Hypothesis to generate random Debezium payload shapes and verify
parsing invariants.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from hypothesis import assume, given, settings, strategies as st

from db_monitor.ingestion.event_parser import ParsedEvent, parse_event_payload


# ── Strategy: random valid payload ─────────────────────────────────────

# The set of event-type values that the parser recognises
_KNOWN_EVENT_TYPES: list[str] = [
    "c", "r", "u", "d", "create", "read", "update", "delete",
    "INSERT", "UPDATE", "DELETE", "event", "data", "change",
]

_JSON_PRIMITIVE = st.one_of(
    st.none(),
    st.booleans(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.integers(),
    st.text(max_size=200),
)

_JSON_VALUE = st.recursive(
    _JSON_PRIMITIVE,
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(
            st.text(min_size=1, max_size=20),
            children,
            max_size=8,
        ),
    ),
    max_leaves=20,
)


def _is_valid_json(s: str) -> bool:
    try:
        json.loads(s)
        return True
    except json.JSONDecodeError:
        return False


# ── Invariant tests ────────────────────────────────────────────────────


@given(_JSON_VALUE)
def test_parse_never_raises(payload: object) -> None:
    """``parse_event_payload`` must never raise for any JSON-serialisable
    input — not even deeply nested, empty, or otherwise hostile payloads.
    """
    raw = json.dumps(payload, default=str)
    result = parse_event_payload(raw)
    assert isinstance(result, ParsedEvent)


@given(
    service_name=st.text(min_size=1, max_size=50),
    user_id=st.text(min_size=1, max_size=50),
    event_type=st.sampled_from(_KNOWN_EVENT_TYPES),
)
def test_parse_known_event_type_fields(
    service_name: str,
    user_id: str,
    event_type: str,
) -> None:
    """When the payload contains well-known field names those values must
    appear verbatim in the parsed result.

    Empty strings are collapsed to ``None`` by the parser's ``or``-chain
    fallback logic, so we use ``min_size=1`` to avoid empty values.
    """
    payload: dict[str, object] = {
        "event_type": event_type,
        "service_name": service_name,
        "user_id": user_id,
    }
    raw = json.dumps(payload)
    result = parse_event_payload(raw)

    assert result.event_type == event_type
    assert result.service_name == service_name
    assert result.user_id == user_id


@given(
    st.lists(_JSON_PRIMITIVE, min_size=1, max_size=10),
)
def test_parse_non_dict_json_list(values: list[object]) -> None:
    """Top-level JSON arrays are wrapped in a ``data`` key and parsed
    as a non-dict payload. The event_type falls back to ``\"unknown\"``
    because none of the recognised event-type keys are present.
    """
    raw = json.dumps(values)
    result = parse_event_payload(raw)
    # Arrays are wrapped in {'data': <array>} — no event_type key exists
    assert result.event_type == "unknown"
    assert result.event_data is not None
    assert result.event_data.get("data") == values


@given(st.text().filter(lambda s: not _is_valid_json(s)))
def test_parse_malformed_json(raw: str) -> None:
    """Invalid JSON must produce ``event_type=\"unparsed\"`` and
    ``event_data=None``.
    """
    result = parse_event_payload(raw)
    assert result.event_type == "unparsed"
    assert result.event_data is None
    assert result.raw_payload == raw


@given(
    st.dictionaries(
        st.text(min_size=1, max_size=20),
        _JSON_VALUE,
        min_size=0,
        max_size=10,
    )
)
def test_parse_roundtrip_raw_payload(data: dict[str, object]) -> None:
    """The ``raw_payload`` field must always equal the input string."""
    raw = json.dumps(data)
    result = parse_event_payload(raw)
    assert result.raw_payload == raw


@given(st.text(max_size=100))
def test_parse_event_time_never_none(raw: str) -> None:
    """``event_time`` must always be a timezone-aware datetime."""
    try:
        result = parse_event_payload(raw)
    except Exception:
        return
    assert isinstance(result.event_time, datetime)
    assert result.event_time.tzinfo is not None


@given(st.text(max_size=100))
def test_parse_event_time_reasonable(raw: str) -> None:
    """Parsed ``event_time`` must be within a reasonable range
    (1970–2100).
    """
    try:
        result = parse_event_payload(raw)
    except Exception:
        return
    assert result.event_time.year >= 1970, (
        f"event_time {result.event_time} is before 1970"
    )
    assert result.event_time.year <= 2100, (
        f"event_time {result.event_time} is after 2100"
    )


@given(
    st.one_of(
        # Values treated as seconds (< 10B threshold): year 1970-2100
        st.integers(min_value=0, max_value=4_102_444_800),
        # Values treated as milliseconds (>= 10B threshold): year 1971-2100
        st.integers(
            min_value=31_557_600_000,
            max_value=4_102_444_800_000,
        ),
    )
)
def test_parse_debezium_timestamp_ms(ts_ms: int) -> None:
    """A Debezium-style ``payload.ts_ms`` must be parsed into a
    reasonable ``event_time``.

    Values below 10 B are treated as seconds (epoch); values >= 10 B
    are treated as milliseconds.  The upper bounds correspond to
    2100-01-01 in both paths.
    """
    payload = {"payload": {"op": "c", "ts_ms": ts_ms}}
    raw = json.dumps(payload)
    result = parse_event_payload(raw)
    assert isinstance(result.event_time, datetime)
    assert result.event_time.year >= 1970, (
        f"event_time {result.event_time} year < 1970"
    )
    assert result.event_time.year <= 2100, (
        f"event_time {result.event_time} year > 2100"
    )
