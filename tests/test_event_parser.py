import pytest
from datetime import datetime, timezone
from app.event_parser import parse_event_payload

def test_parse_valid_json_with_explicit_event_time():
    raw_payload = '{"event_type": "insert", "event_time": "2023-10-10T10:10:10Z", "user_id": "user123", "service_name": "orders", "data": {"total": 100}}'
    parsed = parse_event_payload(raw_payload)
    
    assert parsed.event_type == "insert"
    assert parsed.user_id == "user123"
    assert parsed.service_name == "orders"
    assert parsed.event_time.year == 2023
    assert parsed.event_data is not None
    assert "data" in parsed.event_data

def test_parse_debezium_format():
    raw_payload = '{"op": "c", "ts_ms": 1696932610000, "source": {"name": "dbserver1"}, "after": {"id": 1}}'
    parsed = parse_event_payload(raw_payload)
    
    assert parsed.event_type == "c"
    assert parsed.service_name == "dbserver1"
    assert parsed.event_time.year == 2023

def test_parse_invalid_json():
    raw_payload = 'INVALID { JSON ]'
    parsed = parse_event_payload(raw_payload)
    
    assert parsed.event_type == "unparsed"
    assert parsed.event_data is None
    assert parsed.raw_payload == raw_payload

def test_parse_fallback_now_time():
    raw_payload = '{"event_type": "update", "user": "alice"}'
    # This should default the event_time to `datetime.now(timezone.utc)`
    before = datetime.now(timezone.utc)
    parsed = parse_event_payload(raw_payload)
    after = datetime.now(timezone.utc)
    
    assert before <= parsed.event_time <= after
    assert parsed.user_id == "alice"
    assert parsed.service_name == "None" or parsed.service_name is None
