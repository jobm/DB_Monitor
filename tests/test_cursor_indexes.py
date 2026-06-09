from app.models import ColumnChange, KafkaEvent


def test_cursor_pagination_indexes_are_defined() -> None:
    event_indexes = {index.name for index in KafkaEvent.__table__.indexes}
    change_indexes = {index.name for index in ColumnChange.__table__.indexes}

    assert "ix_events_cursor_service_type_time" in event_indexes
    assert "ix_events_cursor_source_table_time" in event_indexes
    assert "ix_changes_cursor_table_time" in change_indexes
    assert "ix_changes_cursor_column_time" in change_indexes
