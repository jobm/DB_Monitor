from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


# Stub Textual modules for dashboard unit tests without a UI runtime.
textual_app_stub = types.ModuleType("textual.app")
textual_app_stub.ComposeResult = object

textual_containers_stub = types.ModuleType("textual.containers")
textual_containers_stub.Horizontal = object
textual_containers_stub.Vertical = object

textual_message_stub = types.ModuleType("textual.message")


class _Message:
    def __init__(self, *args, **kwargs):
        del args, kwargs


textual_message_stub.Message = _Message

textual_screen_stub = types.ModuleType("textual.screen")


class _Screen:
    def __init__(self, *args, **kwargs):
        del args, kwargs


textual_screen_stub.Screen = _Screen

textual_widgets_stub = types.ModuleType("textual.widgets")


class _DataTable:
    class RowHighlighted:
        def __init__(self, cursor_row: int = 0):
            self.cursor_row = cursor_row

    class RowSelected:
        def __init__(self, cursor_row: int = 0):
            self.cursor_row = cursor_row


class _Footer:
    pass


class _Header:
    pass


class _Label:
    def update(self, _value: str) -> None:
        return None


class _Static:
    def update(self, _value: str) -> None:
        return None


class _Tree:
    class NodeSelected:
        pass


textual_widgets_stub.DataTable = _DataTable
textual_widgets_stub.Footer = _Footer
textual_widgets_stub.Header = _Header
textual_widgets_stub.Label = _Label
textual_widgets_stub.Static = _Static
textual_widgets_stub.Tree = _Tree

sys.modules.setdefault("textual.app", textual_app_stub)
sys.modules.setdefault("textual.containers", textual_containers_stub)
sys.modules.setdefault("textual.message", textual_message_stub)
sys.modules.setdefault("textual.screen", textual_screen_stub)
sys.modules.setdefault("textual.widgets", textual_widgets_stub)


client_stub = types.ModuleType("client")
client_stub.api_client = object()
sys.modules.setdefault("client", client_stub)


presentation_stub = types.ModuleType("presentation")
presentation_stub.event_change_pairs = lambda _event: []
presentation_stub.format_time_label = lambda _value, compact=False: "time"
presentation_stub.format_operation_label = lambda value: str(value or "UPDATE")
presentation_stub.operation_badge = lambda value: str(value or "UPDATE")
presentation_stub.record_identity = lambda _event: "record"
presentation_stub.summarize_event_changes = lambda _event: "summary"
presentation_stub.table_display_name = lambda _event, _table_map=None: "table"
presentation_stub.truncate_text = (
    lambda value, max_length=64: value[:max_length]
    if isinstance(value, str)
    else str(value)
)
sys.modules.setdefault("presentation", presentation_stub)


admin_stub = types.ModuleType("screens.admin")


class _AdminScreen:
    pass


admin_stub.AdminScreen = _AdminScreen
sys.modules.setdefault("screens.admin", admin_stub)


detail_stub = types.ModuleType("screens.event_detail")


class _EventDetailScreen:
    def __init__(self, *args, **kwargs):
        del args, kwargs


detail_stub.EventDetailScreen = _EventDetailScreen
sys.modules.setdefault("screens.event_detail", detail_stub)


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1] / "tui" / "screens" / "dashboard.py"
)
dashboard_spec = importlib.util.spec_from_file_location(
    "tui_dashboard",
    DASHBOARD_PATH,
)
dashboard_module = importlib.util.module_from_spec(dashboard_spec)
assert dashboard_spec is not None
assert dashboard_spec.loader is not None
dashboard_spec.loader.exec_module(dashboard_module)
DashboardScreen = dashboard_module.DashboardScreen


class _FakeLabel:
    def __init__(self) -> None:
        self.updates: list[str] = []

    def update(self, value: str) -> None:
        self.updates.append(value)


class _FakeTable:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []
        self.cursor_row = None

    def clear(self) -> None:
        self.rows.clear()

    def add_row(self, *values: object) -> None:
        self.rows.append(values)

    def move_cursor(
        self,
        row: int,
        column: int,
        animate: bool,
        scroll: bool,
    ) -> None:
        del column, animate, scroll
        self.cursor_row = row


def _build_screen_instance() -> DashboardScreen:
    screen = DashboardScreen.__new__(DashboardScreen)
    screen.selected_service = None
    screen.selected_table_id = None
    screen.selected_row_identity = None
    screen._focused_record_label = None
    screen._table_index = {}
    screen._table_names_by_id = {}
    screen._ws_task = None
    screen._visible_events = []
    screen._displayed_event_ids = set()
    return screen


def test_prepend_event_skips_duplicate_event_ids() -> None:
    """Repeated websocket events with the same id should not be duplicated."""
    screen = _build_screen_instance()
    label = _FakeLabel()

    def fake_query_one(_selector: str, _widget_type=None):
        return label

    screen.query_one = fake_query_one

    def fake_render_visible_events() -> None:
        screen._displayed_event_ids = {
            str(event.get("id", ""))
            for event in screen._visible_events
            if event.get("id") is not None
        }

    screen._render_visible_events = fake_render_visible_events

    event = {"id": 42, "service_name": "orderdb", "source_table_id": 1}

    screen._prepend_event(event)
    screen._prepend_event(event)

    assert len(screen._visible_events) == 1
    assert screen._visible_events[0]["id"] == 42
    assert label.updates == ["Recent Events | View: All (live)"]


def test_render_visible_events_deduplicates_duplicate_ids() -> None:
    """Table rendering should collapse duplicate ids into one row."""
    screen = _build_screen_instance()
    table = _FakeTable()

    def fake_query_one(selector: str, _widget_type=None):
        if selector == "#events-table":
            return table
        raise AssertionError(f"Unexpected selector: {selector}")

    screen.query_one = fake_query_one
    screen._render_detail_pane = lambda _event: None
    screen._update_filter_bar = lambda: None

    screen._visible_events = [
        {"id": 101, "operation": "INSERT"},
        {"id": 101, "operation": "INSERT"},
        {"id": 202, "operation": "UPDATE"},
    ]

    screen._render_visible_events()

    assert [row[0] for row in table.rows] == ["101", "202"]
    assert screen._displayed_event_ids == {"101", "202"}
