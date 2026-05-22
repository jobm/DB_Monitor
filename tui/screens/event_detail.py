from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Optional

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Static

from client import api_client
from presentation import (
    event_change_pairs,
    event_service_and_table,
    extract_event_payload,
    format_relative_time,
    format_operation_label,
    operation_badge,
    format_time_label,
    format_value,
    record_identity,
    summarize_event_changes,
    table_display_name,
    truncate_text,
)


class EventDetailScreen(Screen):
    """Full event detail and record-scoped history screen."""

    BINDINGS = [
        Binding("escape", "close", "Back"),
        Binding("q", "close", "Back"),
        Binding("f", "focus_record", "Focus Record"),
        Binding("m", "load_more", "Load More"),
        Binding("n", "next_page", "Next Page"),
        Binding("p", "previous_page", "Prev Page"),
        Binding("]", "next_field_filter", "Next Field"),
        Binding("[", "previous_field_filter", "Prev Field"),
        Binding("a", "clear_field_filter", "All Fields"),
        Binding("r", "toggle_raw_payload", "Toggle Raw"),
    ]

    CSS = """
    #detail-scroll {
        padding: 1 2;
    }
    .detail-section {
        margin-bottom: 1;
        padding: 1;
        border: round $surface;
    }
    .detail-table {
        height: 9;
        margin-bottom: 1;
    }
    #event-snapshot,
    #snapshot-diff,
    #timeline-detail,
    #history-detail,
    #event-raw {
        min-height: 6;
    }
    """

    def __init__(
        self,
        event: dict[str, Any],
        table_names_by_id: Optional[dict[int, str]] = None,
        focus_record_callback: Optional[
            Callable[[dict[str, Any]], None]
        ] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._event = event
        self._table_names_by_id = table_names_by_id or {}
        self._focus_record_callback = focus_record_callback
        self._page_size = 8
        self._timeline_limit = 8
        self._history_limit = 8
        self._timeline_offset = 0
        self._history_offset = 0
        self._timeline_events: list[dict[str, Any]] = []
        self._history_changes: list[dict[str, Any]] = []
        self._available_columns: list[str] = []
        self._selected_column_name: Optional[str] = None
        self._show_raw_payload = False
        self._syncing_tables = False

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll(id="detail-scroll"):
            yield Static(id="event-summary", classes="detail-section")
            yield Static(id="event-diff", classes="detail-section")
            yield Static(id="event-snapshot", classes="detail-section")
            yield Static(id="snapshot-diff", classes="detail-section")
            yield Static(id="event-timeline-label", classes="detail-section")
            yield DataTable(
                id="event-timeline-table",
                classes="detail-table",
            )
            yield Static(id="timeline-detail", classes="detail-section")
            yield Static(id="event-history-label", classes="detail-section")
            yield DataTable(
                id="event-history-table",
                classes="detail-table",
            )
            yield Static(id="history-detail", classes="detail-section")
            yield Static(id="event-raw", classes="detail-section")
        yield Footer()

    async def on_mount(self) -> None:
        self._configure_tables()
        self.query_one("#event-summary", Static).update(
            self._build_summary_text()
        )
        self.query_one("#event-diff", Static).update(self._build_diff_text())
        self._update_raw_payload()
        await self._load_available_columns()
        await self._refresh_detail_views()

    def action_close(self) -> None:
        """Close the detail screen."""
        self.app.pop_screen()

    def action_focus_record(self) -> None:
        """Apply this record as the dashboard filter and return."""
        if self._focus_record_callback is None:
            self.notify(
                "Record focus is unavailable from this screen.",
                severity="warning",
            )
            return

        if not isinstance(self._event.get("row_identity"), dict):
            self.notify(
                "This event does not include row identity metadata.",
                severity="warning",
            )
            return

        self._focus_record_callback(self._event)
        self.app.pop_screen()

    def action_load_more(self) -> None:
        """Load more record events and field history rows."""
        self._timeline_limit += 8
        self._history_limit += 8
        asyncio.create_task(self._refresh_detail_views())

    def action_next_page(self) -> None:
        """Advance the timeline and history views to the next page."""
        self._timeline_offset += self._page_size
        self._history_offset += self._page_size
        asyncio.create_task(self._refresh_detail_views())

    def action_previous_page(self) -> None:
        """Move the timeline and history views back one page."""
        self._timeline_offset = max(0, self._timeline_offset - self._page_size)
        self._history_offset = max(0, self._history_offset - self._page_size)
        asyncio.create_task(self._refresh_detail_views())

    def action_next_field_filter(self) -> None:
        """Cycle to the next field-specific history filter."""
        if not self._available_columns:
            self.notify("No field filters are available for this table.")
            return

        if self._selected_column_name is None:
            self._selected_column_name = self._available_columns[0]
        else:
            current_index = self._available_columns.index(
                self._selected_column_name
            )
            next_index = (current_index + 1) % len(self._available_columns)
            self._selected_column_name = self._available_columns[next_index]
        self._history_offset = 0
        asyncio.create_task(self._refresh_detail_views())

    def action_previous_field_filter(self) -> None:
        """Cycle to the previous field-specific history filter."""
        if not self._available_columns:
            self.notify("No field filters are available for this table.")
            return

        if self._selected_column_name is None:
            self._selected_column_name = self._available_columns[-1]
        else:
            current_index = self._available_columns.index(
                self._selected_column_name
            )
            next_index = (current_index - 1) % len(self._available_columns)
            self._selected_column_name = self._available_columns[next_index]
        self._history_offset = 0
        asyncio.create_task(self._refresh_detail_views())

    def action_clear_field_filter(self) -> None:
        """Show history for all tracked fields again."""
        if self._selected_column_name is None:
            self.notify("All fields are already visible.")
            return

        self._selected_column_name = None
        self._history_offset = 0
        asyncio.create_task(self._refresh_detail_views())

    def action_toggle_raw_payload(self) -> None:
        """Show or hide the raw payload section."""
        self._show_raw_payload = not self._show_raw_payload
        self._update_raw_payload()

    def on_data_table_row_highlighted(
        self,
        event: DataTable.RowHighlighted,
    ) -> None:
        """Keep timeline and history selections synchronized."""
        if self._syncing_tables:
            return

        if event.data_table.id == "event-timeline-table":
            self._handle_timeline_highlight(event.cursor_row)
            return

        if event.data_table.id == "event-history-table":
            self._handle_history_highlight(event.cursor_row)

    def _configure_tables(self) -> None:
        """Initialize stable column layouts for the detail tables."""
        timeline_table = self.query_one("#event-timeline-table", DataTable)
        timeline_table.cursor_type = "row"
        timeline_table.add_columns("When", "Operation", "Summary")

        history_table = self.query_one("#event-history-table", DataTable)
        history_table.cursor_type = "row"
        history_table.add_columns("When", "Column", "Before", "After")

    async def _load_available_columns(self) -> None:
        """Load table columns so field-specific filtering can cycle cleanly."""
        service_name, table_name = event_service_and_table(
            self._event,
            table_names_by_id=self._table_names_by_id,
        )
        if not service_name or not table_name:
            self._available_columns = []
            return

        try:
            table = await api_client.get_table(service_name, table_name)
        except Exception:
            self._available_columns = []
            return

        columns = table.get("columns", [])
        self._available_columns = sorted(
            str(column.get("column_name"))
            for column in columns
            if column.get("column_name")
        )

    async def _refresh_detail_views(self) -> None:
        """Reload the timeline, history, and selection-specific panes."""
        self.query_one("#event-timeline-label", Static).update(
            self._build_timeline_label()
        )
        self.query_one("#event-history-label", Static).update(
            self._build_history_label()
        )
        await self._populate_record_event_timeline_table()
        await self._populate_history_table()
        self._select_initial_rows()

    def _select_initial_rows(self) -> None:
        """Place cursors on the first available timeline and history rows."""
        if self._timeline_events:
            self._move_cursor("#event-timeline-table", 0)
            self._render_snapshot(self._timeline_events[0])
            self._render_snapshot_diff(self._timeline_events[0])
            self._render_timeline_detail(self._timeline_events[0])
        else:
            self.query_one("#event-snapshot", Static).update(
                self._build_snapshot_text(None)
            )
            self.query_one("#snapshot-diff", Static).update(
                self._build_snapshot_diff_text(None)
            )
            self.query_one("#timeline-detail", Static).update(
                "[bold]Timeline Selection[/bold]\n"
                "Select a record event to inspect its full snapshot."
            )

        if self._history_changes:
            self._move_cursor("#event-history-table", 0)
            self._render_history_detail(self._history_changes[0])
        else:
            self.query_one("#history-detail", Static).update(
                "[bold]Field Detail[/bold]\n"
                "Select a field-history row to inspect full values."
            )

    def _move_cursor(self, table_id: str, row_index: int) -> None:
        """Move a table cursor without triggering recursive sync loops."""
        table = self.query_one(table_id, DataTable)
        self._syncing_tables = True
        try:
            table.move_cursor(
                row=row_index,
                column=0,
                animate=False,
                scroll=False,
            )
        finally:
            self._syncing_tables = False

    def _handle_timeline_highlight(self, cursor_row: int) -> None:
        """Update selected-event detail and sync matching field history."""
        if cursor_row < 0 or cursor_row >= len(self._timeline_events):
            return

        selected_event = self._timeline_events[cursor_row]
        self._render_snapshot(selected_event)
        self._render_snapshot_diff(selected_event)
        self._render_timeline_detail(selected_event)
        self._update_raw_payload(selected_event)

        event_id = selected_event.get("id")
        if event_id is None:
            return

        history_index = self._find_history_index_for_event(event_id)
        if history_index is not None:
            self._move_cursor("#event-history-table", history_index)
            self._render_history_detail(self._history_changes[history_index])

    def _handle_history_highlight(self, cursor_row: int) -> None:
        """Update field detail and sync the matching timeline event."""
        if cursor_row < 0 or cursor_row >= len(self._history_changes):
            return

        selected_change = self._history_changes[cursor_row]
        self._render_history_detail(selected_change)

        event_id = selected_change.get("event_id")
        if event_id is None:
            return

        timeline_index = self._find_timeline_index_for_event(event_id)
        if timeline_index is not None:
            self._move_cursor("#event-timeline-table", timeline_index)
            selected_event = self._timeline_events[timeline_index]
            self._render_snapshot(selected_event)
            self._render_snapshot_diff(selected_event)
            self._render_timeline_detail(selected_event)
            self._update_raw_payload(selected_event)

    def _find_timeline_index_for_event(self, event_id: Any) -> Optional[int]:
        """Return the timeline row index for a given event id."""
        for index, event in enumerate(self._timeline_events):
            if event.get("id") == event_id:
                return index
        return None

    def _find_history_index_for_event(self, event_id: Any) -> Optional[int]:
        """Return the first history row index for a given event id."""
        for index, change in enumerate(self._history_changes):
            if change.get("event_id") == event_id:
                return index
        return None

    def _build_summary_text(self) -> str:
        """Render summary metadata and key bindings for this record."""
        operation_label = format_operation_label(self._event.get("operation"))
        table_name = table_display_name(
            self._event,
            self._table_names_by_id,
        )
        actions = [
            "Esc/q back",
            "f focus record",
            "m load more",
            "n/p page",
            "field prev/next",
            "a all fields",
            "r toggle raw",
        ]
        return "\n".join(
            [
                "[bold]Event Summary[/bold]",
                f"Table: {table_name}",
                f"Record: {record_identity(self._event)}",
                f"Operation: {operation_label}",
                f"Time: {format_time_label(self._event.get('event_time'))}",
                f"Summary: {summarize_event_changes(self._event)}",
                "",
                "Actions: " + " | ".join(actions),
            ]
        )

    def _build_diff_text(self) -> str:
        """Render the current event's changed fields."""
        changes = event_change_pairs(self._event)
        if not changes:
            return (
                "[bold]Changed Fields[/bold]\n"
                "No changed fields were detected for this event."
            )

        diff_lines = ["[bold]Changed Fields[/bold]"]
        for field_name, old_value, new_value in changes:
            diff_lines.append(f"- {field_name}")
            diff_lines.append(f"  before: {old_value}")
            diff_lines.append(f"  after:  {new_value}")
        return "\n".join(diff_lines)

    def _build_timeline_label(self) -> str:
        """Describe the record event timeline view and its current limit."""
        return (
            "[bold]Recent Record Events[/bold]\n"
            "Timeline for this exact record. "
            f"Page {self._timeline_offset // self._page_size + 1}, "
            f"showing up to {self._timeline_limit} events."
        )

    def _build_history_label(self) -> str:
        """Describe the field-history view and its current limit."""
        field_scope = self._selected_column_name or "all fields"
        return (
            "[bold]Recent Record History[/bold]\n"
            "Exact row-scoped field history for this record. "
            f"Page {self._history_offset // self._page_size + 1}, "
            f"showing up to {self._history_limit} changes, "
            f"filter={field_scope}."
        )

    async def _populate_record_event_timeline_table(self) -> None:
        """Render the recent event timeline for the current record."""
        service_name, _table_name = event_service_and_table(
            self._event,
            table_names_by_id=self._table_names_by_id,
        )
        table_id = self._event.get("source_table_id")
        row_identity = self._event.get("row_identity")
        timeline_table = self.query_one("#event-timeline-table", DataTable)
        timeline_table.clear()
        self._timeline_events = []

        if (
            not service_name
            or not isinstance(table_id, int)
            or not row_identity
        ):
            timeline_table.add_row(
                "Unavailable",
                "",
                "Record event timeline unavailable.",
            )
            return

        try:
            self._timeline_events = await api_client.get_events(
                limit=self._timeline_limit,
                offset=self._timeline_offset,
                service_name=service_name,
                source_table_id=table_id,
                row_identity=row_identity,
            )
        except Exception as exc:
            timeline_table.add_row(
                "Error",
                "",
                f"Could not load record events: {exc}",
            )
            return

        if not self._timeline_events:
            timeline_table.add_row(
                "Empty",
                "",
                "No record events available.",
            )
            return

        for event in self._timeline_events:
            timeline_table.add_row(
                format_time_label(event.get("event_time"), compact=True),
                operation_badge(event.get("operation")),
                truncate_text(
                    summarize_event_changes(event),
                    max_length=58,
                ),
            )

    async def _populate_history_table(self) -> None:
        """Render exact row-scoped field history for the current record."""
        service_name, table_name = event_service_and_table(
            self._event,
            table_names_by_id=self._table_names_by_id,
        )
        history_table = self.query_one("#event-history-table", DataTable)
        history_table.clear()
        self._history_changes = []

        if not service_name or not table_name:
            history_table.add_row(
                "Unavailable",
                "",
                "Table metadata unavailable.",
                "",
            )
            return

        try:
            self._history_changes = await api_client.get_changes(
                table_name=table_name,
                service_name=service_name,
                column_name=self._selected_column_name,
                row_identity=self._event.get("row_identity"),
                limit=self._history_limit,
                offset=self._history_offset,
            )
        except Exception as exc:
            history_table.add_row(
                "Error",
                "",
                f"Could not load record history: {exc}",
                "",
            )
            return

        if not self._history_changes:
            history_table.add_row(
                "Empty",
                "",
                "No record history available.",
                "",
            )
            return

        for change in self._history_changes:
            history_table.add_row(
                format_time_label(change.get("changed_at"), compact=True),
                str(change.get("column_name", "unknown")),
                truncate_text(
                    format_value(
                        change.get("old_value"),
                        field_name=change.get("column_name"),
                    ),
                    max_length=28,
                ),
                truncate_text(
                    format_value(
                        change.get("new_value"),
                        field_name=change.get("column_name"),
                    ),
                    max_length=28,
                ),
            )

    def _render_snapshot(self, event: Optional[dict[str, Any]]) -> None:
        """Show a compact record snapshot for the selected timeline event."""
        self.query_one("#event-snapshot", Static).update(
            self._build_snapshot_text(event)
        )

    def _render_snapshot_diff(self, event: Optional[dict[str, Any]]) -> None:
        """Render a field-by-field state diff for the selected event."""
        self.query_one("#snapshot-diff", Static).update(
            self._build_snapshot_diff_text(event)
        )

    def _build_snapshot_text(
        self,
        event: Optional[dict[str, Any]],
    ) -> str:
        """Render the selected event's post-change record snapshot."""
        if event is None:
            return (
                "[bold]Record Snapshot[/bold]\n"
                "Select a timeline row to inspect the record state."
            )

        payload = extract_event_payload(event)
        after_value = payload.get("after")
        before_value = payload.get("before")
        if isinstance(after_value, dict):
            snapshot = after_value
        else:
            snapshot = before_value

        lines = [
            "[bold]Record Snapshot[/bold]",
            f"When: {format_time_label(event.get('event_time'))}",
            f"Operation: {format_operation_label(event.get('operation'))}",
            f"Summary: {summarize_event_changes(event)}",
        ]

        if not isinstance(snapshot, dict) or not snapshot:
            lines.append("No full row snapshot is available for this event.")
            return "\n".join(lines)

        lines.append("")
        lines.append("[bold]Current row state[/bold]")
        visible_items = list(snapshot.items())[:8]
        for key, value in visible_items:
            rendered = truncate_text(
                format_value(value, field_name=key),
                max_length=88,
            )
            lines.append(f"- {key}: {rendered}")

        hidden_count = len(snapshot) - len(visible_items)
        if hidden_count > 0:
            lines.append(f"- +{hidden_count} more fields")

        return "\n".join(lines)

    def _build_snapshot_diff_text(
        self,
        event: Optional[dict[str, Any]],
    ) -> str:
        """Render a side-by-side before/after diff for the selected event."""
        if event is None:
            return (
                "[bold]State Transition[/bold]\n"
                "Select a timeline row to compare the before and after state."
            )

        payload = extract_event_payload(event)
        before_value = payload.get("before")
        after_value = payload.get("after")
        before = before_value if isinstance(before_value, dict) else {}
        after = after_value if isinstance(after_value, dict) else {}
        keys = list(dict.fromkeys([*before.keys(), *after.keys()]))

        lines = [
            "[bold]State Transition[/bold]",
            f"When: {format_time_label(event.get('event_time'))}",
            f"Operation: {format_operation_label(event.get('operation'))}",
        ]

        if not keys:
            lines.append("No comparable before/after row state is available.")
            return "\n".join(lines)

        lines.append("")
        lines.append("[bold]Field | Before | After[/bold]")
        for key in keys[:8]:
            before_rendered = truncate_text(
                format_value(before.get(key), field_name=key),
                max_length=28,
            )
            after_rendered = truncate_text(
                format_value(after.get(key), field_name=key),
                max_length=28,
            )
            lines.append(
                f"- {key}: {before_rendered} | {after_rendered}"
            )

        hidden_count = len(keys) - 8
        if hidden_count > 0:
            lines.append(f"- +{hidden_count} more fields")

        return "\n".join(lines)

    def _render_timeline_detail(self, event: dict[str, Any]) -> None:
        """Show selection details for the active timeline row."""
        detail_lines = [
            "[bold]Timeline Selection[/bold]",
            f"Event ID: {event.get('id', 'unknown')}",
            f"When: {format_time_label(event.get('event_time'))}",
            f"Relative: {format_relative_time(event.get('event_time'))}",
            f"Operation: {format_operation_label(event.get('operation'))}",
            f"Summary: {summarize_event_changes(event)}",
            "Select a field-history row to jump between linked changes.",
        ]
        self.query_one("#timeline-detail", Static).update(
            "\n".join(detail_lines)
        )

    def _render_history_detail(self, change: dict[str, Any]) -> None:
        """Show full before/after values for the active history row."""
        column_name = str(change.get("column_name", "unknown"))
        old_value = format_value(
            change.get("old_value"),
            field_name=column_name,
        )
        new_value = format_value(
            change.get("new_value"),
            field_name=column_name,
        )
        detail_lines = [
            "[bold]Field Detail[/bold]",
            f"Event ID: {change.get('event_id', 'unknown')}",
            f"When: {format_time_label(change.get('changed_at'))}",
            f"Relative: {format_relative_time(change.get('changed_at'))}",
            f"Column: {column_name}",
            f"Before: {old_value}",
            f"After:  {new_value}",
        ]
        self.query_one("#history-detail", Static).update(
            "\n".join(detail_lines)
        )

    def _update_raw_payload(
        self,
        event: Optional[dict[str, Any]] = None,
    ) -> None:
        """Render the raw payload for the selected event when requested."""
        raw_widget = self.query_one("#event-raw", Static)
        if not self._show_raw_payload:
            raw_widget.update(
                "[bold]Raw Payload[/bold]\n"
                "Hidden by default. Press [bold]r[/bold] to toggle it."
            )
            return

        target_event = event or self._event
        payload = extract_event_payload(target_event)
        serialized = json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=str,
        )
        raw_widget.update("[bold]Raw Payload[/bold]\n" + serialized)
