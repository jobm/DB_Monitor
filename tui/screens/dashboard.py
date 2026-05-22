import asyncio

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label, Static, Tree

from client import api_client
from presentation import (
    event_change_pairs,
    format_time_label,
    format_operation_label,
    operation_badge,
    record_identity,
    summarize_event_changes,
    table_display_name,
    truncate_text,
)
from screens.admin import AdminScreen
from screens.event_detail import EventDetailScreen


class DashboardScreen(Screen):
    """Main dashboard for monitored tables and recent change activity."""

    BINDINGS = [
        ("enter", "open_event_detail", "Open Detail"),
        ("a", "open_admin", "Admin"),
        ("c", "clear_filters", "Clear Filters"),
    ]

    CSS = """
    #sidebar {
        width: 30%;
        border-right: solid green;
    }
    #main-content {
        width: 70%;
        padding: 1;
    }
    #events-table {
        margin-top: 1;
        height: 1fr;
    }
    #filter-bar {
        margin-top: 1;
        min-height: 2;
        padding: 0 1;
        border: round $surface;
    }
    #detail-pane {
        height: 14;
        margin-top: 1;
        padding: 1;
        border-top: solid green;
    }
    """

    class RefreshData(Message):
        def __init__(self, event=None):
            super().__init__()
            self.event = event

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.selected_service = None
        self.selected_table_id = None
        self.selected_row_identity = None
        self._focused_record_label = None
        self._table_index: dict[tuple[str, str], dict] = {}
        self._table_names_by_id: dict[int, str] = {}
        self._ws_task = None
        self._visible_events: list[dict] = []
        self._displayed_event_ids: set[str] = set()

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Label("Monitored Tables", classes="title")
                yield Tree("Databases", id="db-tree")
            with Vertical(id="main-content"):
                yield Label(
                    "Recent Events | View: All",
                    id="view-label",
                    classes="title",
                )
                yield Static(id="filter-bar")
                yield DataTable(id="events-table")
                yield Static(id="detail-pane")
        yield Footer()

    async def on_mount(self) -> None:
        table = self.query_one("#events-table", DataTable)
        table.cursor_type = "row"
        table.add_columns(
            "ID",
            "Time",
            "Table",
            "Record",
            "Operation",
            "Changes",
        )

        await self._load_sidebar()
        await self._load_events()

        try:
            await api_client.connect_ws()
        except Exception as e:
            self.notify(f"WebSocket connect failed: {e}", severity="warning")

        self._ws_task = asyncio.create_task(self._ws_loop())

    async def _ws_loop(self):
        while True:
            ev = await api_client.next_event(timeout=5.0)
            if ev is not None:
                self.post_message(self.RefreshData(ev))

    def on_unmount(self):
        if self._ws_task:
            self._ws_task.cancel()
        asyncio.create_task(api_client._disconnect_ws())

    async def on_dashboard_screen_refresh_data(
        self,
        event: RefreshData,
    ) -> None:
        if event.event:
            self._prepend_event(event.event)
        else:
            await self._load_events()

    def action_clear_filters(self) -> None:
        """Clear the active record focus or wider table/service filters."""
        if self.selected_row_identity is not None:
            self.selected_row_identity = None
            self._focused_record_label = None
            asyncio.create_task(self._load_events())
            return

        if (
            self.selected_table_id is not None
            or self.selected_service is not None
        ):
            self.selected_service = None
            self.selected_table_id = None
            self._focused_record_label = None
            asyncio.create_task(self._load_events())
            return

        self.notify("The dashboard is already showing all events.")

    def action_open_event_detail(self) -> None:
        """Open the current event in a full detail screen."""
        event = self._current_event()
        if event is None:
            return
        self.app.push_screen(
            EventDetailScreen(
                event,
                table_names_by_id=self._table_names_by_id,
                focus_record_callback=self.focus_record,
            )
        )

    def action_open_admin(self) -> None:
        """Open the admin console when authenticated as an admin."""
        if not api_client.is_admin():
            self.notify(
                "Admin access requires an admin API key.",
                severity="warning",
            )
            return

        self.app.push_screen(AdminScreen())

    def focus_record(self, event: dict) -> None:
        """Filter the dashboard feed down to the selected record."""
        service_name = event.get("service_name")
        table_id = event.get("source_table_id")
        row_identity = event.get("row_identity")

        if (
            not service_name
            or not isinstance(table_id, int)
            or not isinstance(row_identity, dict)
            or not row_identity
        ):
            self.notify(
                "This event does not include enough record metadata to "
                "focus the dashboard.",
                severity="warning",
            )
            return

        self.selected_service = service_name
        self.selected_table_id = table_id
        self.selected_row_identity = row_identity
        self._focused_record_label = record_identity(event)
        asyncio.create_task(self._load_events())

    def _current_event(self):
        table = self.query_one("#events-table", DataTable)
        cursor_row = table.cursor_row
        if cursor_row is None:
            return None
        if cursor_row < 0 or cursor_row >= len(self._visible_events):
            return None
        return self._visible_events[cursor_row]

    def _render_detail_pane(self, event: dict | None) -> None:
        detail = self.query_one("#detail-pane", Static)
        if event is None:
            detail.update("[bold]Selected Event[/bold]\nNo event selected.")
            return

        change_lines = event_change_pairs(event)
        preview_lines = [
            "[bold]Selected Event[/bold]",
            f"Table: {table_display_name(event, self._table_names_by_id)}",
            f"Record: {record_identity(event)}",
            f"Operation: {format_operation_label(event.get('operation'))}",
            f"Summary: {summarize_event_changes(event)}",
            "",
            "[bold]Changed fields[/bold]",
        ]
        if change_lines:
            for field_name, old_value, new_value in change_lines[:4]:
                preview_lines.append(
                    f"- {field_name}: {old_value} -> {new_value}"
                )
            hidden_count = len(change_lines) - 4
            if hidden_count > 0:
                preview_lines.append(f"- +{hidden_count} more field changes")
        else:
            preview_lines.append("- No changed fields detected")

        preview_lines.extend(
            [
                "",
                (
                    "Press [bold]Enter[/bold] for full diff and "
                    "linked record history."
                ),
            ]
        )
        detail.update("\n".join(preview_lines))

    def _update_filter_bar(self) -> None:
        """Render the currently active service, table, and record scope."""
        filter_bar = self.query_one("#filter-bar", Static)
        scope_parts: list[str] = []

        if self.selected_service:
            scope_parts.append(f"service={self.selected_service}")
        if self.selected_table_id is not None:
            table_name = self._table_names_by_id.get(
                self.selected_table_id,
                "selected table",
            )
            scope_parts.append(f"table={table_name}")
        if self.selected_row_identity is not None:
            record_label = self._focused_record_label or "selected record"
            scope_parts.append(f"record={record_label}")

        if not scope_parts:
            admin_hint = ""
            if api_client.is_admin():
                admin_hint = (
                    " Press [bold]a[/bold] to open the admin console."
                )
            filter_bar.update(
                "[bold]Scope[/bold] All events. Select a table or press "
                "[bold]f[/bold] in the detail view to focus a record."
                + admin_hint
            )
            return

        filter_bar.update(
            "[bold]Scope[/bold] "
            + " | ".join(scope_parts)
            + "\nPress [bold]c[/bold] to clear the active focus."
        )

    def _event_matches_current_view(self, ev: dict) -> bool:
        if self.selected_row_identity is not None:
            if ev.get("row_identity") != self.selected_row_identity:
                return False
        if self.selected_table_id is not None:
            return ev.get("source_table_id") == self.selected_table_id
        if self.selected_service is None:
            return True
        return ev.get("service_name") == self.selected_service

    def _current_view_text(self) -> str:
        if self.selected_row_identity is not None:
            table_name = self._table_names_by_id.get(
                self.selected_table_id,
                "Selected table",
            )
            record_label = self._focused_record_label or "Selected record"
            return f"{table_name} | {record_label}"
        if self.selected_table_id is not None:
            return self._table_names_by_id.get(
                self.selected_table_id,
                "Selected table",
            )
        if self.selected_service:
            return self.selected_service
        return "All"

    def _format_event_row(
        self,
        ev: dict,
    ) -> tuple[object, object, object, object, object, object]:
        return (
            str(ev.get("id", "")),
            format_time_label(ev.get("event_time"), compact=True),
            table_display_name(ev, self._table_names_by_id),
            truncate_text(record_identity(ev), max_length=36),
            operation_badge(ev.get("operation")),
            truncate_text(summarize_event_changes(ev), max_length=52),
        )

    def _render_visible_events(self) -> None:
        table = self.query_one("#events-table", DataTable)
        table.clear()
        self._displayed_event_ids.clear()

        for ev in self._visible_events:
            event_id = str(ev.get("id", ""))
            if not event_id or event_id in self._displayed_event_ids:
                continue
            table.add_row(*self._format_event_row(ev))
            self._displayed_event_ids.add(event_id)

        if self._visible_events:
            table.move_cursor(row=0, column=0, animate=False, scroll=False)
            self._render_detail_pane(self._visible_events[0])
        else:
            self._render_detail_pane(None)
        self._update_filter_bar()

    def _prepend_event(self, ev: dict) -> None:
        if not self._event_matches_current_view(ev):
            return

        event_id = str(ev.get("id", ""))
        if not event_id or event_id in self._displayed_event_ids:
            return

        self._visible_events.insert(0, ev)
        self._render_visible_events()
        label = self.query_one("#view-label", Label)
        view_text = self._current_view_text()
        label.update(f"Recent Events | View: {view_text} (live)")

    async def _load_sidebar(self) -> None:
        tree = self.query_one("#db-tree", Tree)
        tree.root.expand()
        try:
            tables = await api_client.get_tables()
            services = {}
            for t in tables:
                svc = t.get("service_name", "unknown")
                table_name = t.get("table_name", "unknown")
                table_id = t.get("id")
                self._table_index[(svc, table_name)] = t
                if isinstance(table_id, int):
                    self._table_names_by_id[table_id] = f"{svc}.{table_name}"
                if svc not in services:
                    services[svc] = []
                services[svc].append(t)
            for svc_name, svc_tables in services.items():
                svc_node = tree.root.add(svc_name, expand=True)
                for t in svc_tables:
                    svc_node.add_leaf(t.get("table_name", "unknown"))
        except Exception as e:
            self.notify(f"Error loading tables: {e}", severity="error")

    async def _load_events(self) -> None:
        try:
            events = await api_client.get_events(
                limit=50,
                service_name=self.selected_service,
                source_table_id=self.selected_table_id,
                row_identity=self.selected_row_identity,
            )
            self._visible_events = [
                ev for ev in events if self._event_matches_current_view(ev)
            ]
            self._render_visible_events()
            label = self.query_one("#view-label", Label)
            view_text = self._current_view_text()
            label.update(f"Recent Events | View: {view_text}")
            self._update_filter_bar()
        except Exception as e:
            self.notify(f"Error loading events: {e}", severity="error")

    def on_data_table_row_highlighted(
        self,
        event: DataTable.RowHighlighted,
    ) -> None:
        if 0 <= event.cursor_row < len(self._visible_events):
            self._render_detail_pane(self._visible_events[event.cursor_row])

    def on_data_table_row_selected(
        self,
        event: DataTable.RowSelected,
    ) -> None:
        if 0 <= event.cursor_row < len(self._visible_events):
            self.app.push_screen(
                EventDetailScreen(
                    self._visible_events[event.cursor_row],
                    table_names_by_id=self._table_names_by_id,
                    focus_record_callback=self.focus_record,
                )
            )

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node_label = str(event.node.label)
        if node_label == "Databases":
            self.selected_service = None
            self.selected_table_id = None
            self.selected_row_identity = None
            self._focused_record_label = None
        else:
            if event.node.parent == event.node.tree.root:
                self.selected_service = node_label
                self.selected_table_id = None
                self.selected_row_identity = None
                self._focused_record_label = None
            else:
                self.selected_service = str(event.node.parent.label)
                table = self._table_index.get(
                    (self.selected_service, node_label)
                )
                self.selected_table_id = table.get("id") if table else None
                self.selected_row_identity = None
                self._focused_record_label = None
        await self._load_events()
