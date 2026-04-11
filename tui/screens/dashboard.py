from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import Header, Footer, DataTable, Tree, Static, Label
from textual.screen import Screen
from textual.message import Message
import asyncio

from client import api_client


class DashboardScreen(Screen):
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
        height: 100%;
    }
    """

    class RefreshData(Message):
        def __init__(self, event=None):
            super().__init__()
            self.event = event

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.selected_service = None
        self._ws_task = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Label("Monitored Tables", classes="title")
                yield Tree("Databases", id="db-tree")
            with Vertical(id="main-content"):
                yield Label(
                    "Recent Events | View: All", id="view-label", classes="title"
                )
                yield DataTable(id="events-table")
        yield Footer()

    async def on_mount(self) -> None:
        table = self.query_one("#events-table", DataTable)
        table.add_columns("ID", "Time", "Service", "Operation", "Type", "Table ID")

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

    async def on_dashboard_screen_refresh_data(self, event: RefreshData) -> None:
        if event.event:
            self._prepend_event(event.event)
        else:
            await self._load_events()

    def _prepend_event(self, ev: dict) -> None:
        table = self.query_one("#events-table", DataTable)
        time_str = ev.get("event_time", "")
        if time_str and "T" in time_str:
            time_str = time_str.split("T")[1][:8]
        table.add_row(
            str(ev.get("id", "")),
            time_str,
            ev.get("service_name", ""),
            ev.get("operation", ""),
            ev.get("event_type", ""),
            str(ev.get("source_table_id", "")),
        )
        label = self.query_one("#view-label", Label)
        view_text = self.selected_service if self.selected_service else "All"
        label.update(f"Recent Events | View: {view_text} (live)")

    async def _load_sidebar(self) -> None:
        tree = self.query_one("#db-tree", Tree)
        tree.root.expand()
        try:
            tables = await api_client.get_tables()
            services = {}
            for t in tables:
                svc = t.get("service_name", "unknown")
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
                limit=50, service_name=self.selected_service
            )
            table = self.query_one("#events-table", DataTable)
            table.clear()
            for ev in events:
                time_str = ev.get("event_time", "")
                if time_str and "T" in time_str:
                    time_str = time_str.split("T")[1][:8]
                table.add_row(
                    str(ev.get("id", "")),
                    time_str,
                    ev.get("service_name", ""),
                    ev.get("operation", ""),
                    ev.get("event_type", ""),
                    str(ev.get("source_table_id", "")),
                )
            label = self.query_one("#view-label", Label)
            view_text = self.selected_service if self.selected_service else "All"
            label.update(f"Recent Events | View: {view_text}")
        except Exception as e:
            self.notify(f"Error loading events: {e}", severity="error")

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node_label = str(event.node.label)
        if node_label == "Databases":
            self.selected_service = None
        else:
            if event.node.parent == event.node.tree.root:
                self.selected_service = node_label
            else:
                self.selected_service = str(event.node.parent.label)
        await self._load_events()
