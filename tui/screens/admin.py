from __future__ import annotations

import asyncio
import json
from typing import Any

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, DataTable, Footer, Header, Input, Static

from client import api_client
from presentation import format_time_label, truncate_text


class AdminScreen(Screen):
    """Dedicated operator view for admin-only workflows."""

    BINDINGS = [
        Binding("escape", "close", "Back"),
        Binding("q", "close", "Back"),
        Binding("r", "refresh", "Refresh"),
        Binding("v", "create_viewer_key", "New Viewer Key"),
        Binding("g", "create_admin_key", "New Admin Key"),
        Binding("o", "rotate_selected_key", "Rotate Key"),
        Binding("x", "revoke_selected_key", "Revoke Key"),
        Binding("i", "toggle_inactive_keys", "Toggle Inactive Keys"),
        Binding("p", "replay_selected", "Replay Selected"),
        Binding("b", "replay_batch", "Replay Batch"),
        Binding("t", "toggle_replayed", "Toggle Replayed"),
    ]

    CSS = """
    #admin-left {
        width: 48%;
        padding: 1;
        border-right: solid green;
    }
    #admin-right {
        width: 52%;
        padding: 1;
    }
    .admin-section {
        margin-bottom: 1;
        padding: 1;
        border: round $surface;
    }
    .admin-table {
        height: 1fr;
        min-height: 12;
        margin-bottom: 1;
    }
    .admin-form-row {
        height: auto;
        margin-bottom: 1;
    }
    #admin-status {
        height: 13;
    }
    #api-key-table {
        height: 12;
    }
    #api-key-detail,
    #api-key-output {
        min-height: 7;
    }
    #checkpoint-table {
        height: 12;
    }
    #dlq-table {
        height: 14;
    }
    #dlq-detail {
        min-height: 12;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._info: dict[str, Any] = {}
        self._readiness: dict[str, Any] = {}
        self._api_keys: list[dict[str, Any]] = []
        self._current_api_key_id: int | None = None
        self._checkpoints: list[dict[str, Any]] = []
        self._dead_letters: list[dict[str, Any]] = []
        self._include_inactive_keys = True
        self._include_replayed = False
        self._dlq_limit = 50
        self._api_key_output_text = (
            "[bold]Key Output[/bold]\n"
            "Create or rotate a key to display the one-time secret here."
        )

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="admin-left"):
                yield Static(id="admin-status", classes="admin-section")
                yield Static(id="api-key-label", classes="admin-section")
                with Horizontal(classes="admin-form-row"):
                    yield Input(
                        placeholder="Owner name",
                        id="api-key-owner",
                    )
                    yield Input(value="90", id="api-key-ttl")
                with Horizontal(classes="admin-form-row"):
                    yield Button(
                        "Create Viewer",
                        id="create-viewer-key",
                        variant="success",
                    )
                    yield Button(
                        "Create Admin",
                        id="create-admin-key",
                        variant="warning",
                    )
                    yield Button("Rotate Selected", id="rotate-key")
                    yield Button(
                        "Revoke Selected",
                        id="revoke-key",
                        variant="error",
                    )
                yield DataTable(
                    id="api-key-table",
                    classes="admin-table",
                )
                yield Static(id="api-key-detail", classes="admin-section")
                yield Static(id="api-key-output", classes="admin-section")
            with Vertical(id="admin-right"):
                yield Static(
                    "[bold]Consumer Checkpoints[/bold]",
                    classes="admin-section",
                )
                yield DataTable(
                    id="checkpoint-table",
                    classes="admin-table",
                )
                yield Static(id="dlq-label", classes="admin-section")
                yield DataTable(id="dlq-table", classes="admin-table")
                yield Static(id="dlq-detail", classes="admin-section")
        yield Footer()

    async def on_mount(self) -> None:
        self._configure_tables()
        self.query_one("#api-key-output", Static).update(
            self._api_key_output_text
        )
        await self._refresh_data()

    def action_close(self) -> None:
        self.app.pop_screen()

    def action_refresh(self) -> None:
        asyncio.create_task(self._refresh_data())

    def action_toggle_replayed(self) -> None:
        self._include_replayed = not self._include_replayed
        asyncio.create_task(self._refresh_data())

    def action_toggle_inactive_keys(self) -> None:
        self._include_inactive_keys = not self._include_inactive_keys
        asyncio.create_task(self._refresh_data())

    def action_create_viewer_key(self) -> None:
        asyncio.create_task(self._create_key("viewer"))

    def action_create_admin_key(self) -> None:
        asyncio.create_task(self._create_key("admin"))

    def action_rotate_selected_key(self) -> None:
        selected = self._current_api_key()
        if selected is None:
            self.notify("Select an API key first.", severity="warning")
            return
        if not selected.get("can_rotate"):
            self.notify(
                "Only active keys can be rotated.",
                severity="warning",
            )
            return
        asyncio.create_task(self._rotate_key(int(selected["id"])))

    def action_revoke_selected_key(self) -> None:
        selected = self._current_api_key()
        if selected is None:
            self.notify("Select an API key first.", severity="warning")
            return
        if not selected.get("can_revoke"):
            self.notify(
                "The selected key cannot be revoked from this session.",
                severity="warning",
            )
            return
        asyncio.create_task(self._revoke_key(int(selected["id"])))

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "create-viewer-key":
            self.action_create_viewer_key()
            return
        if button_id == "create-admin-key":
            self.action_create_admin_key()
            return
        if button_id == "rotate-key":
            self.action_rotate_selected_key()
            return
        if button_id == "revoke-key":
            self.action_revoke_selected_key()

    def action_replay_selected(self) -> None:
        selected = self._current_dead_letter()
        if selected is None:
            self.notify("Select a DLQ record first.", severity="warning")
            return

        asyncio.create_task(self._replay_one(selected["id"]))

    def action_replay_batch(self) -> None:
        asyncio.create_task(self._replay_batch())

    def on_data_table_row_highlighted(
        self,
        event: DataTable.RowHighlighted,
    ) -> None:
        if event.data_table.id == "api-key-table":
            self._render_api_key_detail(event.cursor_row)
            return
        if event.data_table.id != "dlq-table":
            return
        self._render_dlq_detail(event.cursor_row)

    async def _refresh_data(self) -> None:
        try:
            (
                self._info,
                self._readiness,
                api_keys_payload,
                self._checkpoints,
                self._dead_letters,
            ) = await asyncio.gather(
                api_client.get_info(),
                api_client.get_readiness(),
                api_client.list_api_keys(
                    include_inactive=self._include_inactive_keys,
                ),
                api_client.get_consumer_checkpoints(),
                api_client.get_dead_letter_events(
                    limit=self._dlq_limit,
                    include_replayed=self._include_replayed,
                ),
            )
        except Exception as exc:
            self.notify(
                f"Failed to load admin data: {exc}",
                severity="error",
            )
            return

        self._api_keys = list(api_keys_payload.get("keys") or [])
        current_api_key_id = api_keys_payload.get("current_api_key_id")
        if isinstance(current_api_key_id, int):
            self._current_api_key_id = current_api_key_id

        self._render_status()
        self._render_api_keys()
        self._render_checkpoints()
        self._render_dead_letters()

    async def _create_key(self, role: str) -> None:
        form_values = self._read_key_form_values()
        if form_values is None:
            return

        owner_name, ttl_days = form_values
        try:
            response = await api_client.create_api_key(
                owner_name=owner_name,
                role=role,
                ttl_days=ttl_days,
            )
        except Exception as exc:
            self.notify(f"Key creation failed: {exc}", severity="error")
            return

        self._api_key_output_text = self._format_key_output(
            title=f"Created {role} key",
            response=response,
        )
        self.query_one("#api-key-output", Static).update(
            self._api_key_output_text
        )
        self.notify(
            f"Created {role} key for {owner_name}.",
            severity="information",
        )
        await self._refresh_data()

    async def _rotate_key(self, key_id: int) -> None:
        form_values = self._read_key_form_values(allow_blank_owner=True)
        if form_values is None:
            return

        _owner_name, ttl_days = form_values
        try:
            response = await api_client.rotate_api_key(
                key_id=key_id,
                ttl_days=ttl_days,
            )
        except Exception as exc:
            self.notify(f"Key rotation failed: {exc}", severity="error")
            return

        self._api_key_output_text = self._format_key_output(
            title=f"Rotated key #{key_id}",
            response=response,
        )
        self.query_one("#api-key-output", Static).update(
            self._api_key_output_text
        )
        self.notify(
            f"Rotated API key #{key_id}.",
            severity="information",
        )
        await self._refresh_data()

    async def _revoke_key(self, key_id: int) -> None:
        try:
            response = await api_client.revoke_api_key(key_id)
        except Exception as exc:
            self.notify(f"Key revoke failed: {exc}", severity="error")
            return

        self._api_key_output_text = (
            "[bold]Key Output[/bold]\n"
            f"Revoked key #{response.get('revoked_key_id')} for "
            f"{response.get('owner_name') or 'unknown'}.\n"
            f"Role: {response.get('role') or '-'}\n"
            f"Revoked at: {response.get('revoked_at') or '-'}"
        )
        self.query_one("#api-key-output", Static).update(
            self._api_key_output_text
        )
        self.notify(
            f"Revoked API key #{key_id}.",
            severity="information",
        )
        await self._refresh_data()

    async def _replay_one(self, dlq_event_id: int) -> None:
        try:
            result = await api_client.replay_dead_letter_event(dlq_event_id)
        except Exception as exc:
            self.notify(f"Replay failed: {exc}", severity="error")
            return

        self.notify(
            (
                f"Replay {result.get('status', 'completed')} for "
                f"DLQ #{dlq_event_id}."
            ),
            severity="information",
        )
        await self._refresh_data()

    async def _replay_batch(self) -> None:
        try:
            result = await api_client.replay_dead_letter_events(
                limit=self._dlq_limit,
                include_replayed=self._include_replayed,
            )
        except Exception as exc:
            self.notify(f"Batch replay failed: {exc}", severity="error")
            return

        self.notify(
            (
                f"Batch replay finished: {result.get('replayed_count', 0)} "
                f"replayed, {result.get('duplicate_count', 0)} duplicates, "
                f"{result.get('failed_count', 0)} failed."
            ),
            severity="information",
        )
        await self._refresh_data()

    def _configure_tables(self) -> None:
        api_key_table = self.query_one("#api-key-table", DataTable)
        api_key_table.cursor_type = "row"
        api_key_table.add_columns(
            "ID",
            "Owner",
            "Role",
            "Status",
            "Expires",
            "Current",
        )

        checkpoint_table = self.query_one("#checkpoint-table", DataTable)
        checkpoint_table.cursor_type = "row"
        checkpoint_table.add_columns(
            "Group",
            "Topic",
            "Partition",
            "Offset",
            "Updated",
        )

        dlq_table = self.query_one("#dlq-table", DataTable)
        dlq_table.cursor_type = "row"
        dlq_table.add_columns(
            "ID",
            "Failed",
            "Topic",
            "Offset",
            "Op",
            "Replay",
            "Error",
        )

    def _render_status(self) -> None:
        auth_context = api_client.get_auth_context()
        consumer = self._readiness.get("checks", {}).get("consumer", {})
        lifecycle = self._readiness.get("checks", {}).get("lifecycle", {})
        clusters = consumer.get("clusters") or {}

        lines = [
            "[bold]Admin Console[/bold]",
            (
                f"User: {auth_context.get('owner_name') or 'unknown'} "
                f"({auth_context.get('role') or 'unknown'})"
            ),
            f"App: {self._info.get('title') or 'DB Monitor'}",
            f"Readiness: {self._readiness.get('status') or 'unknown'}",
            f"Healthy: {self._info.get('healthy')}",
            f"Shutting down: {self._info.get('shutting_down')}",
            (
                f"Consumer: {consumer.get('status') or 'unknown'} | "
                f"lag={consumer.get('lag_total', 0)} | "
                f"dlq={consumer.get('dlq_messages_total', 0)} | "
                f"breaker={consumer.get('circuit_breaker_state') or 'n/a'}"
            ),
            (
                f"Lifecycle: {lifecycle.get('status') or 'unknown'} | "
                f"clusters={len(clusters)}"
            ),
            "",
            "[bold]Cluster Status[/bold]",
        ]

        if clusters:
            for cluster_name, cluster in sorted(clusters.items()):
                lines.append(
                    (
                        f"- {cluster_name}: "
                        f"connected={cluster.get('connected')} "
                        f"running={cluster.get('running')} "
                        f"lag={cluster.get('lag_total', 0)}"
                    )
                )
        else:
            lines.append("- No cluster data reported")

        self.query_one("#admin-status", Static).update("\n".join(lines))

    def _render_api_keys(self) -> None:
        label = self.query_one("#api-key-label", Static)
        label.update(
            (
                "[bold]API Keys[/bold]\n"
                f"Showing {len(self._api_keys)} keys | "
                f"include inactive={self._include_inactive_keys} | "
                "[bold]v[/bold] viewer | "
                "[bold]g[/bold] admin | "
                "[bold]o[/bold] rotate | "
                "[bold]x[/bold] revoke | "
                "[bold]i[/bold] toggle inactive"
            )
        )

        table = self.query_one("#api-key-table", DataTable)
        table.clear()

        for api_key in self._api_keys:
            expires_at = api_key.get("expires_at")
            table.add_row(
                str(api_key.get("id") or "-"),
                str(api_key.get("owner_name") or "-"),
                str(api_key.get("role") or "-"),
                str(api_key.get("status") or "unknown"),
                (
                    format_time_label(expires_at, compact=True)
                    if expires_at
                    else "never"
                ),
                "yes" if api_key.get("current_authenticated") else "no",
            )

        if self._api_keys:
            table.move_cursor(row=0, column=0, animate=False, scroll=False)
            self._render_api_key_detail(0)
            return

        self.query_one("#api-key-detail", Static).update(
            "[bold]Key Detail[/bold]\nNo API keys matched this filter."
        )

    def _current_api_key(self) -> dict[str, Any] | None:
        table = self.query_one("#api-key-table", DataTable)
        cursor_row = table.cursor_row
        if cursor_row is None:
            return None
        if cursor_row < 0 or cursor_row >= len(self._api_keys):
            return None
        return self._api_keys[cursor_row]

    def _render_api_key_detail(self, row_index: int) -> None:
        if row_index < 0 or row_index >= len(self._api_keys):
            return

        api_key = self._api_keys[row_index]
        lines = [
            "[bold]Key Detail[/bold]",
            f"ID: {api_key.get('id')}",
            f"Owner: {api_key.get('owner_name') or '-'}",
            f"Role: {api_key.get('role') or '-'}",
            f"Status: {api_key.get('status') or 'unknown'}",
            f"Current session key: {api_key.get('current_authenticated')}",
            (
                f"Created: "
                f"{format_time_label(api_key.get('created_at'))}"
                if api_key.get("created_at")
                else "Created: unknown"
            ),
            (
                f"Expires: "
                f"{format_time_label(api_key.get('expires_at'))}"
                if api_key.get("expires_at")
                else "Expires: never"
            ),
        ]

        if api_key.get("revoked_at"):
            lines.append(
                f"Revoked: {format_time_label(api_key.get('revoked_at'))}"
            )

        lines.extend(
            [
                "",
                (
                    f"Rotate allowed: {api_key.get('can_rotate')} | "
                    f"Revoke allowed: {api_key.get('can_revoke')}"
                ),
            ]
        )
        self.query_one("#api-key-detail", Static).update("\n".join(lines))

    def _read_key_form_values(
        self,
        allow_blank_owner: bool = False,
    ) -> tuple[str, int] | None:
        owner_name = self.query_one("#api-key-owner", Input).value.strip()
        ttl_value = self.query_one("#api-key-ttl", Input).value.strip()

        if not owner_name and not allow_blank_owner:
            self.notify("Owner name is required.", severity="warning")
            return None

        try:
            ttl_days = int(ttl_value)
        except ValueError:
            self.notify("TTL days must be an integer.", severity="warning")
            return None

        if ttl_days < 1:
            self.notify(
                "TTL days must be greater than zero.",
                severity="warning",
            )
            return None

        return owner_name, ttl_days

    def _format_key_output(
        self,
        title: str,
        response: dict[str, Any],
    ) -> str:
        return "\n".join(
            [
                f"[bold]{title}[/bold]",
                f"Owner: {response.get('owner_name') or '-'}",
                f"Role: {response.get('role') or '-'}",
                f"Expires: {response.get('expires_at') or '-'}",
                f"API Key: {response.get('api_key') or '-'}",
                str(response.get("message") or "Store this secret securely."),
            ]
        )

    def _render_checkpoints(self) -> None:
        table = self.query_one("#checkpoint-table", DataTable)
        table.clear()

        for checkpoint in self._checkpoints:
            table.add_row(
                str(checkpoint.get("consumer_group") or "-"),
                str(checkpoint.get("kafka_topic") or "-"),
                str(checkpoint.get("kafka_partition") or "-"),
                str(checkpoint.get("kafka_offset") or "-"),
                format_time_label(
                    checkpoint.get("updated_at"),
                    compact=True,
                ),
            )

    def _render_dead_letters(self) -> None:
        label = self.query_one("#dlq-label", Static)
        label.update(
            (
                "[bold]Dead Letter Queue[/bold]\n"
                f"Showing {len(self._dead_letters)} records | "
                f"include replayed={self._include_replayed} | "
                "[bold]p[/bold] replay selected | "
                "[bold]b[/bold] replay batch | "
                "[bold]t[/bold] toggle replayed"
            )
        )

        table = self.query_one("#dlq-table", DataTable)
        table.clear()

        for event in self._dead_letters:
            table.add_row(
                str(event.get("id") or "-"),
                format_time_label(event.get("failed_at"), compact=True),
                str(event.get("kafka_topic") or "-"),
                (
                    f"{event.get('kafka_partition', '-')}:"
                    f"{event.get('kafka_offset', '-')}"
                ),
                str(event.get("operation") or "-"),
                "yes" if event.get("is_replayed") else "no",
                truncate_text(str(event.get("error_message") or "-"), 36),
            )

        if self._dead_letters:
            table.move_cursor(row=0, column=0, animate=False, scroll=False)
            self._render_dlq_detail(0)
            return

        self.query_one("#dlq-detail", Static).update(
            "[bold]DLQ Detail[/bold]\nNo DLQ records available."
        )

    def _current_dead_letter(self) -> dict[str, Any] | None:
        table = self.query_one("#dlq-table", DataTable)
        cursor_row = table.cursor_row
        if cursor_row is None:
            return None
        if cursor_row < 0 or cursor_row >= len(self._dead_letters):
            return None
        return self._dead_letters[cursor_row]

    def _render_dlq_detail(self, row_index: int) -> None:
        if row_index < 0 or row_index >= len(self._dead_letters):
            return

        event = self._dead_letters[row_index]
        raw_payload = event.get("raw_payload")
        if isinstance(raw_payload, str):
            payload_preview = truncate_text(raw_payload, 900)
        else:
            payload_preview = truncate_text(
                json.dumps(raw_payload, sort_keys=True),
                900,
            )

        lines = [
            "[bold]DLQ Detail[/bold]",
            f"ID: {event.get('id')}",
            f"Service: {event.get('service_name') or 'unknown'}",
            f"Topic: {event.get('kafka_topic') or '-'}",
            (
                f"Partition/Offset: {event.get('kafka_partition', '-')} / "
                f"{event.get('kafka_offset', '-')}"
            ),
            f"Operation: {event.get('operation') or '-'}",
            f"Failed: {format_time_label(event.get('failed_at'))}",
            f"Replayed: {event.get('is_replayed')}",
        ]

        replayed_at = event.get("replayed_at")
        if replayed_at:
            lines.append(f"Replayed at: {format_time_label(replayed_at)}")
        if event.get("replay_error"):
            lines.append(f"Replay error: {event.get('replay_error')}")

        lines.extend(
            [
                "",
                "[bold]Error[/bold]",
                str(event.get("error_message") or "-"),
                "",
                "[bold]Payload Preview[/bold]",
                payload_preview,
            ]
        )
        self.query_one("#dlq-detail", Static).update("\n".join(lines))
