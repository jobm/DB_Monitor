from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import Screen
from textual.widgets import Button, Input, Label

from client import api_client


class LoginScreen(Screen):
    """Screen for connecting to the backend."""

    CSS = """
    LoginScreen {
        align: center middle;
    }
    #login-box {
        width: 60;
        height: auto;
        border: solid $success;
        padding: 2 4;
        background: $surface;
    }
    #login-title {
        text-align: center;
        text-style: bold;
        color: $success;
        margin-bottom: 2;
    }
    .field-label {
        margin-top: 1;
        color: $text-muted;
    }
    Input {
        margin-top: 0;
    }
    #status-msg {
        margin-top: 1;
        text-align: center;
        min-height: 1;
    }
    #connect-btn {
        width: 100%;
        margin-top: 2;
    }
    """

    def compose(self) -> ComposeResult:
        with Container(id="login-box"):
            yield Label("⬡  DB Monitor TUI", id="login-title")
            yield Label("FastAPI Host URL", classes="field-label")
            yield Input(value="http://localhost:8000", id="host-url")
            yield Label("API Key", classes="field-label")
            yield Input(
                placeholder="e.g. 1.XyZAbCd...",
                password=True,
                id="api-key",
            )
            yield Button("Connect", id="connect-btn", variant="success")
            yield Label("", id="status-msg")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id != "connect-btn":
            return

        host = self.query_one("#host-url", Input).value.strip()
        key = self.query_one("#api-key", Input).value.strip()
        status = self.query_one("#status-msg", Label)

        if not host or not key:
            status.update("[red]Both fields are required.[/red]")
            return

        status.update("[yellow]Connecting...[/yellow]")
        self.query_one("#connect-btn", Button).disabled = True

        # Force client to be recreated with new base_url
        await api_client.close()
        api_client.set_credentials(host, key)

        if not await api_client.check_health():
            status.update(
                "[red]Could not reach host. Is the server running?[/red]"
            )
            self.query_one("#connect-btn", Button).disabled = False
            return

        if not await api_client.verify_auth():
            status.update("[red]Invalid API Key.[/red]")
            self.query_one("#connect-btn", Button).disabled = False
            return

        status.update("[green]Connected![/green]")
        from screens.dashboard import DashboardScreen
        self.app.push_screen(DashboardScreen())
