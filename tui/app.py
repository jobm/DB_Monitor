from textual.app import App
from screens.login import LoginScreen
from client import api_client


class DBMonitorApp(App):
    """DB Monitor Textual Application."""

    TITLE = "Database Change Monitor"

    def on_mount(self) -> None:
        """Called when app starts — always begin at login."""
        self.push_screen(LoginScreen())

    async def on_unmount(self) -> None:
        """Called when app closes — clean up HTTP client."""
        await api_client.close()


if __name__ == "__main__":
    app = DBMonitorApp()
    app.run()
