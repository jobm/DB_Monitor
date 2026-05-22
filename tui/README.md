# DB Monitor TUI

The Textual client is an operator-oriented frontend for the DB Monitor API.

## Capabilities

- Browse monitored services and tables
- Inspect recent CDC events with linked record history and diffs
- Focus the dashboard on a single record from the detail view
- Open the admin console with an admin API key to inspect readiness,
  manage API keys, review checkpoints, and drive DLQ recovery

## Running Locally

```bash
cd tui
uv run python app.py
```

Use the API host URL and an existing DB Monitor API key on the login screen.

## Keyboard Shortcuts

- Dashboard: `Enter` opens event detail, `c` clears filters, `a` opens the
	admin console for admin credentials
- Event detail: `f` focuses the selected record, `m` loads more history,
	`n`/`p` pages timeline and history, `[`/`]` cycles field filters, `a`
	clears the field filter, `r` toggles raw payload
- Admin console: `r` refreshes data, `v` creates a viewer key, `g` creates an
	admin key, `o` rotates the selected key, `x` revokes the selected key,
	`i` toggles inactive keys, `p` replays the selected DLQ record, `b`
	replays the visible DLQ batch, `t` toggles whether replayed records are
	shown
