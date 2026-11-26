# DB Monitor — Usage Guide

Quick reference for the `make` targets most operators and contributors use.
Run `make monitor-help` for the full list.

## One command to start everything (Compose)

```bash
make monitor-start   # starts core + sandbox (Kafka/Debezium) and waits for health
make monitor-api-key # generate an admin API key
make monitor-tui     # launch the dashboard
```

## Pick one stack

DB Monitor supports two local stacks. Only one binds `localhost:8000`, so
check with `make monitor-status` before starting the TUI.

- **Compose (default for daily dev)** — fast, hot-reload, no image rebuild.
  - Start: `make monitor-up` (or `make monitor-up-sandbox` for Kafka/Debezium)
  - Dev server: `make monitor-dev`
  - TUI: `make monitor-tui`
  - API key: `make monitor-api-key` (`OWNER_NAME=`, `TTL_DAYS=`)
- **Kind (deploy validation)** — tests the real container image + Helm chart.
  - Port-forwards: `make monitor-port-forward` / `make monitor-port-forward-stop`
  - TUI: `make monitor-tui-kind` (preflight checks 8000/8001)
  - Seed demo data: `make monitor-seed-kind` (`SEED_BASE=N` for a second batch)
  - API key: `make monitor-api-key-kind` (`OWNER_NAME=`, `TTL_DAYS=`)

## Verification gate (before claiming a change is done)

```bash
make monitor-check   # lint + typecheck + core tests
```

Or individually: `make monitor-lint`, `make monitor-typecheck`,
`make monitor-test-core`.

## Demo mode (clean slate per run)

Guarded by `DEMO_MODE=1` so it can never wipe a real database by accident.

```bash
# Compose
make monitor-demo-reset          # truncate events/tables/columns/etc.
make monitor-start && make monitor-tui

# Kind
make monitor-demo-kind           # reset + seed 35 events
make monitor-tui-kind
```

## Diagnostics

- `make monitor-status` — which stack is responding on 8000/8001.
- `make monitor-logs` / `make monitor-logs-app` — Compose logs.
- Port-forward logs: `/tmp/db-monitor-port-forward/`.

## Notes

- Generating a key for an existing `OWNER_NAME` deactivates prior admin keys
  for that owner. Use a distinct owner (e.g. `OWNER_NAME=tui`) to keep the
  original key working.
- After editing Python for Kind, rebuild + reload the image and restart the
  consumer before declaring the fix live.
- Operational gotchas are recorded in `state.md`.
