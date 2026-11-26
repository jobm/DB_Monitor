# DB Monitor — Agent Instructions

These rules are always in effect for this repository. They override any
conflicting guidance from other instruction files or model defaults.

## Tooling: always use Make

- Use `make` targets for every reusable workflow (lint, typecheck, tests,
  migrate, run, seed, port-forward, API-key generation). Run `make monitor-help`
  to list targets.
- If a reusable command does not exist as a target, **add it to the Makefile
  first**, then run it via `make`. Do not run ad-hoc shell commands that
  duplicate an existing target, and do not introduce a parallel command
  (e.g. a new script entrypoint) when a target already covers it.
- New reusable operations must be added as `monitor-*` targets (and short
  aliases where muscle memory warrants) so the next agent finds them.

## Verification gate (mandatory before claiming a change is done)

- `make monitor-lint` (Ruff) — must pass.
- `make monitor-typecheck` (mypy) — must pass.
- `make monitor-test-core` (or the targeted test file) — must pass.
- Bug fixes must include a regression test.

## Code style (do not deviate)

- Python 3.11+. Package lives in `src/db_monitor/` (canonical); `app/` holds
  legacy shims — never add new logic there.
- Ruff: `target-version = "py311"`, `line-length = 79`. Run via make.
- Type hints on every function; use built-in generics (`list[str]`,
  `dict[str, int]`, `X | None`), not `typing.List`/`Dict`/`Optional`.
- `from __future__ import annotations` at the top of every module.
- Imports from the `db_monitor` namespace only (`from db_monitor.x import y`),
  never bare module names.
- Docstrings follow PEP 257; keep comments on *why*, not *what*.
- Async SQLAlchemy: `async with AsyncSessionLocal() as session:`.
- Structured logging via the project logger; no `print` in library code.
- Do not reformat untouched code or switch formatting tools/styles.

## Architecture / data flow

- Single package `db_monitor`; entry points `db-monitor`, `db-monitor-serve`,
  `db-monitor-migrate`.
- Broker adapters implement `MessageConsumerAdapter`; factories
  `build_message_consumer` / `build_dead_letter_publisher`.
- Events are idempotent on `(kafka_topic, kafka_partition, kafka_offset,
  capture_time)`.

## Testing

- Pytest with `conftest.py` fixtures; `@pytest.mark.anyio` for async tests;
  `@pytest.mark.slow` for DB/broker-dependent tests.
- Each test owns its state; no order dependence.
- Hypothesis property tests encouraged for edge cases.

## Database / migrations

- Migrations are numbered SQL files in `migrations/sql/`; applied via
  `make monitor-migrate`. Never hand-edit applied migrations.
- Partitioned tables (`events`, `column_changes`) — retention drops partitions.

## Secrets

- Never commit secrets or `.env`. API keys are `{id}.{raw_secret}` (argon2 hash).

## Kind / TUI workflow

- Port-forwards: `make monitor-port-forward` / `...-stop` (resilient supervisor).
- Seed demo data: `make monitor-seed-kind` (`SEED_BASE=N` for a second batch).
- Generate an admin API key: `make monitor-api-key` (local) or
  `make monitor-api-key-kind` (Kind; `OWNER_NAME=`, `TTL_DAYS=`).
- TUI against Compose: `make monitor-tui`. Against Kind: `make monitor-tui-kind`
  (requires `make monitor-port-forward`). Check which stack is live with
  `make monitor-status`.

## Operational notes

- Record environment/Kind gotchas and non-obvious operational findings in
  `state.md` (Gotchas section) so the next agent does not rediscover them.
- When editing Python, rebuild/reload the Kind image before declaring a fix
  live (`podman build` → `kind load docker-image` → `kubectl rollout restart`).
