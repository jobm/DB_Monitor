---
name: DB Monitor Development
description: DB Monitor development workflow: Makefile-first commands, lint/typecheck/test gates, Kind deploy, seeding, and API-key generation.
---

# DB Monitor Development Workflow

Use this skill when working on the DB Monitor repository.

## Rule: Makefile first

- Prefer `make <target>` over ad-hoc commands. Run `make monitor-help` to
  discover targets.
- If a reusable command is missing, add a `monitor-*` target to the Makefile,
  then run it. Do not duplicate logic in shell snippets.
- Standard verification gate: `make monitor-lint`, `make monitor-typecheck`,
  `make monitor-test-core` (or `make monitor-check`).

## Before editing

- Read `AGENTS.md` at the repo root for hard rules (style, architecture).
- Canonical code lives in `src/db_monitor/`; `app/` is legacy shims.

## Style

- Ruff line-length 79, target py311. mypy on `src/db_monitor`.
- `from __future__ import annotations`; built-in generics; `db_monitor` imports.
- Type hints + PEP 257 docstrings. No `typing.List`/`Dict`/`Optional`.

## Testing

- Regression test for every bug fix. `@pytest.mark.anyio` for async.
- Run targeted tests first, then `make monitor-test-core`.

## Kind / TUI

- Port-forwards: `make monitor-port-forward` / `...-stop`.
- Seed demo data: `make monitor-seed-kind` (`SEED_BASE=N`).
- API key: `make monitor-api-key` (local) / `make monitor-api-key-kind`.
- After a Python change: rebuild image, `kind load docker-image`, rollout restart
  the consumer (and API if needed), then verify `/readyz` and `/tables`.

## Done criteria

- lint + typecheck + tests green; state.md updated for operational gotchas.
