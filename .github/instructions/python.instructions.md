---
description: 'Python coding conventions for DB Monitor'
applyTo: '**/*.py'
---

# Python Coding Conventions (DB Monitor)

Authoritative rules live in the repo-root `AGENTS.md`. Summary:

- Python 3.11+. Canonical package: `src/db_monitor/`. Never add logic to `app/`.
- Line length 79 (Ruff). Type hints on every function using built-in generics.
- `from __future__ import annotations` in every module.
- Imports from the `db_monitor` namespace only.
- Docstrings per PEP 257.
- Async SQLAlchemy via `AsyncSessionLocal`.
- Structured logging; no `print` in library code.
- Bug fixes require a regression test (`@pytest.mark.anyio` for async).
- Run via Make: `make monitor-lint`, `make monitor-typecheck`,
  `make monitor-test-core`.

Do not introduce alternative styles, formatters, or conventions.
