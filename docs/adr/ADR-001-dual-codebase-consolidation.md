# ADR-001: Dual Codebase Consolidation

**Status:** Executed  
**Date:** 2026-06-23  
**Deciders:** Project leads  
**Context:** [docs/module-inventory.md](../module-inventory.md), [Workstream A](../../docs/remaining-gaps.md#workstream-a-resolve-the-dual-codebase-priority-p0-effort-5-7-days-)

---

## Context

During early development, the project maintained two parallel code trees:

- **`app/`** — The primary runtime directory where all real logic lived.
- **`src/db_monitor/`** — A compatibility facade that delegated to `app/` at
  runtime via `legacy_runtime.py` and `sys.path.insert` tricks.

This dual layout existed because the packaging metadata (`pyproject.toml`)
pointed at `src/db_monitor/` for `pip install`, while the daily development
workflow ran from `app/`. The split made imports fragile, broke static analysis,
confused new contributors, and created a maintenance tax on every module change
(both trees had to be kept in sync).

## Decision

Consolidate all logic into `src/db_monitor/` and delete `app/`.

### Migration strategy

1. Move modules in dependency order, from lowest-risk leaf modules (metrics,
   tracing, WS manager) through core models, ingestion, consumer, and routes.
2. Keep thin compatibility wrappers in `app/` during the transition so that
   active compose workflows were not broken mid-migration.
3. After every module was moved, remove compatibility wrappers and delete the
   `app/` directory in a single cleanup pass.
4. Update all test imports from `from app.*` → `from db_monitor.*`.

### Rejected alternatives

- **Keep the dual layout:** Would have continued the import fragililty and
  confusion forever.
- **Keep only `app/` as the canonical path:** Would have required a packaging
  refactor anyway, and `src/` layout is Python best practice.
- **Gradual deprecation without cleanup:** Leaves dead wrappers that rot.

## Consequences

**Positive:**

- Single import namespace (`db_monitor.*`) for all runtime code.
- Static analysis (mypy, ruff, IDE autocomplete) works without
  `sys.path` hacks.
- New contributors have one obvious place to look for code.
- `pip install -e .` now reflects the actual runtime layout.

**Negative:**

- Large diff in a single cleanup pass (touches imports across ~60 files).
- Risk of missed `app.*` imports in scripts or CI paths.

**Mitigations:**

- All imports audited via `grep -r 'from app import'` before final deletion.
- CI gate validates no `from app import` remains.
- Full test suite (235+ tests) passes after consolidation.

## Compliance

- [x] All logic in `src/db_monitor/`
- [x] No `app/` directory
- [x] No `sys.path.insert` / `legacy_runtime.py`
- [x] Tests import from `db_monitor.*`
- [x] `pip install -e .` works
