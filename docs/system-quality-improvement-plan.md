# System Quality Improvement Plan

**Date:** 2026-06-21 (last revised: 2026-06-23)
**Author:** Staff Engineer Review
**Status:** Planning — Workstreams A–H not yet started

> **Current state:** The OS-release critical-path gaps (bulk write,
> partitioning, cursor pagination, Helm chart, idempotent offsets, migration
> locking, scale validation) have been closed since this plan was written.
> See `TODO.md` and `docs/os-release-readiness-assessment.md` for status.
>
> The structural improvements described below (Workstreams A–H) remain
> unstarted. This plan is the authoritative roadmap for the next phase of
> work after the OS-release gaps are proven stable.

---

## 1. Executive Summary

DB Monitor has a solid core: CDC ingestion, schema discovery, change tracking, WebSocket streaming, auth, metrics, a TUI dashboard, SDKs, and Helm/Terraform deployments. The architecture correctly separates broker abstraction, event pipelines, storage, and API surfaces.

However, the codebase exhibits structural problems typical of a project that grew organically from prototype toward production without a mid-course refactoring. The most critical issues are: **dual codebase** (`app/` vs `src/db_monitor/`), **configuration sprawl**, **monolithic modules**, **incomplete migration strategy**, and **inconsistent documentation**.

This plan prioritizes work that reduces friction for new contributors, improves CI reliability, and makes the project feel like a well-engineered open-source system rather than a personal prototype.

---

## 2. Critical Issues

### 2.1 Dual Codebase (`app/` vs `src/db_monitor/`)

**Severity: Critical**

Two near-identical module trees exist:

```
app/                          src/db_monitor/
├── config.py                 ├── config.py
├── main.py                   ├── main.py (delegates to legacy_runtime)
├── start_monitor.py          ├── start_monitor.py (delegates to legacy_runtime)
├── auth.py                   ├── auth.py
├── metrics.py                ├── metrics.py
├── app_factory.py            ├── app_factory.py (identical copy)
├── core/
│   ├── config.py (proxy)     ├── core/
│   │                          ├── config.py
│   │                          ├── config_loader.py
│   ├── db.py                 │   ├── db.py
│   ├── lifecycle.py          │   ├── lifecycle.py
│   ├── models.py             │   └── models.py
│   └── responses.py          │
├── ingestion/                ├── ingestion/
│   ├── pipeline.py           │   ├── pipeline.py
│   ├── consumer.py           │   ├── consumer.py
│   ├── schema.py             │   ├── schema.py
│   └── brokers.py            │   └── brokers.py
├── routes/                   ├── api/
│   ├── data.py               │   ├── factory.py
│   ├── auth.py               │   ├── router.py (delegates to routes)
│   └── ops.py                │
├── repositories/             ├── repositories/
└── ...                       └── ...
```

The `pyproject.toml` builds from `src/db_monitor/`, but most modules in that tree are just compatibility wrappers that import from `app/` via runtime sys.path manipulation. The `legacy_runtime.py` bridge works but creates confusion:

- Which `config.py` is canonical?
- Why do some modules exist in both trees with identical code?
- Why do tests sometimes import from `app.*` and sometimes from direct paths?

**Impact:** New contributors cannot understand the module hierarchy. Bug fixes must be applied to both trees. Packaging is fragile.

### 2.2 Configuration Sprawl

**Severity: High**

Configuration is scattered across at least five files:

| File | Role | Problem |
|------|------|---------|
| `app/config.py` | Main config (300+ lines) | Module-level `os.getenv()` calls execute on import, making testing painful. Mixes loading, validation, and defaults. |
| `app/core/config.py` | Compatibility proxy | Just does `from config import *` via `__getattr__`. Adds indirection with no value. |
| `src/db_monitor/config.py` | Duplicate of `app/config.py` | Second copy of the same logic. |
| `src/db_monitor/core/config.py` | Exports | Minimal, but adds another import path. |
| `src/db_monitor/core/config_loader.py` | Utility functions | The only file with proper helper functions. |

**Impact:** Importing the wrong `config` module causes subtle bugs. The module-level `os.getenv()` calls on import make it impossible to reconfigure during tests without environment variable manipulation.

### 2.3 Monolithic Modules

**Severity: High**

Several modules are too large and mix unrelated concerns:

| Module | Lines | Concerns |
|--------|-------|----------|
| `app/consumer_service.py` | ~1300 | Message consumption, parsing, DB writes, DLQ, checkpoints, WebSocket broadcast, webhooks — all in one module |
| `app/config.py` | ~700+ | Configuration loading, validation, cluster normalization, SLO policies — mixes data loading with business logic |
| `app/routes/data.py` | ~400+ | Event queries, table metadata, point-in-time lookups, change history — should be split per resource |
| `app/consumer_service.py` `consumer_task` | ~500 lines | Single function handling connection retry, message loop, quota enforcement, batch processing, error handling |

**Impact:** Hard to reason about, hard to test in isolation, high cognitive load for new contributors.

### 2.4 Incomplete Migration to Formal Schema Migrations

**Severity: Medium**

Despite having migration SQL files (`migrations/sql/0012_*.sql`, `0013_*.sql`) and a `migrations.py` module, the startup still uses `Base.metadata.create_all()` in the core lifecycle. The migration system exists but is not the primary path. This means:

- Schema changes must be manually applied in development
- There's no rollback story
- The migration SQL files may diverge from the SQLAlchemy model definitions

### 2.5 Test Fragility

**Severity: Medium**

Despite recent fixes, some patterns remain:

- `test_quota_limiter_throttles_then_allows` had a 20ms timing window (now fixed to 0.5s)
- Some tests depend on test ordering (the `-x` flag showed this)
- The `test_load_test.py` uses hardcoded string comparison that breaks on locale changes
- `test_smoke_test_endpoints.py` uses real TCP ports and threading — can flake on port conflicts

### 2.6 Documentation Gaps

**Severity: Medium**

| Gap | Impact |
|-----|--------|
| No module-level docstrings explaining the package hierarchy | New contributors can't understand the structure |
| No API reference generated from code | SDK/docs drift |
| No contributing guide or coding standards | Inconsistent code review |
| No architecture decision records (ADRs) | Design rationale is lost |
| No changelog | Users can't track what changed between versions |
| No security policy | External contributors can't responsibly report issues |

---

## 3. Improvement Plan

### Workstream A: Resolve the Dual Codebase (Priority: P0, Effort: Large)

#### A.1 Audit and Catalog

- List every module in `app/` and `src/db_monitor/`
- Classify each as: (a) identical copy, (b) wrapper delegating to `app/`, (c) unique
- Identify test imports that target the wrong tree

**Files to create:** `docs/module-inventory.md`

#### A.2 Consolidate Into a Single Tree

Chosen target: `src/db_monitor/` (the package that `pyproject.toml` builds from)

Steps:
1. For identical copies (`app_factory.py`), keep one copy in `src/db_monitor/`
2. For wrappers (`main.py`, `asgi.py`), inline the delegation
3. For `config.py`, create a single canonical version in `src/db_monitor/core/config.py` (see Workstream B)
4. For `app/ingestion/` → `src/db_monitor/ingestion/`, copy the real logic
5. Remove `legacy_runtime.py` and `ensure_legacy_path()` — no more sys.path manipulation
6. Remove the `app/` directory

**Acceptance criteria:**
- `pip install -e .` gives a fully functional `db_monitor` package
- All tests import from `db_monitor.*` and pass
- `python -m db_monitor` works
- No module uses `sys.path.insert` to find legacy code

#### A.3 Update All Imports

- `tests/` imports that reference `from app.*` → `from db_monitor.*`
- SDK imports
- Script imports

#### A.4 Remove Dead Code

- `app/core/config.py` (the proxy)
- `app/core/__init__.py` (lazy loader — no longer needed)
- `app/__init__.py` (empty)
- `app/config.py` (replaced by `src/db_monitor/core/config.py`)
- Any module that was only delegating

---

### Workstream B: Unify Configuration (Priority: P0, Effort: Medium)

#### B.1 Create a Single Config Module

Move all configuration to `src/db_monitor/core/config.py` with:

- **Lazy loading:** Use `@dataclass` or a `Settings` class loaded on first access, not `os.getenv()` at module level
- **Validation:** Centralize validation in `__post_init__` or a `validate()` method
- **Typing:** All config values typed with `dataclass` or `TypedDict` fields
- **Documentation:** Each field has a docstring describing purpose, default, and units

**Design:**

```python
@dataclass
class DatabaseSettings:
    url: str = field(default_factory=lambda: _get_env_or_file("POSTGRES_URL", DEFAULT_POSTGRES_URL))
    pool_size: int = 10
    max_overflow: int = 20
    ...

@dataclass
class BrokerSettings:
    kafka_broker: str = "kafka:9092"
    topics: list[str] = field(default_factory=lambda: ["orderdb.public.orders"])
    ...

@dataclass
class AppSettings:
    env: str = "development"
    db: DatabaseSettings = field(default_factory=DatabaseSettings)
    broker: BrokerSettings = field(default_factory=BrokerSettings)
    jwt_secret: str = "dev-insecure-..."
    ...

    @classmethod
    def from_env(cls) -> AppSettings: ...
```

#### B.2 Remove Redundant Config Files

- Delete `app/config.py` (after migration completes)
- Delete `app/core/config.py`
- Delete `src/db_monitor/config.py`
- Update `src/db_monitor/core/__init__.py` to export `config_loader` only

#### B.3 Update All Consumers

- Every module that does `from core.config import ...` or `from config import ...` → `from db_monitor.core.config import ...`
- Tests use `monkeypatch` on the config class, not on environment variables

---

### Workstream C: Break Up Monoliths (Priority: P1, Effort: Large)

#### C.1 Refactor `consumer_service.py`

Split into:
- `db_monitor/consumer/consumer.py` — main message loop and connection management
- `db_monitor/consumer/batch.py` — batch processing logic
- `db_monitor/consumer/dlq.py` — dead-letter queue operations
- `db_monitor/consumer/checkpoint.py` — offset checkpoint management
- `db_monitor/consumer/webhook.py` — webhook delivery (currently in `_broadcast_event`)

#### C.2 Refactor `app/config.py` (after consolidation)

Split into:
- `db_monitor/core/config/__init__.py` — exports the unified `AppSettings`
- `db_monitor/core/config/settings.py` — dataclass definitions
- `db_monitor/core/config/loaders.py` — file loading, CSV parsing, JSON loading
- `db_monitor/core/config/validators.py` — cross-field validation rules

#### C.3 Refactor API Routes

- Ensure `/events`, `/changes`, `/tables` each have their own route module
- Extract middleware and dependency injection into a separate module

#### C.4 Refactor `consumer_task` Function

The ~500-line `consumer_task` function should be split:
- `_connect_with_retry()` — connection loop
- `_process_message()` — single message handling
- `_quota_check()` — quota enforcement step
- `_handle_error()` — error path with DLQ fallback

---

### Workstream D: Formalize Database Migrations (Priority: P1, Effort: Medium)

#### D.1 Make Migrations the Primary Path

- Remove `Base.metadata.create_all()` from the startup lifecycle
- Ensure `migrations.py` is called during `db-monitor-migrate`
- Add a `db-monitor-migrate validate` command that checks schema drift

#### D.2 Migration Testing

- Add a test that runs all migrations on a fresh database
- Verify the final schema matches the SQLAlchemy models
- Add a test for rollback/down migrations (if supported)

#### D.3 Document Migration Workflow

- Add a `docs/migrations.md` explaining how to create, apply, and validate migrations
- Include the migration naming convention and review checklist

---

### Workstream E: Test Infrastructure Overhaul (Priority: P1, Effort: Medium)

#### E.1 Eliminate Timing-Dependent Tests

- Audit remaining tests for `asyncio.sleep()` with values < 0.5s
- Replace with explicit event coordination (`asyncio.Event`, `anyio.Event`)
- For the quota tests, the `CI_SAFE_*` constants are good — add a similar pattern for lifecycle tests

#### E.2 Add Integration Test Fixtures

- Create a `conftest.py` fixture that provides a real (or in-memory) Kafka broker for consumer tests
- Use `testcontainers` or a lightweight embedded broker to avoid dependency on a real Kafka cluster

#### E.3 Improve Test Isolation

- Ensure each test creates its own state (no shared session factories across tests)
- Add `autouse` fixtures that clean up global state between tests
- Add a fixture for temporary database with migrations applied

#### E.4 Add Property-Based Tests

- For `event_parser.py`: generate random Debezium payload shapes and verify parsing invariants
- For `ingestion_quota.py`: verify that quota decisions are monotonic (more events → less allowed)

---

### Workstream F: Documentation Overhaul (Priority: P1, Effort: Medium)

#### F.1 Module-Level Docstrings

Every public module should have a docstring explaining:
- What the module does
- Key classes/functions exported
- Example usage (brief)
- Related modules

**Scan existing modules** — many have no docstrings at all (e.g., `app/__init__.py`, `app/routes/utils.py`, `app/message_brokers.py`).

#### F.2 Architecture Decision Records

Create `docs/adr/` with records for key decisions:
- Why two codebases existed (and why they're being consolidated)
- Why RabbitMQ support was added alongside Kafka
- Why the quota throttle loop uses sleep instead of pushback from the broker
- Why WebSocket tokens have a short TTL

#### F.3 Contributing Guide (`CONTRIBUTING.md`)

Must include:
- How to set up a development environment
- Code style (ruff settings, type annotation requirements)
- Testing expectations (new code must have tests)
- Pull request process
- How to run the full CI suite locally

#### F.4 Changelog (`CHANGELOG.md`)

Adopt [Keep a Changelog](https://keepachangelog.com/) format. Start by logging all changes from this session.

#### F.5 API Documentation

- Generate from FastAPI's OpenAPI schema
- Host as a static page (GitHub Pages or similar)
- Include example requests/responses for each endpoint

#### F.6 Security Policy (`SECURITY.md`)

- How to report vulnerabilities
- Supported versions
- Disclosure timeline

#### F.7 README Rewrite

The current README is dense. Restructure:

```
# DB Monitor

> One-line description

## Quick Start (30 seconds)

## Features

## Architecture (high-level diagram)

## Documentation

## Installation

## Configuration

## Development

## License
```

---

### Workstream G: Developer Experience (Priority: P2, Effort: Medium)

#### G.1 Pre-commit Hooks

Add a `.pre-commit-config.yaml` with:
- `ruff check` (lint)
- `ruff format` (formatting)
- `mypy` (type checking)
- Trailing whitespace and EOF newline fixers

#### G.2 Makefile Modernization

- Add `monitor-lint-fix` (auto-fix lint issues)
- Add `monitor-test-quota` (run only quota tests for fast iteration)
- Add `monitor-test-lifecycle` (run only lifecycle tests)
- Rename targets to shorter aliases (`make test` → core tests, `make lint` → ruff, `make typecheck` → mypy)

#### G.3 VS Code Configuration

The existing `.vscode/settings.json` should include:
- `python.defaultInterpreterPath` pointing to the project's `.venv`
- `python.analysis.typeCheckingMode = "basic"` (Pylance)
- `ruff.enable = true`
- `ruff.format.args = ["--line-length=79"]`

#### G.4 Dockerfile Improvement

The current `Dockerfile` is at `app/Dockerfile` — confused with `Dockerfile.register` at root. Consolidate to a single `Dockerfile` at the repo root that:
- Installs the package via `pip install .`
- Uses `db-monitor-serve` as the entrypoint
- Supports `DB_SCHEMA_MODE` for migration strategy

---

### Workstream H: Packaging and Distribution (Priority: P2, Effort: Small)

#### H.1 Version Management

- Use `importlib.metadata.version("db-monitor")` consistently (already done in `__init__.py`)
- Add a `__version__` to the package that's readable without metadata
- Tag releases in git with `v*.*.*`

#### H.2 CI/CD Pipeline

- Publish to PyPI on tagged commits (Test PyPI first)
- Add a release workflow that builds wheel + sdist and uploads
- Add a `publish` Makefile target

#### H.3 SDK Maturity

The Python SDK (`sdk/python/db_monitor_sdk/`) is minimal:
- Add async support
- Add type annotations and mypy compliance
- Add docstrings for all public methods
- Package separately on PyPI as `db-monitor-client`
- The JavaScript SDK (`sdk/javascript/db_monitor_client.mjs`) should have a `package.json` and be publishable to npm

---

## 4. Implementation Order

| Phase | Workstreams | Rationale |
|-------|-------------|-----------|
| **Phase 1** (now) | A (consolidate app/ → src/) + B (unify config) | Removes the biggest confusing factor. Everything else gets easier after this. |
| **Phase 2** (1-2 weeks) | C (break up monoliths) + D (formal migrations) | Makes the codebase navigable and the database story production-ready. |
| **Phase 3** (2-3 weeks) | E (test infrastructure) + F (documentation) | Lays the foundation for confident contributions. |
| **Phase 4** (ongoing) | G (dev experience) + H (packaging) | Polish that makes the project feel professional. |

---

## 5. Effort Estimates

| Workstream | Estimated Person-Days | Dependencies |
|------------|----------------------|--------------|
| A. Consolidate dual codebase | 5-7 | None |
| B. Unify configuration | 3-5 | A |
| C. Break up monoliths | 5-8 | A, B |
| D. Formalize migrations | 2-3 | A |
| E. Test infrastructure | 3-5 | A |
| F. Documentation | 3-4 | A |
| G. Developer experience | 2-3 | A, F |
| H. Packaging | 2-3 | A |

**Total:** ~25-38 person-days for all workstreams.

---

## 6. Acceptance Criteria

The project can be considered to meet open-source quality standards when:

1. **Single import tree:** `from db_monitor import ...` works without sys.path manipulation
2. **Tests are fast and reliable:** `make test` runs in <30s with 0 flakes in 10 consecutive runs
3. **Documentation is complete:** README, CONTRIBUTING, CHANGELOG, SECURITY, ADRs all present
4. **CI is green:** All gates (lint, typecheck, tests) pass on every PR
5. **Migration workflow works:** `db-monitor-migrate apply` and `validate` both work
6. **Package is publishable:** `pip install db-monitor` gives a functional CLI
7. **SDKs are usable:** Both Python and JavaScript SDKs have examples and docs

---

## 7. Review Checklist for Future Changes

When reviewing PRs or planning new work, ask:

- [ ] Does this change increase or decrease the module count in `src/db_monitor/`?
- [ ] Is configuration accessed through the unified `AppSettings` or via raw `os.getenv()`?
- [ ] Are new modules under 300 lines? If not, should they be split?
- [ ] Are new functions under 50 lines? If not, should they be split?
- [ ] Do new tests avoid `asyncio.sleep()` for timing coordination?
- [ ] Do new tests follow the existing conftest/fixture patterns?
- [ ] Is documentation updated in the same PR?
- [ ] Does the change maintain backward compatibility for existing deployments?
