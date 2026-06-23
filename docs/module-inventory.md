# Module Inventory: Dual Codebase Consolidation

**Status:** Audit complete — consolidation **not yet started**
**Target:** Move all real logic from `app/` into `src/db_monitor/`, eliminate `legacy_runtime.py`, remove `app/` directory

> **Current state (2026-06-23):** The dual codebase still exists.
> `legacy_runtime.py` remains active in `src/db_monitor/` — all proxy, wrapper,
> and delegation modules still use `ensure_legacy_path()` to import from `app/`
> at runtime. This inventory remains accurate as a migration roadmap. The
> consolidation is tracked as Workstream A in
> `docs/system-quality-improvement-plan.md`.

---

## 1. Classification Legend

| Label | Meaning |
|-------|---------|
| **REAL** | Contains the actual implementation. The module to keep. |
| **PROXY** | `ensure_legacy_path()` + `importlib.import_module()` — delegates to `app/`. Remove after migration. |
| **WRAPPER** | Selectively re-exports specific names from `app/`. Remove after migration. |
| **COPY** | Identical implementation in both trees. Keep one copy in `src/db_monitor/`. |
| **UNIQUE** | Only exists in one tree. |

---

## 2. Top-Level Modules

### `__init__.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| Empty file (`app/__init__.py`) | Version + metadata (`src/db_monitor/__init__.py`) | `app/` → DELETE. `src/db_monitor/` → KEEP. |

### `config.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| **REAL** — 700+ lines. Module-level `os.getenv()` calls, cluster loading, validation, SLO policies. Imports `from db_monitor.core.config_loader import ...`. | **PROXY** — `ensure_legacy_path()` + `importlib.import_module("config")` | `app/` → MOVE to `src/db_monitor/config.py`. Rewrite as `@dataclass` (see config workstream). Remove proxy. |

### `main.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| **REAL** — Defines the FastAPI app: lifespan, middleware, WebSocket handler, metrics endpoint, tracing init, route inclusion. Imports from `db_monitor.*` (the proxy layer). | **WRAPPER** — `from db_monitor.asgi import app` | `app/` → MOVE to `src/db_monitor/main.py`. Remove wrapper. |

### `start_monitor.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| **REAL** — Topic configuration from manifest, Uvicorn server startup, signal handling, startup/shutdown hooks. | **WRAPPER** — `run_legacy_server()` | `app/` → MOVE to `src/db_monitor/start_monitor.py`. Remove wrapper. |

### `app_factory.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| `create_application()` — trivial factory | Same function, identical code | **COPY.** Keep one copy in `src/db_monitor/app_factory.py`. DELETE `app/app_factory.py`. |

### `cli.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| Does not exist | **UNIQUE** — Argparse CLI, `db-monitor serve`, `db-monitor migrate`. Delegates to `run_legacy_server()` and `run_legacy_migrate()`. | KEEP. After migration, remove delegation. |

### `__main__.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| Does not exist | **UNIQUE** — `python -m db_monitor` entry point | KEEP. |

### `asgi.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| Does not exist | **UNIQUE** — Loads legacy `app` from `main` module for ASGI servers | REMOVE after migration (not needed once `main.py` is in the package). |

### `legacy_runtime.py`

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| Does not exist | **UNIQUE** — `ensure_legacy_path()`, `run_legacy_server()`, `run_legacy_migrate()`. The bridge module. | REMOVE after all modules are migrated. |

---

## 3. Proxy Modules (same pattern, batch migration)

These `src/db_monitor/` modules all follow the same pattern:
```python
from db_monitor.legacy_runtime import ensure_legacy_path
ensure_legacy_path()
_legacy_module = importlib.import_module("module_name")
def __getattr__(name): return getattr(_legacy_module, name)
```

They each delegate to a real module in `app/`. The fix is the same for all: move the real module from `app/` to `src/db_monitor/` and delete the proxy.

| src/db_monitor/ module | Delegates to app/ | Lines in app/ | Action |
|------------------------|-------------------|---------------|--------|
| `auth.py` | `auth.py` | ~200 | MOVE → DELETE proxy |
| `extensions.py` | `extensions.py` | ~50 | MOVE → DELETE proxy |
| `metrics.py` | `metrics.py` | ~100 | MOVE → DELETE proxy |
| `audit_log.py` | `audit_log.py` | ~200 | MOVE → DELETE proxy |
| `ws_manager.py` | `ws_manager.py` | ~80 | MOVE → DELETE proxy |
| `tracing.py` | `tracing.py` | ~100 | MOVE → DELETE proxy |
| `retention.py` | `retention.py` | ~200 | MOVE → DELETE proxy |
| `lifespan.py` | `lifespan.py` | ~150 | MOVE → DELETE proxy |
| `routes.py` | `routes/` (package) | ~20 | MOVE → DELETE proxy |
| `ingestion/runtime_state.py` | `consumer/runtime_state.py` | ~100 | MOVE → DELETE proxy |
| `ingestion/quota.py` | `ingestion_quota.py` | ~260 | MOVE → DELETE proxy |
| `repositories/events.py` | `repositories/events.py` | ~100 | MOVE → DELETE proxy |

---

## 4. Wrapper Modules (named exports)

These `src/db_monitor/` modules import specific names rather than using `__getattr__`.

| Module | Exports | Delegates to app/ | Action |
|--------|---------|-------------------|--------|
| `ingestion/brokers.py` | `BrokerMessage`, `DeadLetterPublisher`, `build_message_consumer`, `build_dead_letter_publisher` | `message_brokers.py` | MOVE `message_brokers.py` → DELETE wrapper |
| `ingestion/consumer.py` | `consumer_task`, `get_consumer_health`, `list_*`, `replay_*` | `consumer_service.py` | MOVE `consumer_service.py` → DELETE wrapper |
| `ingestion/pipeline.py` | `parse_event_payload`, `EventPipeline`, `event_pipeline` | `event_parser.py` + `event_pipeline.py` | MOVE both → DELETE wrapper |
| `ingestion/schema.py` | `SchemaDiscovery`, `extract_operation`, `schema_cache_backplane` | `schema_discovery.py` | MOVE → DELETE wrapper |
| `ingestion/changes.py` | `ChangeProcessor` | `change_processor.py` | MOVE → DELETE wrapper |
| `repositories/audit_log_replay.py` | `replay_spill_entries`, `AsyncSessionLocal` | `repositories/audit_log_replay.py` | MOVE → DELETE wrapper |
| `repositories/audit_log_spill.py` | `list_audit_log_spill_snapshot` | `repositories/audit_log_spill.py` | MOVE → DELETE wrapper |

---

## 5. Core Subpackage

| app/ | src/db_monitor/ | Verdict |
|------|----------------|---------|
| `core/__init__.py` — Lazy submodule loader | `core/__init__.py` — Simple namespace exports | KEEP `src/db_monitor/core/__init__.py`, DELETE `app/core/__init__.py` |
| `core/config.py` — Proxy to `app/config.py` | `core/config.py` — **PROXY** to `app/config.py` | Both are proxies! The real config is `app/config.py`. After moving config, these become obsolete. |
| `core/config_loader.py` — Does not exist | **UNIQUE** — Utility functions (get_env_or_file, parse_csv_list, config_error) | KEEP, this is the canonical config utility module |
| `core/db.py` — Does not exist | `core/db.py` — **PROXY** to `app/extensions.py` | REMOVE after `extensions.py` is moved |
| `core/lifecycle.py` — Does not exist | `core/lifecycle.py` — **PROXY** to `app/lifecycle_manager.py` | REMOVE after `lifecycle_manager.py` is moved |
| `core/models.py` — SQLAlchemy models | Does not exist in `src/db_monitor/core/` | MOVE |
| `core/responses.py` — Response helpers | Does not exist | MOVE |

---

## 6. App-Only Modules (no src/ equivalent — must be MOVE'd)

These modules exist only in `app/` with no counterpart in `src/db_monitor/`. They must be moved directly.

| Module | Lines | Description | Target in src/db_monitor/ |
|--------|-------|-------------|--------------------------|
| `consumer_service.py` | ~1300 | Main consumer loop, batch processing, DLQ | `consumer/consumer.py` (split per monolith workstream) |
| `consumer/recovery.py` | ~300 | Checkpoint persistence, DLQ persistence, replay | `consumer/recovery.py` |
| `consumer/runtime_state.py` | ~200 | Circuit breaker, consumer runtime dicts | `consumer/runtime_state.py` |
| `consumer/__init__.py` | ~20 | Package marker | `consumer/__init__.py` |
| `event_parser.py` | ~150 | Parse Debezium payloads | `ingestion/event_parser.py` |
| `event_pipeline.py` | ~100 | Filter/transform pipeline | `ingestion/event_pipeline.py` |
| `lifecycle_manager.py` | ~200 | Graceful shutdown, task supervision | `core/lifecycle_manager.py` |
| `schema_discovery.py` | ~300 | Table/column discovery from CDC | `ingestion/schema_discovery.py` |
| `change_processor.py` | ~200 | Column-level delta extraction | `ingestion/change_processor.py` |
| `source_metadata.py` | ~50 | Source name extraction | `ingestion/source_metadata.py` |
| `row_identity.py` | ~80 | Row identity extraction | `ingestion/row_identity.py` |
| `message_brokers.py` | ~300 | Kafka/RabbitMQ adapters | `ingestion/brokers.py` (merge with existing wrapper) |
| `webhooks.py` | ~200 | Webhook delivery with retries | `webhooks.py` |
| `ingestion_quota.py` | ~260 | Quota enforcement | `ingestion/quota.py` (merge with existing proxy) |
| `migrate.py` | ~100 | CLI migration runner | `cli/migrate.py` |
| `migrations.py` | ~150 | Migration logic | `core/migrations.py` |
| `models.py` | ~100 | KafkaEvent, DeadLetterEvent models | `models.py` |
| `response_models.py` | ~50 | Pydantic response models | `api/response_models.py` |
| `routes/__init__.py` | ~15 | Router composition | `routes/__init__.py` |
| `routes/data.py` | ~400 | Data API routes | `routes/data.py` |
| `routes/auth.py` | ~200 | Auth API routes | `routes/auth.py` |
| `routes/ops.py` | ~300 | Ops/admin API routes | `routes/ops.py` |
| `routes/utils.py` | ~100 | Route helpers | `routes/utils.py` |
| `api/__init__.py` | ~30 | API package facade | Remove (replaced by `api/factory.py` in src) |
| `api/factory.py` | ~30 | API factory | **COPY** — identical to `src/db_monitor/api/factory.py` |
| `api/router.py` | ~10 | API router | **COPY** — identical to `src/db_monitor/api/router.py` (which wraps `routes`) |
| `ingestion/__init__.py` | ~50 | Ingestion package exports | `ingestion/__init__.py` |
| `ingestion/pipeline.py` | ~30 | Ingestion pipeline | **COPY** — identical to `src/db_monitor/ingestion/pipeline.py`? Check |
| `ingestion/consumer.py` | ~30 | Ingestion consumer | **COPY** — identical to `src/db_monitor/ingestion/consumer.py`? Check |
| `ingestion/schema.py` | ~30 | Ingestion schema | **COPY** — identical to `src/db_monitor/ingestion/schema.py`? Check |
| `ingestion/brokers.py` | ~30 | Ingestion brokers | **COPY** — identical to `src/db_monitor/ingestion/brokers.py`? Check |
| `repositories/__init__.py` | ~15 | Repo exports | `repositories/__init__.py` |
| `repositories/events.py` | ~100 | Events repository | `repositories/events.py` |
| `repositories/audit_log_replay.py` | ~150 | Audit log replay repo | `repositories/audit_log_replay.py` |
| `repositories/audit_log_spill.py` | ~80 | Audit log spill repo | `repositories/audit_log_spill.py` |
| `retention.py` | ~200 | Retention cleanup | `retention.py` |
| `audit_log.py` | ~200 | Audit log writer | `audit_log.py` |
| `metrics.py` | ~200 | Prometheus metrics | `metrics.py` |
| `tracing.py` | ~100 | OpenTelemetry tracing | `tracing.py` |
| `ws_manager.py` | ~80 | WebSocket connection manager | `ws_manager.py` |
| `lifespan.py` | ~150 | App lifespan management | `lifespan.py` |
| `auth.py` | ~200 | API key auth, JWT | `auth.py` |
| `extensions.py` | ~50 | SQLAlchemy engine, init_db | `extensions.py` |

---

## 7. Unique to `src/db_monitor/` (must keep after migration)

| Module | Description | Post-migration action |
|--------|-------------|----------------------|
| `public_api.py` | Public API exports | KEEP |
| `provisioning.py` | Provisioning logic (tenant setup) | KEEP |
| `provisioning_local.py` | Local provisioning | KEEP |
| `core/config_loader.py` | Config utility functions | KEEP — canonical |

---

## 8. Import Reference Map

### Tests importing from `db_monitor.*` (working correctly)

These already target the canonical package:
- `test_package_startup_surfaces.py` — `import db_monitor.start_monitor`
- `test_package_cli.py` — `import db_monitor.cli`
- `test_package_phase2_surfaces.py` — `import db_monitor.auth`, `db_monitor.ingestion.*`, `db_monitor.repositories.*`, `db_monitor.routes`
- `test_package_runtime_surfaces.py` — `import db_monitor.config`, `db_monitor.extensions`
- `test_package_public_api_contract.py` — `import db_monitor.public_api`
- `test_package_namespace.py` — `import db_monitor.core.config`, `db_monitor.core.db`
- `test_provisioning.py` / `test_provisioning_local.py` — `import db_monitor.provisioning`
- `test_auth_service.py` — likely imports from `db_monitor.auth`
- `test_routes.py` — likely imports from `db_monitor.routes`

### Tests importing from `app.*` (need rewriting)

These reference the legacy module paths directly:
- `test_event_parser.py` — `from app.event_parser import parse_event_payload`

After migration, these should all import from `db_monitor.*`.

### `app/` modules importing from `db_monitor.*` (circular dependency)

The fact that `app/main.py` imports from `db_monitor.*` (the proxy layer) creates a circular dependency:
- `app/main.py` → `db_monitor.core.config` → `legacy_runtime.py` → `app/` (sys.path)

This works at runtime because `legacy_runtime.py` adds `app/` to `sys.path`. After migration, this circular dependency is eliminated because all real code lives under `db_monitor.*`.

---

## 9. Migration Strategy

### Step 1: Move a single module as proof of concept

Pick a small, independent module that has no internal `app/` imports:
- **Candidate:** `app/app_factory.py` (already identical to `src/db_monitor/app_factory.py`)
- **Action:** Delete `app/app_factory.py`, keep `src/db_monitor/app_factory.py`
- **Risk:** None — identical code
- **Verification:** `pytest -m core` still passes

### Step 2: Move all COPY modules

Modules that are identical in both trees:
- `app/app_factory.py` → already done
- `app/api/factory.py` → is it identical to `src/db_monitor/api/factory.py`? Check and merge
- `app/api/router.py` → same

### Step 3: Move individual proxy targets

For each proxy module in `src/db_monitor/`, move the real `app/` module and delete the proxy. Order by dependency:

**Batch 1 — No internal `app/` imports:**
- `metrics.py` (no app imports)
- `ws_manager.py` (no app imports)
- `tracing.py` (no app imports)
- `retention.py` (no app imports)
- `extensions.py` (no app imports)

**Batch 2 — Imports from `core.*`:**
- `core/models.py`, `core/responses.py`, `core/__init__.py`
- `lifecycle_manager.py`
- `auth.py`
- `audit_log.py`

**Batch 3 — Imports from multiple sources:**
- `config.py` (moved as part of config workstream)
- `main.py` (depends on many modules)
- `start_monitor.py` (depends on config)

**Batch 4 — Ingestion modules:**
- `event_parser.py`, `event_pipeline.py`, `schema_discovery.py`
- `message_brokers.py`, `change_processor.py`, `source_metadata.py`, `row_identity.py`
- `consumer_service.py` (the big one — may want to split first)
- `consumer/recovery.py`, `consumer/runtime_state.py`
- `ingestion_quota.py`

**Batch 5 — Routes and API:**
- `routes/*` (all route modules)
- `repositories/*` (all repository modules)

### Step 4: Update all test imports

- Change `from app.* import ...` → `from db_monitor.* import ...`
- Run the full test suite to verify

### Step 5: Remove proxy modules and legacy_runtime.py

After every module is moved:
- Delete all proxy/wrapper `__getattr__` modules in `src/db_monitor/`
- Delete `src/db_monitor/legacy_runtime.py`
- Delete `src/db_monitor/asgi.py`
- Delete `src/db_monitor/core/db.py` (no longer needed)
- Delete `src/db_monitor/core/lifecycle.py` (no longer needed)

### Step 6: Remove `app/` directory

- Final cleanup: delete `app/` entirely
- Update Dockerfile and any references
- Run full CI

---

## 10. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Missed import path in `app/` module that references another `app/` module | Medium | Breakage when moving that module | Move leaf modules first (no internal deps), test after each move |
| Circular import after moving config | Low | Startup failure | Keep `core/config_loader.py` separate, use lazy loading for `AppSettings` |
| Test that imports from `app.*` breaks | Medium | Test failure | Fix imports as part of module move, not separately |
| `main.py` has too many dependencies to move atomically | High | Can't move in one PR | Move dependencies first, then `main.py` last |
| Docker/container references to `app/` | Low | Build failure | Update `Dockerfile` and `container-entrypoint.sh` |

---

## 11. Migration Order (Recommended)

```
Phase 1a: Low-risk moves (1-2 PRs)
  app/app_factory.py         → DELETE (already identical)
  app/extensions.py          → MOVE to src/db_monitor/extensions.py
  app/ws_manager.py           → MOVE to src/db_monitor/ws_manager.py
  app/tracing.py              → MOVE to src/db_monitor/tracing.py
  app/retention.py            → MOVE to src/db_monitor/retention.py
  app/metrics.py              → MOVE to src/db_monitor/metrics.py
  → Verify: tests pass, no circular imports

Phase 1b: Core modules (1-2 PRs)
  app/core/models.py          → MOVE to src/db_monitor/core/models.py
  app/core/responses.py       → MOVE to src/db_monitor/core/responses.py
  app/core/__init__.py        → MERGE with src/db_monitor/core/__init__.py
  app/lifecycle_manager.py    → MOVE to src/db_monitor/core/lifecycle_manager.py
  app/audit_log.py            → MOVE to src/db_monitor/audit_log.py
  app/auth.py                 → MOVE to src/db_monitor/auth.py
  → Verify: tests pass

Phase 1c: Ingestion modules (2-3 PRs)
  app/event_parser.py         → MOVE to src/db_monitor/ingestion/event_parser.py
  app/event_pipeline.py       → MOVE to src/db_monitor/ingestion/event_pipeline.py
  app/schema_discovery.py     → MOVE to src/db_monitor/ingestion/schema_discovery.py
  app/message_brokers.py      → MOVE to src/db_monitor/ingestion/brokers.py (merge)
  app/change_processor.py     → MOVE to src/db_monitor/ingestion/change_processor.py
  app/source_metadata.py      → MOVE to src/db_monitor/ingestion/source_metadata.py
  app/row_identity.py         → MOVE to src/db_monitor/ingestion/row_identity.py
  app/ingestion_quota.py      → MOVE to src/db_monitor/ingestion/quota.py (merge)
  app/webhooks.py             → MOVE to src/db_monitor/webhooks.py
  → Verify: tests pass

Phase 1d: Consumer (1-2 PRs — largest module)
  app/consumer/recovery.py    → MOVE to src/db_monitor/consumer/recovery.py
  app/consumer/runtime_state.py → MOVE to src/db_monitor/consumer/runtime_state.py
  app/consumer_service.py     → MOVE to src/db_monitor/consumer/consumer.py
  → Optionally split consumer_service.py first (see Workstream C)
  → Verify: tests pass

Phase 1e: Routes, repositories, and remaining modules (2-3 PRs)
  app/routes/*                → MOVE to src/db_monitor/routes/
  app/repositories/*          → MOVE to src/db_monitor/repositories/
  app/lifespan.py             → MOVE to src/db_monitor/lifespan.py
  → Verify: tests pass

Phase 1f: Main entry points and config (1-2 PRs — highest risk)
  app/config.py               → MOVE to src/db_monitor/config.py (rewrite as dataclass)
  app/main.py                 → MOVE to src/db_monitor/main.py
  app/start_monitor.py        → MOVE to src/db_monitor/start_monitor.py
  app/migrate.py              → MOVE to src/db_monitor/cli/migrate.py
  app/migrations.py           → MOVE to src/db_monitor/core/migrations.py
  → Verify: full CI pipeline, container build

Phase 1g: Cleanup (1 PR)
  Delete all proxy/wrapper modules from src/db_monitor/
  Delete src/db_monitor/legacy_runtime.py
  Delete src/db_monitor/asgi.py
  Delete src/db_monitor/core/db.py
  Delete src/db_monitor/core/lifecycle.py
  Delete app/ directory
  Update Dockerfile, container-entrypoint.sh
  Update tests that import from app.*
  → Verify: full CI pipeline, container build, pip install works
```

## 12. Verification Checklist

After each phase, run:
```bash
make monitor-lint        # ruff
make monitor-typecheck   # mypy
make monitor-test-core   # pytest -m core
```

After Phase 1g (complete removal):
```bash
pip install -e .          # verify package installs
python -m db_monitor      # verify entry point works
db-monitor --help         # verify CLI works
make monitor-test-core    # verify all tests pass
```
