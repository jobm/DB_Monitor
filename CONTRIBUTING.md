# Contributing to DB Monitor

Thank you for considering contributing to DB Monitor! This guide covers the
development workflow, code standards, and expectations for contributions.

## Table of Contents

- [Development Environment](#development-environment)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Documentation](#documentation)
- [Release Process](#release-process)

## Development Environment

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (package manager)
- Docker + Docker Compose (for integration tests and sandbox)
- PostgreSQL (for local development without Docker)

### Setup

```bash
# Clone the repository
git clone https://github.com/jobm/DB_Monitor.git
cd DB_Monitor

# Install dependencies (including dev/test/dev extras)
uv sync --group dev

# Set up secrets for local development
mkdir -p secrets
cp secrets/monitor_postgres_url.example secrets/monitor_postgres_url
cp secrets/monitor_jwt_secret.example secrets/monitor_jwt_secret
cp secrets/monitor_jwt_secret_next.example secrets/monitor_jwt_secret_next

# Set up pre-commit hooks (optional but recommended)
# uv run pre-commit install
```

### Running the stack

```bash
# Full sandbox (Kafka + source DBs + monitor DB + app)
make monitor-up-sandbox

# Core only (Kafka + monitor DB + app)
make monitor-up

# Run the TUI dashboard
make monitor-tui
```

## Code Style

DB Monitor uses [Ruff](https://docs.astral.sh/ruff/) for linting and
formatting, and [mypy](https://mypy-lang.org/) for static type checking.

### Linting

```bash
make monitor-lint
```

The Ruff configuration enforces:

- Python 3.11 target
- 79-character line length
- Error-catching rules (E9, F63, F7, F82)

### Type Checking

```bash
make monitor-typecheck
```

All new code should include type annotations for public functions and methods.
Use `from __future__ import annotations` for postponed evaluation of annotations
(Python 3.11+).

### Context Memory Checks

The assistant's always-on context comes from `.goosehints` and `.goosemem`.
Both are checked by a single target:

```bash
make monitor-check-context
```

It runs two scripts and fails on the first problem:

- **Structure, grounding and retrieval**
  (`scripts/verify_goosemem_retrieval.py`) — every `.goosemem` block parses,
  has valid keyword tags, names only repository paths that exist, and routes
  the natural-language and keyworded test questions to the expected domain.
- **Module coverage** (`scripts/validate_goosemem_coverage.sh`) — every
  non-`__init__` module under `src/db_monitor/` is named by a single block, so
  a new file cannot land without a home in the memory.

Run it whenever you edit `.goosemem`, and when you add or move a module under
`src/db_monitor/` — the coverage check will fail until the block that owns the
package names the new file. The checks also run in CI and as pre-commit hooks
whenever `.goosemem` changes. `GOOSE_CONTEXT_STRATEGY.md` explains the block
format and the retrieval-scoring rules behind the tests.

### Module docstrings

Every public module should have a module-level docstring describing:

- What the module does
- Key exports (classes, functions, constants)
- A brief usage example when helpful
- Related modules

### Imports

- Use canonical `db_monitor.*` import paths — never `from app.*` or
  `sys.path.insert`.
- Group imports: standard library → third-party → first-party.
- Use `from __future__ import annotations` in all modules.

## Testing

### Running tests

```bash
# Run the full core test suite
make monitor-test-core

# Run specific test files
uv run python -m pytest tests/test_schema_discovery.py -v --tb=short

# Run property-based (Hypothesis) tests
uv run python -m pytest tests/test_event_parser_property.py -v --tb=short

# Run integration tests (requires stack running)
make monitor-test-integration

# Run slow/live migration tests (requires PostgreSQL running)
uv run python -m pytest tests/test_migration_schema.py -m slow -v
```

### Test expectations

- **New code must have tests.** Bug fixes should include a regression test.
- Use the project's `conftest.py` fixtures where possible.
- Prefer synchronous assertion-based tests for pure functions.
- Use `@pytest.mark.anyio` for async tests.
- Tests that require a running PostgreSQL or broker should use `@pytest.mark.slow`.
- Hypothesis property-based tests are encouraged for edge-case validation.

### Test isolation

- Each test should create its own state — don't rely on test order.
- Global module-level state (e.g., `consumer_runtime` dicts) is automatically
  cleared by the `_reset_consumer_module_state` autouse fixture.
- Tests that need broker or DB infrastructure should use containers or the
  sandbox stack — not mock the entire infra.

## Pull Request Process

1. **Create an issue** describing the change (bug fix, feature, documentation,
   etc.) before starting work, unless the change is trivial.
2. **Create a branch** from `main` with a descriptive name:
   `fix/retention-cleanup-race`, `feat/tenant-quota-overrides`, etc.
3. **Make your changes** following the code style and testing guidelines above.
4. **Run the full lint + typecheck + core test suite** locally before pushing:
   ```bash
   make monitor-lint
   make monitor-typecheck
   make monitor-test-core
   make monitor-check-context
   ```
5. **Open a pull request** against `main`. Include:
   - A clear description of what the change does and why.
   - Links to related issues or ADRs.
   - Screenshots or CLI output for UI/UX changes.
6. **Address review feedback.** All PRs require at least one maintainer review.
7. **Ensure CI passes.** The CI pipeline runs lint, type checking, tests, and
   packaging smoke checks.

### PR size guidelines

- Keep PRs focused on a single concern. A 200-line change is easier to review
  than a 2000-line change.
- Large workstreams (like the ones tracked in `docs/BACKLOG.md`) may be
  broken into multiple sequential PRs.

## Documentation

- Changes that add or modify environment variables should update
  `docs/configuration.md`.
- New API endpoints should update `api-docs.md`.
- Architectural decisions should be recorded as ADRs in `docs/adr/`.
- The `docs/BACKLOG.md` file is the single source of truth for open work —
  update it when starting, changing, or closing an item, and record the shipped
  change in `CHANGELOG.md` instead of keeping a completed checklist.

## Release Process

Releases are automated via GitHub Actions:

1. A maintainer creates and pushes a signed annotated tag:
   ```bash
   git tag -s vX.Y.Z
   git push origin vX.Y.Z
   ```
2. CI verifies the tag signature, builds wheel/sdist, generates checksums,
   attests provenance, creates a GitHub Release, and publishes to PyPI.
3. The Release Drafter workflow continuously updates a draft changelog on
   `main`.

See `docs/deployment.md` for operational release workflows.
