# API Stability And Compatibility

This document defines the public Python API contract for DB Monitor.
Anything outside this contract is internal and may change between releases
without deprecation notices.

## Supported Public Python Modules

The modules listed below are public and covered by compatibility guarantees:

- `db_monitor`
- `db_monitor.__main__`
- `db_monitor.asgi`
- `db_monitor.cli`
- `db_monitor.main`
- `db_monitor.start_monitor`

### Public Symbols Covered By The Contract

- `db_monitor.__version__`
- `db_monitor.asgi.app`
- `db_monitor.cli.main`
- `db_monitor.cli.serve`
- `db_monitor.cli.migrate`
- `db_monitor.main.app`
- `db_monitor.start_monitor.main`

Runtime-facing namespace proxies such as `db_monitor.core.*`,
`db_monitor.ingestion.*`, `db_monitor.routes`, and `db_monitor.repositories.*`
remain operational but are not yet part of this long-term compatibility
contract until they are explicitly listed here.

## Deprecation And Removal Policy

When a public module or symbol is deprecated, DB Monitor follows all rules
below:

1. A replacement path or behavior is documented in release notes.
2. The deprecated API remains available for at least 2 minor releases.
3. The deprecated API remains available for at least 90 calendar days.
4. Removals only happen after both minimum windows above are satisfied.

## Versioning Policy

DB Monitor uses semantic versioning (`MAJOR.MINOR.PATCH`) for package releases.

- `PATCH`: bug fixes and internal changes with no public API break.
- `MINOR`: backward-compatible feature additions and deprecations.
- `MAJOR`: breaking changes to public APIs listed in this document.

## Compatibility Matrix

| Package version | Python versions | Public API compatibility |
| --- | --- | --- |
| `0.0.x` | `3.11`, `3.12` | Compatibility guaranteed only for APIs listed in this document |
| `0.y.z` (future pre-1.0 minors) | `3.11+` per release notes | Same: only listed public APIs are guaranteed |
| `1.x` and later | declared per release | Full semver guarantees for listed public APIs |

## Contract Source Of Truth

Machine-readable contract constants live in
`src/db_monitor/public_api.py` and are validated by tests.
