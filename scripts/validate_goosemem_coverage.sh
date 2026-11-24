#!/usr/bin/env bash
# validate_goosemem_coverage.sh — Ensure every module under src/db_monitor/
# is referenced by at least one .goosemem block.
#
# Coverage is block-scoped: a module counts only if a single block names its
# package path and its filename (or its full package/file.py path). A mention
# split across two blocks does not help, because Goose only ever has the
# retrieved block in context. __init__.py files are exempt (package markers);
# see scripts/validate_goosemem_coverage.py for the full rules.
#
# Kept as the stable entrypoint for CI and pre-commit; the block parsing and
# matching live in the Python module next to it.
#
# Exit codes:
#   2 — .goosemem or src/db_monitor/ missing
#   1 — unreferenced modules found
#   0 — every module is covered
#
# Usage:
#   bash scripts/validate_goosemem_coverage.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec python3 "$SCRIPT_DIR/validate_goosemem_coverage.py" "$@"
