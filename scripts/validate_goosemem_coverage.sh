#!/usr/bin/env bash
# validate_goosemem_coverage.sh — Ensure .goosemem references all meaningful source files.
#
# Excludes:
#   - __init__.py files (trivial module markers)
#   - __pycache__, .venv, site-packages
#   - app/core/ (compatibility shims re-exporting from db_monitor)
#   - app/ingestion/ (compatibility shims re-exporting from db_monitor)
#   - app/api/ (compatibility shims re-exporting from db_monitor)
#   - app/repositories/ (covered by dedicated #repositories block)
#   - app/routes/ (covered by api block endpoint listings)
#   - src/db_monitor/ (all thin proxies to app/ per .goosehints architecture)
#
# Exit codes:
#   0 — all files covered
#   1 — coverage gaps found (prints offending files)
#
# Usage:
#   bash scripts/validate_goosemem_coverage.sh
#
set -euo pipefail

GOOSEMEM=".goosemem"
APP_DIR="app"

if [ ! -f "$GOOSEMEM" ]; then
  echo "ERROR: $GOOSEMEM not found" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 1. Collect app/ implementation files, excluding known shim directories
# ---------------------------------------------------------------------------
find_files() {
  find "$1" -name '*.py' \
    -not -name '__init__.py' \
    -not -path '*/__pycache__/*' \
    -not -path '*/.venv/*' \
    -not -path '*/site-packages/*' \
    -not -path 'app/core/*' \
    -not -path 'app/ingestion/*' \
    -not -path 'app/api/*' \
    -not -path 'app/repositories/*' \
    -not -path 'app/routes/*' \
    -not -path 'src/db_monitor/*' \
    -not -path 'app/app_factory.py' \
    -not -path 'app/extensions.py' \
    -not -path 'app/migrations.py' \
    -not -path 'app/start_monitor.py' \
    | sed 's|^./||' | sort -u
}

# ---------------------------------------------------------------------------
# 2. Collect all .py paths referenced in .goosemem
# ---------------------------------------------------------------------------
grep -ohE '[a-zA-Z0-9_/.-]+\.py' "$GOOSEMEM" \
  | sed 's|^|/|' | sort -u > /tmp/goosemem_refs.txt

# ---------------------------------------------------------------------------
# 3. Check a directory, excluding known shim paths
# ---------------------------------------------------------------------------
check_dir() {
  local dir="$1"
  local missing=0

  while IFS= read -r file; do
    local normalized="/${file}"
    if ! grep -qxF "$normalized" /tmp/goosemem_refs.txt; then
      echo "  GAP: $file"
      missing=$((missing + 1))
    fi
  done < <(find_files "$dir")

  return $missing
}

echo "=== .goosemem coverage validation ==="
echo ""

total_gaps=0

echo "--- $APP_DIR/ (implementation files, excluding shim directories) ---"
if check_dir "$APP_DIR" "app"; then
  echo "  (all covered)"
else
  total_gaps=$((total_gaps + $?))
fi
echo ""

echo "--- Stats ---"
blocks=$(grep -c '^=' "$GOOSEMEM" | awk '{print int($1/2)}')
lines=$(wc -l < "$GOOSEMEM")
bytes=$(wc -c < "$GOOSEMEM")
echo "  Blocks:     $blocks"
echo "  Lines:      $lines"
echo "  Bytes:      $bytes"
echo ""

if [ "$total_gaps" -gt 0 ]; then
  echo "FAIL: $total_gaps file(s) not referenced. Add them to the relevant .goosemem block."
  exit 1
else
  echo "PASS: all meaningful source files are covered."
  exit 0
fi
