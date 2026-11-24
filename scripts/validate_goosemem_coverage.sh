#!/usr/bin/env bash
# validate_goosemem_coverage.sh — Ensure every package directory under
# src/db_monitor/ is referenced by at least one .goosemem block.
#
# .goosemem intentionally summarizes many packages instead of listing every
# file (see GOOSE_CONTEXT_STRATEGY.md), so coverage is measured at the
# directory level. File-level counts are printed for information only.
#
# Excludes:
#   - __pycache__ and other bytecode caches
#   - .venv / site-packages
#
# Exit codes:
#   2 — .goosemem or src/db_monitor/ missing
#   1 — unreferenced directories found
#   0 — every directory is covered
#
# Usage:
#   bash scripts/validate_goosemem_coverage.sh
#
set -euo pipefail

GOOSEMEM=".goosemem"
SRC_DIR="src/db_monitor"

if [ ! -f "$GOOSEMEM" ]; then
  echo "ERROR: $GOOSEMEM not found" >&2
  exit 2
fi

if [ ! -d "$SRC_DIR" ]; then
  echo "ERROR: $SRC_DIR not found (run from the repository root)" >&2
  exit 2
fi

# ---------------------------------------------------------------------------
# 1. Enumerate package directories that contain Python source
#    (an empty leftover directory is not worth mentioning in .goosemem).
# ---------------------------------------------------------------------------
mapfile -t dirs < <(
  find "$SRC_DIR" -name '*.py' \
    -not -path '*/__pycache__/*' \
    -not -path '*/.venv/*' \
    -not -path '*/site-packages/*' \
    | xargs -r -n1 dirname | LC_ALL=C sort -u
)

# ---------------------------------------------------------------------------
# 2. Check each directory for a mention in .goosemem
#
# A mention counts if the directory path appears with a non-identifier
# character on both sides, so "core/config/dataclasses/" and
# "src/db_monitor/core/config/dataclasses" both match, while "core" does not
# match inside "core_utils".
# ---------------------------------------------------------------------------
echo "=== .goosemem directory coverage ($SRC_DIR) ==="
echo ""

covered=0
total=0
gaps=()

for dir in "${dirs[@]}"; do
  rel="${dir#"$SRC_DIR"/}"
  [ "$rel" = "$dir" ] && continue          # the root, handled by any src/ mention
  total=$((total + 1))

  if grep -qE "(^|/)${rel}(/|[^A-Za-z0-9_]|$)" "$GOOSEMEM"; then
    covered=$((covered + 1))
  else
    gaps+=("$rel")
  fi
done

for gap in "${gaps[@]:-}"; do
  [ -n "$gap" ] && echo "  GAP: $SRC_DIR/$gap/"
done

echo ""
echo "--- Stats ---"
blocks=$(grep -c '^=' "$GOOSEMEM" | awk '{print int($1/2)}')
existing=$(find "$SRC_DIR" -name '*.py' -not -path '*/__pycache__/*' | wc -l)
referenced=$(
  grep -ohE "$SRC_DIR/[A-Za-z0-9_/.-]+\.py" "$GOOSEMEM" \
    | sed 's/^/.\//' | sort -u | wc -l
)
echo "  Blocks:          $blocks"
echo "  Lines:           $(wc -l < "$GOOSEMEM")"
echo "  Bytes:           $(wc -c < "$GOOSEMEM")"
echo "  Directories:     $covered/$total covered"
echo "  Files by path:   $referenced named / $existing existing (info only)"
echo ""

if [ "${#gaps[@]}" -gt 0 ]; then
  echo "FAIL: ${#gaps[@]} director$([ "${#gaps[@]}" -eq 1 ] && echo y || echo ies) not referenced."
  echo "Add each to the FILES:/references of the block that owns it."
  exit 1
fi

echo "PASS: every package directory is referenced."
exit 0
