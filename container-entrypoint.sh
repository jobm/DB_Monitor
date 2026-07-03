#!/bin/sh
set -eu

# Respect DB_SCHEMA_MODE for migration strategy.
# - "apply" (default): run migrations before starting
# - "validate": verify schema is up to date, then start
# - "skip": start without any schema check
SCHEMA_MODE="${DB_SCHEMA_MODE:-apply}"

case "${SCHEMA_MODE}" in
    apply)
        echo ">>> Applying database migrations..."
        db-monitor-migrate apply
        ;;
    validate)
        echo ">>> Validating database schema..."
        db-monitor-migrate validate
        ;;
    skip)
        echo ">>> Skipping database schema check (DB_SCHEMA_MODE=skip)"
        ;;
    *)
        echo ">>> Unknown DB_SCHEMA_MODE='${SCHEMA_MODE}', falling back to apply"
        db-monitor-migrate apply
        ;;
esac

echo ">>> Starting DB Monitor server..."
exec db-monitor-serve
