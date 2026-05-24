#!/bin/sh
set -eu

/app/.venv/bin/python -m db_monitor.cli migrate apply
exec /app/.venv/bin/python -m db_monitor.start_monitor