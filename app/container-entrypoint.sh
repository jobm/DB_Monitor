#!/bin/sh
set -eu

/app/.venv/bin/python migrate.py apply
exec /app/.venv/bin/python start_monitor.py