#!/bin/sh
set -eu

uv run python migrate.py apply
exec uv run python start_monitor.py