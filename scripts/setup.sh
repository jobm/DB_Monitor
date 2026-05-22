#!/usr/bin/env bash

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

echo "========================================="
echo "  Setting up DB Monitor Environment      "
echo "========================================="

# Check for uv
if ! command -v uv &> /dev/null; then
    echo "[!] uv is not installed. Please install it first: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

# Ensure secrets exist for compose-based runs.
mkdir -p "$repo_root/secrets"

if [ ! -f "$repo_root/secrets/monitor_postgres_url" ]; then
    echo "[-] Creating secrets/monitor_postgres_url..."
    printf '%s\n' \
        'postgresql+asyncpg://postgres:postgres@postgres-monitor:5432/postgres' \
        > "$repo_root/secrets/monitor_postgres_url"
fi

if [ ! -f "$repo_root/secrets/monitor_jwt_secret" ]; then
    echo "[-] Creating secrets/monitor_jwt_secret..."
    printf '%s\n' 'dev-insecure-secret-change-me-please-rotate' \
        > "$repo_root/secrets/monitor_jwt_secret"
fi

# Ensure repo .env exists for local app runs.
if [ ! -f "$repo_root/.env" ]; then
    echo "[-] No .env found. Creating a default one..."
    cat > "$repo_root/.env" <<EOF
KAFKA_BROKER=localhost:9093
POSTGRES_URL=postgresql+asyncpg://postgres:postgres@localhost:5437/postgres
ALLOW_BOOTSTRAP=true
APP_ENV=development
DB_SCHEMA_MODE=apply
EOF
    echo "[+] Created default .env"
else
    echo "[✓] .env already exists."
fi

# Install dependencies using uv.
echo "[-] Syncing app dependencies using uv..."
cd "$repo_root/app"
uv sync --group dev

echo "[-] Syncing TUI dependencies using uv..."
cd "$repo_root/tui"
uv sync

echo "[+] Dependencies installed."

echo "========================================="
echo "  Setup Complete!                        "
echo "  You can now run: make monitor-up       "
echo "  Or run locally: cd app && uv run python start_monitor.py"
echo "========================================="
