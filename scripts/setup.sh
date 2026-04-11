#!/usr/bin/env bash

set -e

echo "========================================="
echo "  Setting up DB Monitor Environment      "
echo "========================================="

# Check for uv
if ! command -v uv &> /dev/null; then
    echo "[!] uv is not installed. Please install it first: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

# Ensure .env exists
if [ ! -f .env ]; then
    echo "[-] No .env found. Creating a default one..."
    cat > .env <<EOF
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=orderdb.public.orders
POSTGRES_URL=postgresql+asyncpg://postgres:postgres@localhost:5437/postgres
EOF
    echo "[+] Created default .env"
else
    echo "[✓] .env already exists."
fi

# Install dependencies using uv
echo "[-] Syncing dependencies using uv..."
cd app
uv sync
echo "[+] Dependencies installed."

echo "========================================="
echo "  Setup Complete!                        "
echo "  You can now run: make monitor-up       "
echo "========================================="
