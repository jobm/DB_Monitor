#!/bin/bash
set -euo pipefail

CONNECT_URL="http://connect:8083/connectors"

echo "Waiting for Kafka Connect REST API at $CONNECT_URL..."
until curl -s "$CONNECT_URL" >/dev/null; do
    sleep 3
done

echo "Kafka Connect is ready. Registering connectors..."

for f in /connectors/*.json; do
    echo "Registering $(basename "$f")..."
    curl -s -X POST -H "Content-Type: application/json" \
        --data @"$f" "$CONNECT_URL" \
        | jq .
done

echo "All connectors registered."
