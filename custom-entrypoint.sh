#!/bin/bash
set -e

echo "Starting Kafka Connect..."
/docker-entrypoint.sh &

# Wait for Kafka Connect REST API
until curl -s http://localhost:8083/connectors >/dev/null; do
    echo "Kafka Connect not ready yet... retrying in 3s"
    sleep 3
done

echo "Kafka Connect is ready — registering connectors..."
bash /register-connectors.sh

# Bring Kafka Connect to foreground
wait -n
