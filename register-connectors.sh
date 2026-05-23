#!/bin/bash
set -euo pipefail

if [[ -d "/connectors" ]]; then
    DEFAULT_CONNECT_URL="http://connect:8083/connectors"
    DEFAULT_TEMPLATE_FILE="/connectors/postgres-template.json"
    DEFAULT_SOURCES_FILE="/connectors/sources.json"
else
    DEFAULT_CONNECT_URL="http://localhost:8083/connectors"
    DEFAULT_TEMPLATE_FILE="connectors/postgres-template.json"
    DEFAULT_SOURCES_FILE="connectors/sources.json"
fi

CONNECT_URL="${CONNECT_URL:-$DEFAULT_CONNECT_URL}"
TEMPLATE_FILE="${TEMPLATE_FILE:-$DEFAULT_TEMPLATE_FILE}"
SOURCES_FILE="${SOURCES_FILE:-$DEFAULT_SOURCES_FILE}"
CONNECTOR_DATABASE_PORT="${CONNECTOR_DATABASE_PORT:-5432}"
CONNECTOR_DATABASE_USER="${CONNECTOR_DATABASE_USER:-postgres}"
CONNECTOR_DATABASE_PASSWORD="${CONNECTOR_DATABASE_PASSWORD:-postgres}"
CONNECTOR_DATABASE_DBNAME="${CONNECTOR_DATABASE_DBNAME:-postgres}"
CONNECTOR_HISTORY_BOOTSTRAP_SERVERS="${CONNECTOR_HISTORY_BOOTSTRAP_SERVERS:-kafka:9092}"
CONNECTOR_STATUS_RETRIES="${CONNECTOR_STATUS_RETRIES:-20}"
CONNECTOR_STATUS_DELAY_SECONDS="${CONNECTOR_STATUS_DELAY_SECONDS:-3}"
DRY_RUN="${DRY_RUN:-false}"

manifest_errors() {
    jq -r '
        def require($condition; $message):
            if $condition then empty else $message end;

        .connectors as $connectors
        | if ($connectors | type) != "array" then
              "sources.json: connectors must be an array"
          else empty end,
          if ($connectors | length) == 0 then
              "sources.json: connectors must not be empty"
          else empty end,
          (
              $connectors
              | to_entries[]?
              | .key as $index
              | .value as $connector
              | if ($connector.enabled // true) then
                    require(
                        (($connector.source_name // "") | length) > 0;
                        "sources.json: connectors[\($index)].source_name is required"
                    ),
                    require(
                        (($connector.database_hostname // "") | length) > 0;
                        "sources.json: connectors[\($index)].database_hostname is required"
                    ),
                    require(
                        (($connector.tables | type) == "array")
                        and (($connector.tables | length) > 0);
                        "sources.json: connectors[\($index)].tables must be a non-empty array"
                    ),
                    require(
                        all($connector.tables[]?; (type == "string") and (length > 0));
                        "sources.json: connectors[\($index)].tables entries must be non-empty strings"
                    )
                else empty end
          ),
          (
              [
                  $connectors[]
                  | select(.enabled // true)
                  | .source_name
              ]
              | group_by(.)[]?
              | select(length > 1)
              | "sources.json: duplicate source_name \(.[0])"
          ),
          (
              [
                  $connectors[]
                  | select(.enabled // true)
                  | (.connector_name // (.source_name + "-connector"))
              ]
              | group_by(.)[]?
              | select(length > 1)
              | "sources.json: duplicate connector name \(.[0])"
          ),
          (
              [
                  $connectors[]
                  | select(.enabled // true)
                  | (.slot_name // (.source_name + "_slot"))
              ]
              | group_by(.)[]?
              | select(length > 1)
              | "sources.json: duplicate slot name \(.[0])"
          )
    ' "$SOURCES_FILE"
}

render_connectors() {
    jq -cn \
        --slurpfile template "$TEMPLATE_FILE" \
        --slurpfile sources "$SOURCES_FILE" \
        --arg db_port "$CONNECTOR_DATABASE_PORT" \
        --arg db_user "$CONNECTOR_DATABASE_USER" \
        --arg db_password "$CONNECTOR_DATABASE_PASSWORD" \
        --arg db_name "$CONNECTOR_DATABASE_DBNAME" \
        --arg history_bootstrap "$CONNECTOR_HISTORY_BOOTSTRAP_SERVERS" '
        $sources[0].connectors[]
        | select(.enabled // true)
        | {
            name: (.connector_name // (.source_name + "-connector")),
            config: (
                $template[0].config
                + {
                    "database.hostname": .database_hostname,
                    "database.port": $db_port,
                    "database.user": $db_user,
                    "database.password": $db_password,
                    "database.dbname": $db_name,
                    "database.server.name": .source_name,
                    "topic.prefix": .source_name,
                    "table.include.list": (.tables | join(",")),
                    "slot.name": (.slot_name // (.source_name + "_slot")),
                    "database.history.kafka.bootstrap.servers": $history_bootstrap,
                    "database.history.kafka.topic": (
                        .history_topic // ("dbhistory." + .source_name)
                    )
                }
                + (.config_overrides // {})
            )
        }
    '
}

desired_topics() {
    jq -r '
        .connectors[]
        | select(.enabled // true)
        | .source_name as $source_name
        | .tables[]
        | "\($source_name).\(.)"
    ' "$SOURCES_FILE"
}

if [[ ! -f "$TEMPLATE_FILE" ]]; then
    echo "Missing connector template: $TEMPLATE_FILE" >&2
    exit 1
fi

if [[ ! -f "$SOURCES_FILE" ]]; then
    echo "Missing connector source manifest: $SOURCES_FILE" >&2
    exit 1
fi

validation_errors="$(manifest_errors)"
if [[ -n "$validation_errors" ]]; then
    printf '%s\n' "$validation_errors" >&2
    exit 1
fi

if [[ "$DRY_RUN" != "true" ]]; then
    echo "Waiting for Kafka Connect REST API at $CONNECT_URL..."
    until curl -s "$CONNECT_URL" >/dev/null; do
        sleep 3
    done

    echo "Kafka Connect is ready. Registering connectors..."
else
    echo "DRY_RUN enabled. Skipping Kafka Connect readiness check."
fi

registered_connectors=()

while IFS= read -r payload; do
    connector_name=$(printf '%s' "$payload" | jq -r '.name')
    echo "Registering $connector_name..."
    if [[ "$DRY_RUN" == "true" ]]; then
        printf '%s\n' "$payload" | jq .
        registered_connectors+=("$connector_name")
        continue
    fi

    connector_config=$(printf '%s' "$payload" | jq -c '.config')
    curl -s -X PUT -H "Content-Type: application/json" \
        --data "$connector_config" "$CONNECT_URL/$connector_name/config" \
        | jq .
    registered_connectors+=("$connector_name")
done < <(render_connectors)

if [[ "$DRY_RUN" != "true" ]]; then
    for connector_name in "${registered_connectors[@]}"; do
        echo "Validating status for $connector_name..."
        running=false
        for ((attempt=1; attempt<=CONNECTOR_STATUS_RETRIES; attempt++)); do
            status_json=$(curl -s "$CONNECT_URL/$connector_name/status")
            connector_state=$(printf '%s' "$status_json" | jq -r '.connector.state // ""')
            tasks_ok=$(printf '%s' "$status_json" | jq -r 'if (.tasks | length) == 0 then "true" else all(.tasks[]; .state == "RUNNING") end')

            if [[ "$connector_state" == "RUNNING" && "$tasks_ok" == "true" ]]; then
                running=true
                break
            fi

            echo "  Attempt $attempt/$CONNECTOR_STATUS_RETRIES: state=$connector_state tasks_ok=$tasks_ok"
            sleep "$CONNECTOR_STATUS_DELAY_SECONDS"
        done

        if [[ "$running" != "true" ]]; then
            echo "Connector $connector_name failed to reach RUNNING state" >&2
            curl -s "$CONNECT_URL/$connector_name/status" | jq . >&2
            exit 1
        fi
    done
fi

echo "Derived KAFKA_TOPICS from sources manifest:"
desired_topics | jq -R . | jq -s .

echo "All connectors registered."
