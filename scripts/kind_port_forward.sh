#!/usr/bin/env bash
# Resilient kubectl port-forward supervisor for the Kind smoke cluster.
#
# kubectl port-forward exits whenever the target pod restarts or the
# connection blips. This script keeps the API and consumer forwards alive by
# restarting them whenever they drop, so the TUI (which expects
# http://localhost:8000) always has a live endpoint.
#
# Usage:
#   scripts/kind_port_forward.sh            # run in foreground, Ctrl-C to stop
#   scripts/kind_port_forward.sh --stop     # kill any existing forwards only
#
# Environment overrides (all optional):
#   KIND_CONTEXT   kubectl context      (default: kind-dbm)
#   NAMESPACE      namespace            (default: dbm-smoke)
#   RELEASE        Helm release name    (default: smoke)
#   API_PORT       host port for API    (default: 8000)
#   CONSUMER_PORT  host port, consumer  (default: 8001)
#   RETRY_DELAY    seconds between retries when a forward drops (default: 2)
#   KUBECTL        kubectl binary to use (auto-detected by default)
#   LOG_DIR        where per-forward logs go (default: /tmp/db-monitor-port-forward)

set -u

KIND_CONTEXT="${KIND_CONTEXT:-kind-dbm}"
NAMESPACE="${NAMESPACE:-dbm-smoke}"
RELEASE="${RELEASE:-smoke}"
API_PORT="${API_PORT:-8000}"
CONSUMER_PORT="${CONSUMER_PORT:-8001}"
RETRY_DELAY="${RETRY_DELAY:-2}"
LOG_DIR="${LOG_DIR:-/tmp/db-monitor-port-forward}"

API_SVC="${RELEASE}-db-monitor-api"
CONSUMER_SVC="${RELEASE}-db-monitor-consumer"

mkdir -p "$LOG_DIR"

log() { printf '%s [port-forward] %s\n' "$(date +%H:%M:%S)" "$*"; }

# Resolve a working kubectl. The snap wrapper (/snap/bin/kubectl) hangs for
# ~12s and exits 46 when snapd is not running, so prefer real binaries and
# verify the candidate actually responds.
resolve_kubectl() {
    if [[ -n "${KUBECTL:-}" ]]; then
        printf '%s\n' "$KUBECTL"
        return 0
    fi

    local candidate
    for candidate in \
        "$(command -v kubectl 2>/dev/null || true)" \
        "$HOME/kubectl" \
        /usr/local/bin/kubectl \
        /usr/bin/kubectl; do
        [[ -n "$candidate" && -x "$candidate" ]] || continue
        # Skip the snap wrapper; it is unusable when snapd is not up.
        case "$candidate" in
            /snap/*) continue ;;
        esac
        if timeout 5 "$candidate" version --client >/dev/null 2>&1; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done

    return 1
}

KUBECTL="$(resolve_kubectl || true)"

stop_existing() {
    # Kill only forwards targeting this release's services.
    pkill -f "port-forward svc/${API_SVC}" 2>/dev/null || true
    pkill -f "port-forward svc/${CONSUMER_SVC}" 2>/dev/null || true
}

if [[ "${1:-}" == "--stop" ]]; then
    stop_existing
    log "stopped existing ${API_SVC} / ${CONSUMER_SVC} forwards"
    exit 0
fi

if [[ -z "$KUBECTL" ]]; then
    log "error: no working kubectl found (the snap wrapper is skipped; install a real kubectl or set KUBECTL=/path/to/kubectl)"
    exit 1
fi
log "using kubectl: $KUBECTL"

CHILD_PIDS=()
cleanup() {
    log "shutting down"
    for pid in "${CHILD_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    stop_existing
    exit 0
}
trap cleanup INT TERM EXIT

supervise() {
    local label="$1" svc="$2" host_port="$3"
    local logfile="${LOG_DIR}/pf-${label}.log"
    while true; do
        log "[$label] forwarding localhost:${host_port} -> svc/${svc}:8000"
        "$KUBECTL" --context "$KIND_CONTEXT" -n "$NAMESPACE" \
            port-forward "svc/${svc}" "${host_port}:8000" >"$logfile" 2>&1
        local rc=$?
        # Do not hot-loop if the port is momentarily held by a stale process.
        log "[$label] forward exited (rc=${rc}); retrying in ${RETRY_DELAY}s (see ${logfile})"
        sleep "$RETRY_DELAY"
    done
}

stop_existing
log "context=${KIND_CONTEXT} namespace=${NAMESPACE} release=${RELEASE}"

supervise api "$API_SVC" "$API_PORT" &
CHILD_PIDS+=("$!")
supervise consumer "$CONSUMER_SVC" "$CONSUMER_PORT" &
CHILD_PIDS+=("$!")

log "forwards active: API http://localhost:${API_PORT}  consumer http://localhost:${CONSUMER_PORT}"
log "press Ctrl-C to stop"

wait
