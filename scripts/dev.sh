#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

DATABASE_URL="${DATABASE_URL:-postgres://saas:saas@localhost:5433/saas?sslmode=disable}"
TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-localhost:7233}"
DELIVERY_GRPC_ADDRESS="${DELIVERY_GRPC_ADDRESS:-localhost:9097}"
GRAPHQL_ADDRESS="${GRAPHQL_ADDRESS:-localhost:8081}"
export DATABASE_URL TEMPORAL_ADDRESS DELIVERY_GRPC_ADDRESS GRAPHQL_ADDRESS

FORCE_INSTALL=0
DO_DOWN=0
DOCKER_STARTED=0
CLEANED_UP=0
BIN_DIR=""
SERVICE_PIDS=()
SERVICE_NAMES=()
DOCKER_CMD=(docker)
PROBE_HOST=""
PROBE_PORT=""

usage() {
  cat <<'EOF'
Usage: ./scripts/dev.sh [options]

Start PostgreSQL, Temporal, the delivery service, the GraphQL API, and the
React dashboard for local development.

Options:
      --install   Force a clean frontend install (`npm ci` in web/)
      --down      Stop Docker Compose services (PostgreSQL and Temporal)
  -h, --help      Show this help message
EOF
}

log() {
  printf '[dev] %s\n' "$*"
}

err() {
  printf '[dev] %s\n' "$*" >&2
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    err "Required command '$cmd' was not found in PATH."
    exit 1
  fi
}

check_docker_compose() {
  require_cmd docker
  if ! docker compose version >/dev/null 2>&1; then
    err "Docker Compose ('docker compose') is not available."
    exit 1
  fi

  local info_out=""
  if info_out="$(docker info 2>&1)"; then
    DOCKER_CMD=(docker)
    return 0
  fi

  if grep -qi 'permission denied' <<<"$info_out"; then
    if command -v sudo >/dev/null 2>&1 && { sudo -n docker info >/dev/null 2>&1 || [[ -t 0 ]]; }; then
      log "Docker socket requires elevated privileges; using 'sudo docker' for Docker Compose commands."
      DOCKER_CMD=(sudo docker)
      if ! "${DOCKER_CMD[@]}" info >/dev/null 2>&1; then
        err "Failed to connect to Docker with 'sudo docker'."
        exit 1
      fi
      return 0
    fi
    err "Permission denied connecting to the Docker socket. Add your user to the 'docker' group (sudo usermod -aG docker \"\$USER\" && re-login) or run interactively."
    exit 1
  fi

  local relevant_err=""
  relevant_err="$(grep -iE 'error|failed|cannot|connect|denied|dial|permission' <<<"$info_out" || true)"
  if [[ -z "$relevant_err" ]]; then
    relevant_err="$info_out"
  fi
  err "Docker daemon is not running or not reachable:"
  printf '%s\n' "$relevant_err" >&2
  exit 1
}

prefix_logs() {
  local label="$1"
  local line
  while IFS= read -r line; do
    printf '[%s] %s\n' "$label" "$line"
  done
  if [[ -n "$line" ]]; then
    printf '[%s] %s\n' "$label" "$line"
  fi
}

split_host_port() {
  local addr="$1"
  local host="" port=""

  if [[ "$addr" =~ ^\[([^\]]*)\]:([0-9]+)$ ]]; then
    host="${BASH_REMATCH[1]}"
    port="${BASH_REMATCH[2]}"
  elif [[ "$addr" =~ ^([^:]*):([0-9]+)$ ]]; then
    host="${BASH_REMATCH[1]}"
    port="${BASH_REMATCH[2]}"
  else
    err "Invalid host:port address: '$addr'"
    return 1
  fi

  case "$host" in
    "" | "0.0.0.0" | "::")
      host="127.0.0.1"
      ;;
  esac

  PROBE_HOST="$host"
  PROBE_PORT="$port"
}

tcp_ready() {
  local host="$1"
  local port="$2"
  if command -v timeout >/dev/null 2>&1; then
    timeout 2 bash -c 'exec 3<>"/dev/tcp/$1/$2"' _ "$host" "$port" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1
  fi
}

wait_for_tcp() {
  local name="$1"
  local addr="$2"
  local timeout_seconds="$3"
  local pid="${4:-}"

  split_host_port "$addr"

  local attempts=0
  local max_attempts=$((timeout_seconds * 2))
  while ((attempts < max_attempts)); do
    if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
      err "$name exited before becoming ready on $addr."
      return 1
    fi
    if tcp_ready "$PROBE_HOST" "$PROBE_PORT"; then
      return 0
    fi
    sleep 0.5
    attempts=$((attempts + 1))
  done

  err "Timed out after ${timeout_seconds}s waiting for $name on $addr."
  return 1
}

stop_tree() {
  local pid="$1"
  local signal="${2:-TERM}"
  local child

  if command -v pgrep >/dev/null 2>&1; then
    while IFS= read -r child; do
      [[ -n "$child" ]] || continue
      stop_tree "$child" "$signal"
    done < <(pgrep -P "$pid" 2>/dev/null || true)
  fi

  kill -s "$signal" "$pid" 2>/dev/null || true
}

cleanup() {
  if [[ "$CLEANED_UP" -eq 1 ]]; then
    return 0
  fi
  CLEANED_UP=1

  local i pid remaining_pids=()
  for ((i = ${#SERVICE_PIDS[@]} - 1; i >= 0; i--)); do
    pid="${SERVICE_PIDS[i]}"
    if kill -0 "$pid" 2>/dev/null; then
      stop_tree "$pid" TERM
    fi
  done

  local wait_steps=50
  while ((wait_steps > 0)); do
    remaining_pids=()
    if [[ ${#SERVICE_PIDS[@]} -gt 0 ]]; then
      for pid in "${SERVICE_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
          remaining_pids+=("$pid")
        fi
      done
    fi
    if [[ ${#remaining_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 0.1
    wait_steps=$((wait_steps - 1))
  done

  for ((i = ${#SERVICE_PIDS[@]} - 1; i >= 0; i--)); do
    pid="${SERVICE_PIDS[i]}"
    if kill -0 "$pid" 2>/dev/null; then
      stop_tree "$pid" KILL
    fi
  done

  if [[ ${#SERVICE_PIDS[@]} -gt 0 ]]; then
    wait "${SERVICE_PIDS[@]}" 2>/dev/null || true
  fi

  if [[ -n "$BIN_DIR" && -d "$BIN_DIR" ]]; then
    rm -rf -- "$BIN_DIR"
  fi
}

handle_signal() {
  trap - INT TERM
  echo ""
  log "Stopping local services..."
  cleanup
  if [[ "$DOCKER_STARTED" -eq 1 ]]; then
    log "PostgreSQL and Temporal are still running in Docker. Run './scripts/dev.sh --down' to stop them."
  fi
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --install)
      FORCE_INSTALL=1
      shift
      ;;
    --down)
      DO_DOWN=1
      shift
      ;;
    *)
      err "Unknown option: $1"
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$DO_DOWN" -eq 1 ]]; then
  check_docker_compose
  log "Stopping PostgreSQL and Temporal..."
  "${DOCKER_CMD[@]}" compose down
  exit 0
fi

trap 'cleanup' EXIT
trap 'handle_signal INT' INT
trap 'handle_signal TERM' TERM

check_docker_compose
require_cmd go
require_cmd npm

log "Starting PostgreSQL and Temporal..."
"${DOCKER_CMD[@]}" compose up -d --wait
DOCKER_STARTED=1

log "Waiting for Temporal at $TEMPORAL_ADDRESS..."
wait_for_tcp "Temporal gRPC" "$TEMPORAL_ADDRESS" 60
if [[ "$TEMPORAL_ADDRESS" == "localhost:7233" ]]; then
  wait_for_tcp "Temporal UI" "localhost:8080" 60
fi

if [[ "$FORCE_INSTALL" -eq 1 || ! -d "$ROOT_DIR/web/node_modules" ]]; then
  log "Installing frontend dependencies in web/..."
  (cd "$ROOT_DIR/web" && npm ci)
fi

BIN_DIR="$(mktemp -d)"
log "Building Go services..."
go build -o "$BIN_DIR/delivery" ./cmd/delivery
go build -o "$BIN_DIR/api" ./cmd/api

log "Starting delivery service on $DELIVERY_GRPC_ADDRESS..."
"$BIN_DIR/delivery" > >(prefix_logs "delivery") 2>&1 &
delivery_pid=$!
SERVICE_PIDS+=("$delivery_pid")
SERVICE_NAMES+=("delivery")
wait_for_tcp "delivery gRPC" "$DELIVERY_GRPC_ADDRESS" 30 "$delivery_pid"

log "Starting GraphQL API on $GRAPHQL_ADDRESS..."
"$BIN_DIR/api" > >(prefix_logs "api") 2>&1 &
api_pid=$!
SERVICE_PIDS+=("$api_pid")
SERVICE_NAMES+=("api")
wait_for_tcp "GraphQL API" "$GRAPHQL_ADDRESS" 30 "$api_pid"

log "Starting React dashboard..."
(cd "$ROOT_DIR/web" && exec npm run dev) > >(prefix_logs "web") 2>&1 &
web_pid=$!
SERVICE_PIDS+=("$web_pid")
SERVICE_NAMES+=("web")

log "All services started:"
log "  Dashboard:     http://localhost:5173 (see Vite output above for actual URL)"
log "  GraphQL API:   http://${GRAPHQL_ADDRESS}/query"
log "  Delivery gRPC: ${DELIVERY_GRPC_ADDRESS}"
log "  Temporal UI:   http://localhost:8080"
log "Press Ctrl+C to stop the local services."

while true; do
  for i in "${!SERVICE_PIDS[@]}"; do
    pid="${SERVICE_PIDS[i]}"
    name="${SERVICE_NAMES[i]}"
    if ! kill -0 "$pid" 2>/dev/null; then
      status=0
      wait "$pid" || status=$?
      err "$name exited unexpectedly (status $status)."
      exit "$(( status == 0 ? 1 : status ))"
    fi
  done
  sleep 0.5
done
