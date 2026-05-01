#!/usr/bin/env bash
set -euo pipefail

# Configuration (override via environment)
LATEST_IMAGE="${LATEST_IMAGE:-overlord:latest}"
DATA_DIR="${DATA_DIR:-$HOME/thebot}"
TZ="${TZ:-Australia/Sydney}"
GRACE_PERIOD="${GRACE_PERIOD:-300}"  # seconds (5 minutes)
RESTART_DELAY="${RESTART_DELAY:-60}"
CONTAINER_NAME="${CONTAINER_NAME:-overlord}"
IMAGE_STATE_FILE="${IMAGE_STATE_FILE:-${XDG_CONFIG_HOME:-$HOME/.config}/overlord/image-state}"
PODMAN_SOCKET="${PODMAN_SOCKET:-/run/user/$(id -u)/podman/podman.sock}"

# --- State file functions ---

load_state() {
    if [[ -f "$IMAGE_STATE_FILE" ]]; then
        # shellcheck disable=SC1090
        source "$IMAGE_STATE_FILE"
        echo "${current:-}"
    fi
}

save_state() {
    local image="$1"
    mkdir -p "$(dirname "$IMAGE_STATE_FILE")"
    echo "current=$image" > "$IMAGE_STATE_FILE"
}

# --- Container management ---

run_container() {
    local image="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting container with image: $image"
    podman run --rm \
        --name "$CONTAINER_NAME" \
        --network=host \
        --userns=keep-id:uid=1000,gid=1000 \
        -e TZ="$TZ" \
        -e CONTAINER_HOST=unix:///run/podman/podman.sock \
        -v "$DATA_DIR:/home/overlord" \
        -v "$PODMAN_SOCKET:/run/podman/podman.sock" \
        "$image"
}

# --- Signal handling ---

cleanup() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Caught signal, stopping container..."
    podman stop "$CONTAINER_NAME" 2>/dev/null || true
    exit 0
}

trap cleanup SIGTERM SIGINT

# --- Main loop ---

main() {
    local current_image
    current_image="$(load_state)"

    while true; do
        # Always try :latest first
        local start_time=$SECONDS
        run_container "$LATEST_IMAGE" || true
        local elapsed=$(( SECONDS - start_time ))

        if (( elapsed >= GRACE_PERIOD )); then
            # Success — :latest ran long enough, promote it
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Image $LATEST_IMAGE ran for ${elapsed}s (>= ${GRACE_PERIOD}s), marking as good"
            save_state "$LATEST_IMAGE"
            current_image="$LATEST_IMAGE"
        else
            # Startup failure — try fallback
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Image $LATEST_IMAGE exited after ${elapsed}s (< ${GRACE_PERIOD}s)"

            if [[ -n "$current_image" && "$current_image" != "$LATEST_IMAGE" ]]; then
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] Falling back to known-good image: $current_image"
                run_container "$current_image" || true
            else
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] No fallback image available, will retry :latest after delay"
            fi
        fi

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Restarting in ${RESTART_DELAY}s..."
        sleep "$RESTART_DELAY"
    done
}

main "$@"
