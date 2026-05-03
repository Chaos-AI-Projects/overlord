#!/usr/bin/env bash
set -euo pipefail

# Build and smoke-test the overlord container image.
# Creates a GitHub issue on failure.
#
# Usage: ./scripts/build.sh [--repo OWNER/REPO] [--sha COMMIT_SHA]
#
# Environment:
#   GITHUB_REPO     - owner/repo (default: auto-detected from git remote)
#   COMMIT_SHA      - commit to report status on (default: HEAD)
#   SMOKE_TIMEOUT   - seconds to wait for container startup (default: 30)
#   CONTAINER_NAME  - name for the smoke-test container (default: overlord-smoke-test)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERLORD_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GITHUB_REPO="${GITHUB_REPO:-}"
COMMIT_SHA="${COMMIT_SHA:-}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-30}"
CONTAINER_NAME="${CONTAINER_NAME:-overlord-smoke-test}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo) GITHUB_REPO="$2"; shift 2 ;;
        --sha)  COMMIT_SHA="$2"; shift 2 ;;
        *)      echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# Auto-detect repo and sha if not provided
if [[ -z "$GITHUB_REPO" ]]; then
    GITHUB_REPO="$(git -C "$OVERLORD_DIR" remote get-url origin | sed -E 's|.*github\.com[:/]||; s|\.git$||')"
fi
if [[ -z "$COMMIT_SHA" ]]; then
    COMMIT_SHA="$(git -C "$OVERLORD_DIR" rev-parse HEAD)"
fi

log "Repository: $GITHUB_REPO"
log "Commit:     ${COMMIT_SHA:0:7}"

report_failure() {
    local description="$1"
    log "Reporting failure to GitHub: $description"
    gh issue create \
        --repo "$GITHUB_REPO" \
        --title "overlord: build failure — $description" \
        --body "Build failed at commit ${COMMIT_SHA:0:7} on $(date '+%Y-%m-%d %H:%M:%S').

**Error:** $description

**Triggered by:** \`ci-pull-and-build.sh\`" \
        || log "Warning: failed to create GitHub issue"
}

cleanup() {
    podman rm -f "$CONTAINER_NAME" 2>/dev/null || true
}
trap cleanup EXIT

# --- Step 1: Build ---
log "Building container image in $OVERLORD_DIR"
cd "$OVERLORD_DIR"
if ! nix --extra-experimental-features nix-command --extra-experimental-features flakes build .#container --out-link "${OVERLORD_DIR}/result"; then
    report_failure "nix build failed"
    exit 1
fi
log "Build succeeded"

# --- Step 2: Load image ---
log "Loading image into podman"
if ! podman load < "${OVERLORD_DIR}/result"; then
    report_failure "podman load failed"
    exit 1
fi
log "Image loaded"

# --- Step 3: Smoke test ---
log "Starting smoke-test container (timeout: ${SMOKE_TIMEOUT}s)"
podman rm -f "$CONTAINER_NAME" 2>/dev/null || true

podman run --rm -d \
    --name "$CONTAINER_NAME" \
    --network=host \
    --userns=keep-id:uid=1000,gid=1000 \
    overlord:latest

# Wait for the daemon to start responding
STARTED=false
for i in $(seq 1 "$SMOKE_TIMEOUT"); do
    if podman exec "$CONTAINER_NAME" overlord daemon-status --mcp-url http://localhost:8000/mcp 2>/dev/null; then
        STARTED=true
        break
    fi
    sleep 1
done

if ! $STARTED; then
    LOGS="$(podman logs "$CONTAINER_NAME" 2>&1 | tail -20)"
    report_failure "Smoke test failed: daemon did not start within ${SMOKE_TIMEOUT}s"
    log "Container logs:"
    echo "$LOGS"
    exit 1
fi

log "Smoke test passed — daemon is responsive"

# Cleanup is handled by trap; if we reach here, build is successful.
# We only report on failure per owner's request.
log "Build and smoke test succeeded for ${COMMIT_SHA:0:7}"
