#!/usr/bin/env bash
set -euo pipefail

# Build and smoke-test the overlord container image.
# Creates a GitHub issue on failure.
#
# Usage: ./scripts/build.sh [--repo OWNER/REPO] [--sha COMMIT_SHA]
#                           [--triggered-by LABEL]
#
# Environment:
#   GITHUB_REPO     - owner/repo (default: auto-detected from git remote)
#   COMMIT_SHA      - commit to report status on (default: HEAD)
#   SMOKE_TIMEOUT   - seconds to wait for container startup (default: 30)
#   CONTAINER_NAME  - name for the smoke-test container (default: overlord-smoke-test)
#   TRIGGERED_BY    - label naming the caller, quoted in failure reports
#                     (default: ci-pull-and-build.sh). Any other caller should
#                     pass --triggered-by so the report is not misleading.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OVERLORD_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

GITHUB_REPO="${GITHUB_REPO:-}"
COMMIT_SHA="${COMMIT_SHA:-}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-30}"
CONTAINER_NAME="${CONTAINER_NAME:-overlord-smoke-test}"
TRIGGERED_BY="${TRIGGERED_BY:-ci-pull-and-build.sh}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)          GITHUB_REPO="$2"; shift 2 ;;
        --sha)           COMMIT_SHA="$2"; shift 2 ;;
        --triggered-by)  TRIGGERED_BY="$2"; shift 2 ;;
        *)               echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# Auto-detect repo and sha if not provided
if [[ -z "$GITHUB_REPO" ]]; then
    GITHUB_REPO="$(git -C "$OVERLORD_DIR" remote get-url origin | sed -E 's|.*github\.com[:/]||; s|\.git$||')"
fi
if [[ -z "$COMMIT_SHA" ]]; then
    COMMIT_SHA="$(git -C "$OVERLORD_DIR" rev-parse HEAD)"
fi

SHORT_SHA="${COMMIT_SHA:0:7}"

log "Repository: $GITHUB_REPO"
log "Commit:     $SHORT_SHA"
log "Triggered by: $TRIGGERED_BY"

# Print the number of an OPEN issue whose title exactly matches $1, or nothing.
# Returns 1 only if the lookup itself failed, so the caller can fall through to
# reporting rather than silently dropping a build failure.
find_existing_open_issue() {
    local title="$1"
    local listing number state existing_title

    listing="$(gh issue list \
        --repo "$GITHUB_REPO" \
        --state open \
        --limit 100 \
        --search "$SHORT_SHA in:title" \
        --json number,state,title \
        --template '{{range .}}{{.number}}{{"\t"}}{{.state}}{{"\t"}}{{.title}}{{"\n"}}{{end}}' \
        2>/dev/null)" || return 1

    while IFS=$'\t' read -r number state existing_title; do
        if [[ -z "$number" ]]; then
            continue
        fi
        # Re-check state locally: a closed report must never suppress a new one.
        if [[ "${state^^}" != "OPEN" ]]; then
            continue
        fi
        if [[ "$existing_title" == "$title" ]]; then
            printf '%s\n' "$number"
            return 0
        fi
    done <<< "$listing"

    return 0
}

# Report a build failure as a GitHub issue, at most once per (SHA, error).
# Always fail-soft: any problem here must not mask the build failure itself.
report_failure() {
    local description="$1"
    # The SHA is in the title so the dedup lookup is an exact, cheap match.
    local title="overlord: build failure — $description ($SHORT_SHA)"
    local existing=""

    if ! existing="$(find_existing_open_issue "$title")"; then
        log "Warning: could not check for an existing failure issue; reporting anyway"
        existing=""
    fi

    if [[ -n "$existing" ]]; then
        log "Skipping duplicate failure report for $SHORT_SHA ($description) — already tracked by issue #$existing"
        return 0
    fi

    log "Reporting failure to GitHub: $description"
    gh issue create \
        --repo "$GITHUB_REPO" \
        --title "$title" \
        --body "Build failed at commit $SHORT_SHA on $(date '+%Y-%m-%d %H:%M:%S').

**Commit:** \`$SHORT_SHA\`

**Error:** $description

**Triggered by:** \`$TRIGGERED_BY\`" \
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
log "Build and smoke test succeeded for $SHORT_SHA"
