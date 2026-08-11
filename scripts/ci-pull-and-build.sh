#!/usr/bin/env bash
set -euo pipefail

# Pull from GitHub and trigger a build if the master branch was updated.
# Designed to be run on a schedule (e.g., cron or overlord job).
#
# Note: Expects a clean working tree. The fast-forward merge will fail
# if there are uncommitted local changes.
#
# Usage: ./scripts/ci-pull-and-build.sh [--repo-dir PATH]
#
# Environment:
#   REPO_DIR  - path to the git repository (default: parent of scripts/)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-dir) REPO_DIR="$2"; shift 2 ;;
        *)          echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

cd "$REPO_DIR"

# Record the current HEAD before pulling
OLD_HEAD="$(git rev-parse HEAD)"
log "Current HEAD: ${OLD_HEAD:0:7}"

# Fetch and fast-forward master
log "Pulling from origin..."
git fetch origin master
git merge-base --is-ancestor origin/master HEAD && {
    log "Already up to date. Nothing to build."
    exit 0
}

# There are new commits — update the working tree
git merge --ff-only origin/master || {
    log "Error: cannot fast-forward master. Manual intervention needed."
    exit 1
}

NEW_HEAD="$(git rev-parse HEAD)"
log "Updated: ${OLD_HEAD:0:7} -> ${NEW_HEAD:0:7}"

# Trigger the build
log "Master updated, starting build..."
exec "$SCRIPT_DIR/build.sh" --sha "$NEW_HEAD" --triggered-by "ci-pull-and-build.sh"
