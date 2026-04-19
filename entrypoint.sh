#!/usr/bin/env bash
set -euo pipefail

# Detect the UID of the mounted /home/overlord volume and adjust the overlord
# user to match, so files created inside the container have the correct
# ownership on the host.  Uses sed instead of usermod to avoid the shadow
# dependency.
OVERLORD_UID=1000
OVERLORD_GID=1000

VOLUME_UID=$(stat -c '%u' /home/overlord)
if [ "$VOLUME_UID" != "0" ] && [ "$VOLUME_UID" != "$OVERLORD_UID" ]; then
    sed -i "s/^overlord:x:${OVERLORD_UID}:/overlord:x:${VOLUME_UID}:/" /etc/passwd
    OVERLORD_UID="$VOLUME_UID"
fi

# Create required directories with correct ownership
mkdir -p /home/overlord/.local/share /home/overlord/brain
chown -R "${OVERLORD_UID}:${OVERLORD_GID}" /home/overlord/.local /home/overlord/brain

# Install claude-code into a persistent prefix on first run
export NPM_CONFIG_PREFIX="/home/overlord/.npm-global"
export PATH="${NPM_CONFIG_PREFIX}/bin:${PATH}"
if [ ! -x "${NPM_CONFIG_PREFIX}/bin/claude" ]; then
    npm install -g @anthropic-ai/claude-code
    chown -R "${OVERLORD_UID}:${OVERLORD_GID}" "${NPM_CONFIG_PREFIX}"
fi

cd /home/overlord/brain

# Initialize the database schema on the mounted volume
setpriv --reuid="$OVERLORD_UID" --regid="$OVERLORD_GID" --clear-groups \
    -- overlord init

# Start the scheduler daemon as overlord
exec setpriv --reuid="$OVERLORD_UID" --regid="$OVERLORD_GID" --clear-groups \
    -- overlord daemon "$@"
