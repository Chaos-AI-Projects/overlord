#!/usr/bin/env bash
set -euo pipefail

# Detect the UID of the mounted /data volume and adjust the overlord user to match,
# so files created inside the container have the correct ownership on the host.
VOLUME_UID=$(stat -c '%u' /data)
if [ "$VOLUME_UID" != "0" ]; then
    CURRENT_UID=$(id -u overlord)
    if [ "$VOLUME_UID" != "$CURRENT_UID" ]; then
        usermod -u "$VOLUME_UID" overlord
        # Fix ownership of app and working dirs after UID change
        chown -R overlord:overlord /app /data /vault
    fi
fi

cd /vault

# Initialize the database schema on the mounted volume
su -s /bin/bash overlord -c "overlord init"

# Start the scheduler daemon as overlord
exec su -s /bin/bash overlord -c "exec overlord daemon $*"
