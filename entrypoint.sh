#!/usr/bin/env bash
set -euo pipefail

# Detect the UID of the mounted /data volume and adjust the overlord user to match,
# so files created inside the container have the correct ownership on the host.
VOLUME_UID=$(stat -c '%u' /data)
if [ "$VOLUME_UID" != "0" ]; then
    CURRENT_UID=$(id -u overlord)
    if [ "$VOLUME_UID" != "$CURRENT_UID" ]; then
        usermod -u "$VOLUME_UID" overlord
        # Fix ownership of app dir after UID change
        chown -R overlord:overlord /app
    fi
fi

cd /home/overlord

# Initialize the database schema on the mounted volume
# Note: su resets the environment, so we must pass XDG_DATA_HOME explicitly
su -s /bin/bash overlord -c "XDG_DATA_HOME=/data overlord init"

# Start the scheduler daemon as overlord
exec su -s /bin/bash overlord -c "XDG_DATA_HOME=/data exec overlord daemon $*"
