#!/usr/bin/env bash
set -euo pipefail

cd /vault

# Initialize the database schema on the mounted volume
overlord init

# Start the scheduler daemon
exec overlord daemon "$@"
