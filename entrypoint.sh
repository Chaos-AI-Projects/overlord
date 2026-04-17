#!/usr/bin/env bash
set -euo pipefail

# Initialize the database schema on the mounted volume
overlord init

# Start the scheduler daemon
exec overlord daemon "$@"
