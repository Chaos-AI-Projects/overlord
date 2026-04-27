#!/usr/bin/env bash
# overlord_job.sh — Consumer job that feeds messages to Claude Code.
#
# The overlord scheduler passes consumed messages as a JSON array on stdin.
# This script extracts the payloads, concatenates them into a single prompt,
# invokes `claude -p` (print mode) in the vault directory, and emits
# structured JSON output on stdout.
#
# Expected stdin format (from executor):
#   [{"key": "...", "consumer": "...", "payload": "...", "subject": "...", "date": "..."}, ...]
#
# Required stdout format (for executor):
#   {"consumer": null, "message": "..."}

set -euo pipefail

# Read all stdin into a variable.
INPUT="$(cat)"

if [ -z "$INPUT" ] || [ "$INPUT" = "[]" ]; then
    echo '{"consumer": null, "message": "No messages to process."}'
    exit 0
fi

# Extract and concatenate all payload fields from the JSON array.
# Each payload is a JSON string from the messages table; use jq to
# decode them into plain text separated by double newlines.
PROMPT="$(echo "$INPUT" | jq -r '[.[].payload] | join("\n\n---\n\n")')"

if [ -z "$PROMPT" ]; then
    echo '{"consumer": null, "message": "Empty payload in messages."}'
    exit 0
fi

# Invoke Claude Code in print mode.  The working directory should be
# the vault (set by the daemon's cwd), so Claude picks up the vault's
# CLAUDE.md automatically.
RESPONSE="$(echo "$PROMPT" | claude -p --output-format text --dangerously-skip-permissions 2>/dev/null)" || {
    # If claude fails, still emit valid JSON.
    echo '{"consumer": null, "message": "Claude invocation failed."}'
    exit 0
}

# Emit structured output.  Use jq to safely JSON-encode the response.
jq -n --arg msg "$RESPONSE" '{"consumer": null, "message": $msg}'
