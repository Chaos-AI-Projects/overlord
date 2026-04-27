---
description: Rotate the overlord daemon log file
---

Rotate the overlord daemon log file.

This closes and reopens the daemon's log file handler, allowing external tools (like `logrotate`) to
rename the old log file beforehand. The daemon will start writing to a fresh file at the original path.

## Log file location

Find the current log file path (does not require the daemon to be running):

```bash
overlord log-path
```

By default the daemon writes logs to `<data-dir>/overlord.log` (typically `~/.local/share/overlord/overlord.log`).
This can be overridden with `--log-file PATH` or disabled entirely with `--log-file=none`.

## Commands

1. **Display the log file path** — run `overlord log-path` first to confirm where the log lives.
2. **Rename the old log file** — e.g., `mv "$(overlord log-path)" "$(overlord log-path).1"`.
3. **Rotate** — run `overlord rotate-log` so the daemon reopens a fresh file at the original path.

## Important

- If the daemon was started with `--log-file=none`, no file handler exists and `rotate-log` will return an error.
- Always check the log path with `overlord log-path` before rotating.
