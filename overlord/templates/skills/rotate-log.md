---
description: Rotate the overlord daemon log file
---

Rotate the overlord daemon log file.

This closes and reopens the daemon's log file handler, allowing external tools (like `logrotate`) to
rename the old log file beforehand. The daemon will start writing to a fresh file at the original path.

## Log file location

By default the daemon writes logs to `<data-dir>/overlord.log` (typically `~/.local/share/overlord/overlord.log`).
This can be overridden with `--log-file PATH` or disabled entirely with `--log-file=none`.

## Command

```bash
overlord rotate-log
```

## Important

- If the daemon was started with `--log-file=none`, no file handler exists and the command will return an error.
- Typically you rename the current log file first, then run this command so the daemon opens a new file at the original path.
