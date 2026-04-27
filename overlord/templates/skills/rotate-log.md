---
description: Rotate the overlord daemon log file
---

Rotate the overlord daemon log file.

This closes and reopens the daemon's log file handler, allowing external tools (like `logrotate`) to
rename the old log file beforehand. The daemon will start writing to a fresh file at the original path.

## Prerequisites

- The daemon must be running with `--log-file PATH` to have a file handler to rotate.

## Command

```bash
overlord rotate-log
```

## Important

- If the daemon was started without `--log-file`, the command will return an error.
- Typically you rename the current log file first, then run this command so the daemon opens a new file at the original path.
