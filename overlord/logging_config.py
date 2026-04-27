"""Structured logging configuration for Overlord.

Provides JSON-formatted log output suitable for machine consumption while
remaining human-readable.  Call ``setup_logging()`` once at process start.
"""

import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created).astimezone().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging(level: int = logging.INFO, log_file: str | None = None) -> None:
    """Configure the ``overlord`` logger hierarchy with JSON output.

    Always adds a stderr handler.  When *log_file* is given, a
    :class:`logging.FileHandler` is added as well so the daemon can
    write to a persistent log file that supports rotation via
    :func:`rotate_log_handler`.
    """
    root = logging.getLogger("overlord")
    if root.handlers:
        return  # already configured

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(JSONFormatter())
    root.addHandler(stderr_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JSONFormatter())
        root.addHandler(file_handler)

    root.setLevel(level)


def rotate_log_handler() -> str:
    """Close and reopen the file-based log handler.

    This allows external tools to rename the current log file and then
    call this function so the daemon starts writing to a fresh file at
    the original path.

    Returns a status message describing what happened.
    """
    root = logging.getLogger("overlord")
    rotated = False
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler):
            old_path = handler.baseFilename
            handler.close()
            handler.stream = handler._open()
            rotated = True
            root.info("Log file rotated: %s", old_path)
    if rotated:
        return "log file rotated"
    return "no file handler configured, nothing to rotate"
