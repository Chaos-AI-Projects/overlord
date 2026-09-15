"""JSON-lines file-based execution history for Overlord.

Execution records are stored as append-only JSON lines in
``<data_dir>/execution.log``.  Each line is a self-contained JSON object
representing either the start or completion of a job execution.

A monotonically increasing execution ID is maintained in a separate
counter file (``<data_dir>/execution_id``).  File locking via ``flock(2)``
ensures safe concurrent access to the counter.  The log file itself
relies on the POSIX guarantee that ``O_APPEND`` writes below
``PIPE_BUF`` (4 096 bytes) are atomic.
"""

import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .models import ExecutionRecord, ExecutionStatus

DEFAULT_DATA_DIR = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord"


_REVERSE_CHUNK_BYTES = 64 * 1024


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_entry(line: bytes) -> Optional[dict]:
    """Decode one log line, returning ``None`` if it is blank or torn."""
    line = line.strip()
    if not line:
        return None
    try:
        entry = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    # A torn fragment can still be valid JSON -- a run of digits parses as
    # an int -- and only a dict has an "id" to match on.
    return entry if isinstance(entry, dict) else None


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _record_to_dict(rec: ExecutionRecord) -> dict:
    return {
        "id": rec.id,
        "job_name": rec.job_name,
        "status": rec.status.value,
        "started_at": rec.started_at if isinstance(rec.started_at, str) else rec.started_at.isoformat(),
        "finished_at": rec.finished_at if rec.finished_at is None or isinstance(rec.finished_at, str) else rec.finished_at.isoformat(),
        "exit_code": rec.exit_code,
        "stdout": rec.stdout,
        "stderr": rec.stderr,
    }


def _dict_to_record(data: dict) -> ExecutionRecord:
    return ExecutionRecord(
        id=data["id"],
        job_id=0,
        job_name=data.get("job_name"),
        status=ExecutionStatus(data["status"]),
        started_at=data["started_at"],
        finished_at=data.get("finished_at"),
        exit_code=data.get("exit_code"),
        stdout=data.get("stdout"),
        stderr=data.get("stderr"),
    )


@contextmanager
def _flock_path(path: Path, *, shared: bool = False):
    """Acquire an flock on a ``.lock`` sibling of *path*."""
    lock_path = path.with_suffix(path.suffix + ".lock")
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class ExecutionLog:
    """Append-only JSON-lines execution history.

    Parameters
    ----------
    data_dir : Path, optional
        Root data directory.  The log file is stored at
        ``<data_dir>/execution.log`` and the ID counter at
        ``<data_dir>/execution_id``.
        Defaults to ``$XDG_DATA_HOME/overlord``.
    """

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or DEFAULT_DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self.data_dir / "execution.log"
        self._counter_path = self.data_dir / "execution_id"

    def _next_id(self) -> int:
        """Atomically increment and return the next execution ID.

        The counter file holds a single integer.  Access is serialised
        via flock on the counter file itself.
        """
        with _flock_path(self._counter_path):
            try:
                current = int(self._counter_path.read_text().strip())
            except (FileNotFoundError, ValueError):
                current = 0
            next_val = current + 1
            self._counter_path.write_text(str(next_val))
            return next_val

    def _append(self, record: ExecutionRecord) -> None:
        """Append a JSON line for *record* to the log file.

        On Linux, ``open("a")`` sets ``O_APPEND`` and writes smaller than
        ``PIPE_BUF`` (4 096 bytes) are atomic — no flock needed.
        """
        line = json.dumps(_record_to_dict(record), separators=(",", ":")) + "\n"
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def _find_latest_entry(self, execution_id: int) -> Optional[dict]:
        """Return the newest log entry for *execution_id*, or ``None``.

        Scans the file backwards in fixed-size chunks and stops at the
        first match.  The log is append-only and the last line for an ID
        is authoritative, so the first match found going backwards is the
        answer, and everything before it can go unread.

        A full forward parse gives the same result, but reads and decodes
        the entire log: 2.94 s and 1.7 GB of peak memory on the live
        370 MB file, paid on every job finish.
        """
        try:
            handle = self._log_path.open("rb")
        except FileNotFoundError:
            return None

        with handle:
            handle.seek(0, os.SEEK_END)
            pos = handle.tell()
            # Bytes of the chunk boundary's leading partial line, carried
            # into the next (earlier) chunk so the line can be rejoined.
            carry = b""

            while pos > 0:
                size = min(_REVERSE_CHUNK_BYTES, pos)
                pos -= size
                handle.seek(pos)
                lines = (handle.read(size) + carry).split(b"\n")
                carry = lines.pop(0)
                for line in reversed(lines):
                    entry = _parse_entry(line)
                    if entry is not None and entry.get("id") == execution_id:
                        return entry

            entry = _parse_entry(carry)
            if entry is not None and entry.get("id") == execution_id:
                return entry

        return None

    def _read_lines(self) -> list[dict]:
        """Read all JSON lines from the log file.

        No flock needed: each line is atomically appended, and the
        ``JSONDecodeError`` guard below safely skips any partial trailing
        line observed during a concurrent write.
        """
        try:
            text = self._log_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        entries = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return entries

    def create_execution(self, job_name: str) -> ExecutionRecord:
        """Record the start of a new execution. Returns the new record."""
        exec_id = self._next_id()
        now = _now_iso()
        record = ExecutionRecord(
            id=exec_id,
            job_id=0,
            job_name=job_name,
            status=ExecutionStatus.RUNNING,
            started_at=now,
        )
        self._append(record)
        return record

    def finish_execution(
        self,
        execution_id: int,
        status: ExecutionStatus,
        exit_code: Optional[int] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
    ) -> None:
        """Record the completion of an execution.

        Appends a new log line with the final state.  The most recent
        line for a given execution ID is authoritative.
        """
        # Read back the start record to preserve started_at and job_name.
        start = self.get_execution(execution_id)
        started_at = start.started_at if start else _now_iso()
        job_name = start.job_name if start else None

        record = ExecutionRecord(
            id=execution_id,
            job_id=0,
            job_name=job_name,
            status=status,
            started_at=started_at,
            finished_at=_now_iso(),
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )
        self._append(record)

    def get_execution(self, execution_id: int) -> Optional[ExecutionRecord]:
        """Return the most recent state for the given execution ID."""
        entry = self._find_latest_entry(execution_id)
        return _dict_to_record(entry) if entry is not None else None

    def get_execution_history(
        self, job_name: str, limit: int = 10
    ) -> list[ExecutionRecord]:
        """Return recent executions for a job, newest first.

        Only the latest log line per execution ID is returned (i.e. the
        completion record takes precedence over the start record).
        """
        entries = self._read_lines()

        # Build a map of execution_id -> latest entry, filtered by job_name.
        latest: dict[int, dict] = {}
        for entry in entries:
            if entry.get("job_name") == job_name:
                latest[entry["id"]] = entry

        # Sort by ID descending and apply limit.
        sorted_entries = sorted(latest.values(), key=lambda e: e["id"], reverse=True)
        return [_dict_to_record(e) for e in sorted_entries[:limit]]

    def fail_running_executions(self) -> int:
        """Mark all RUNNING executions as FAILED.

        Called on startup to clean up executions left running by a
        previous unclean shutdown.  Returns the number of affected
        executions.
        """
        entries = self._read_lines()

        # Find execution IDs whose latest state is RUNNING.
        latest: dict[int, dict] = {}
        for entry in entries:
            latest[entry["id"]] = entry

        count = 0
        for entry in latest.values():
            if entry.get("status") == ExecutionStatus.RUNNING.value:
                self.finish_execution(
                    entry["id"],
                    ExecutionStatus.FAILED,
                    stderr="Marked failed: scheduler restarted while execution was in progress",
                )
                count += 1
        return count
