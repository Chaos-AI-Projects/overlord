"""File-based named lock store for Overlord.

Each lock is a file under ``<data_dir>/locks/<lock_name>`` containing the
holder execution ID.  Atomic creation (``O_CREAT | O_EXCL``) ensures
mutual exclusion without SQLite.
"""

import os
from pathlib import Path
from typing import Optional

from .models import Lock

DEFAULT_DATA_DIR = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord"


class LockStore:
    """File-based named lock store.

    Parameters
    ----------
    data_dir : Path, optional
        Root data directory.  Locks are stored under ``<data_dir>/locks/``.
        Defaults to ``$XDG_DATA_HOME/overlord``.
    """

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or DEFAULT_DATA_DIR
        self._locks_dir = self.data_dir / "locks"
        self._locks_dir.mkdir(parents=True, exist_ok=True)

    def _lock_path(self, lock_name: str) -> Path:
        return self._locks_dir / lock_name

    def acquire_lock(self, lock_name: str, execution_id: int) -> bool:
        """Try to acquire a named lock.  Returns True if acquired."""
        path = self._lock_path(lock_name)
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            try:
                os.write(fd, str(execution_id).encode())
                os.fsync(fd)
            finally:
                os.close(fd)
            return True
        except FileExistsError:
            return False

    def release_lock(self, lock_name: str) -> None:
        """Release a named lock by removing its file."""
        path = self._lock_path(lock_name)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def get_lock(self, lock_name: str) -> Optional[Lock]:
        """Return lock info, or None if not held."""
        path = self._lock_path(lock_name)
        try:
            content = path.read_text().strip()
            execution_id = int(content)
            return Lock(lock_name=lock_name, holder_execution_id=execution_id)
        except (FileNotFoundError, ValueError):
            return None

    def release_stale_locks(self, execution_log) -> int:
        """Release locks held by executions that are no longer running.

        Parameters
        ----------
        execution_log : ExecutionLog
            Used to check whether the holder execution is still running.

        Returns the number of stale locks released.
        """
        from .models import ExecutionStatus

        count = 0
        for entry in self._locks_dir.iterdir():
            if entry.name.startswith("."):
                continue
            try:
                execution_id = int(entry.read_text().strip())
            except (ValueError, FileNotFoundError):
                entry.unlink(missing_ok=True)
                count += 1
                continue
            rec = execution_log.get_execution(execution_id)
            if rec is None or rec.status != ExecutionStatus.RUNNING:
                entry.unlink(missing_ok=True)
                count += 1
        return count
