"""JSON file-based job storage for Overlord.

Each job is persisted as a separate JSON file under ``<data_dir>/jobs/<name>.json``.
Concurrent access is protected by ``flock(2)`` and writes are atomic
(write to a ``.tmp`` sibling, then ``os.replace``).
"""

import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .models import Job, JobStatus

DEFAULT_DATA_DIR = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord"


def _jobs_dir(data_dir: Path) -> Path:
    return data_dir / "jobs"


def _job_path(jobs_dir: Path, name: str) -> Path:
    return jobs_dir / f"{name}.json"


def _job_to_dict(job: Job) -> dict:
    """Serialize a Job to a JSON-compatible dict."""
    return {
        "name": job.name,
        "cron_expression": job.cron_expression,
        "command": job.command,
        "status": job.status.value,
        "exclusive_lock": job.exclusive_lock,
        "timeout_seconds": job.timeout_seconds,
        "max_retries": job.max_retries,
        "retry_delay_seconds": job.retry_delay_seconds,
        "consumes": job.consumes,
        "queue_name": job.queue_name,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }


def _dict_to_job(data: dict) -> Job:
    """Deserialize a dict (from JSON) into a Job."""
    return Job(
        name=data["name"],
        cron_expression=data["cron_expression"],
        command=data["command"],
        status=JobStatus(data["status"]),
        exclusive_lock=data.get("exclusive_lock"),
        timeout_seconds=data.get("timeout_seconds"),
        max_retries=data.get("max_retries", 0),
        retry_delay_seconds=data.get("retry_delay_seconds", 0),
        consumes=data.get("consumes", []),
        queue_name=data.get("queue_name", "default"),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
    )


@contextmanager
def _flock_path(path: Path, *, shared: bool = False):
    """Acquire an flock on *path*, creating it if necessary.

    Uses a separate ``.lock`` file so that readers and writers never
    truncate or replace the file they are locking on.
    """
    lock_path = path.with_suffix(path.suffix + ".lock")
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class JobStore:
    """File-based job store backed by one JSON file per job.

    Parameters
    ----------
    data_dir : Path, optional
        Root data directory.  Jobs are stored under ``<data_dir>/jobs/``.
        Defaults to ``$XDG_DATA_HOME/overlord`` (or ``~/.local/share/overlord``).
    """

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or DEFAULT_DATA_DIR
        self._jobs_dir = _jobs_dir(self.data_dir)
        self._jobs_dir.mkdir(parents=True, exist_ok=True)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _read_job(self, path: Path) -> Optional[Job]:
        """Read and parse a single job file (caller must hold lock)."""
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return None
        return _dict_to_job(data)

    def _write_job(self, job: Job) -> None:
        """Atomically write a job to its JSON file (caller must hold lock)."""
        target = _job_path(self._jobs_dir, job.name)
        data = _job_to_dict(job)
        content = json.dumps(data, indent=2, default=str) + "\n"

        # Write to a temp file in the same directory, then atomically replace.
        fd, tmp_path = tempfile.mkstemp(
            dir=str(self._jobs_dir), suffix=".tmp", prefix=f".{job.name}_"
        )
        try:
            os.write(fd, content.encode("utf-8"))
            os.fsync(fd)
            os.close(fd)
            fd = -1  # mark as closed
            os.replace(tmp_path, str(target))
        except BaseException:
            if fd >= 0:
                os.close(fd)
            # Best-effort cleanup of the temp file.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # -- Public API --

    def create_job(self, job: Job) -> Job:
        """Persist a new job. Raises ``FileExistsError`` if the name is taken."""
        path = _job_path(self._jobs_dir, job.name)
        with _flock_path(path):
            if path.exists():
                raise FileExistsError(f"Job '{job.name}' already exists")
            now = self._now_iso()
            job.created_at = now
            job.updated_at = now
            self._write_job(job)
        return job

    def get_job_by_name(self, name: str) -> Optional[Job]:
        """Return the job with the given name, or ``None``."""
        path = _job_path(self._jobs_dir, name)
        with _flock_path(path, shared=True):
            return self._read_job(path)

    def list_jobs(self, status: Optional[JobStatus] = None) -> list[Job]:
        """Return all jobs, optionally filtered by status."""
        jobs: list[Job] = []
        for entry in sorted(self._jobs_dir.iterdir()):
            if not entry.name.endswith(".json"):
                continue
            with _flock_path(entry, shared=True):
                job = self._read_job(entry)
            if job is None:
                continue
            if status is not None and job.status != status:
                continue
            jobs.append(job)
        return jobs

    def update_job(self, job: Job) -> None:
        """Update an existing job. Raises ``FileNotFoundError`` if missing."""
        path = _job_path(self._jobs_dir, job.name)
        with _flock_path(path):
            if not path.exists():
                raise FileNotFoundError(f"Job '{job.name}' does not exist")
            job.updated_at = self._now_iso()
            self._write_job(job)

    def delete_job(self, name: str) -> None:
        """Remove a job by name. Raises ``FileNotFoundError`` if missing."""
        path = _job_path(self._jobs_dir, name)
        with _flock_path(path):
            if not path.exists():
                raise FileNotFoundError(f"Job '{name}' does not exist")
            path.unlink()
        # Clean up the lock file (best-effort).
        lock_path = path.with_suffix(path.suffix + ".lock")
        try:
            lock_path.unlink()
        except OSError:
            pass
