"""SQLite database layer for the Overlord repeatable tasks manager."""

import os
import sqlite3
from pathlib import Path
from typing import Optional

from .models import (
    ExecutionRecord,
    ExecutionStatus,
    Job,
    JobStatus,
    Lock,
    Message,
)

DEFAULT_DB_PATH = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord" / "overlord.db"

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    cron_expression TEXT NOT NULL,
    command TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'enabled',
    exclusive_lock TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS execution_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    exit_code INTEGER,
    stdout TEXT,
    stderr TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_job_id INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    consumed INTEGER NOT NULL DEFAULT 0,
    consumed_at TIMESTAMP,
    FOREIGN KEY (source_job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS locks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lock_name TEXT NOT NULL UNIQUE,
    holder_execution_id INTEGER NOT NULL,
    acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (holder_execution_id) REFERENCES execution_history(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_execution_history_job_id ON execution_history(job_id);
CREATE INDEX IF NOT EXISTS idx_execution_history_status ON execution_history(status);
CREATE INDEX IF NOT EXISTS idx_messages_consumed ON messages(consumed);
CREATE INDEX IF NOT EXISTS idx_messages_source_job_id ON messages(source_job_id);
"""


class Database:
    """SQLite database for Overlord job management."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            # check_same_thread=False: needed for multi-thread access (e.g. FastAPI).
            # Phase 2's scheduler with concurrent jobs will need proper thread
            # synchronization (connection pool or per-thread connections).
            self._conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def init_schema(self) -> None:
        """Create tables if they don't exist and record schema version."""
        self.conn.executescript(SCHEMA_SQL)
        row = self.conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        if row[0] is None:
            self.conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
            )
            self.conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # -- Job CRUD --

    def create_job(self, job: Job) -> Job:
        cur = self.conn.execute(
            "INSERT INTO jobs (name, cron_expression, command, status, exclusive_lock) "
            "VALUES (?, ?, ?, ?, ?)",
            (job.name, job.cron_expression, job.command, job.status.value, job.exclusive_lock),
        )
        self.conn.commit()
        job.id = cur.lastrowid
        return job

    def get_job(self, job_id: int) -> Optional[Job]:
        row = self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def get_job_by_name(self, name: str) -> Optional[Job]:
        row = self.conn.execute("SELECT * FROM jobs WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def list_jobs(self, status: Optional[JobStatus] = None) -> list[Job]:
        if status is not None:
            rows = self.conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY id", (status.value,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
        return [self._row_to_job(r) for r in rows]

    def update_job(self, job: Job) -> None:
        self.conn.execute(
            "UPDATE jobs SET name=?, cron_expression=?, command=?, status=?, "
            "exclusive_lock=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (job.name, job.cron_expression, job.command, job.status.value,
             job.exclusive_lock, job.id),
        )
        self.conn.commit()

    def delete_job(self, job_id: int) -> None:
        self.conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        self.conn.commit()

    # -- Execution History --

    def create_execution(self, job_id: int) -> ExecutionRecord:
        cur = self.conn.execute(
            "INSERT INTO execution_history (job_id, status) VALUES (?, ?)",
            (job_id, ExecutionStatus.RUNNING.value),
        )
        self.conn.commit()
        # Re-read the row to get the DB-generated UTC started_at, avoiding
        # local-time vs UTC divergence from using datetime.now().
        row = self.conn.execute(
            "SELECT * FROM execution_history WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        return self._row_to_execution(row)

    def finish_execution(
        self, execution_id: int, status: ExecutionStatus,
        exit_code: Optional[int] = None,
        stdout: Optional[str] = None, stderr: Optional[str] = None,
    ) -> None:
        self.conn.execute(
            "UPDATE execution_history SET status=?, finished_at=CURRENT_TIMESTAMP, "
            "exit_code=?, stdout=?, stderr=? WHERE id=?",
            (status.value, exit_code, stdout, stderr, execution_id),
        )
        self.conn.commit()

    def get_execution_history(self, job_id: int, limit: int = 10) -> list[ExecutionRecord]:
        rows = self.conn.execute(
            "SELECT * FROM execution_history WHERE job_id = ? "
            "ORDER BY started_at DESC LIMIT ?",
            (job_id, limit),
        ).fetchall()
        return [self._row_to_execution(r) for r in rows]

    # -- Messages --

    def create_message(self, source_job_id: int, payload: str) -> Message:
        cur = self.conn.execute(
            "INSERT INTO messages (source_job_id, payload) VALUES (?, ?)",
            (source_job_id, payload),
        )
        self.conn.commit()
        return Message(id=cur.lastrowid, source_job_id=source_job_id, payload=payload)

    def poll_messages(self, limit: int = 10) -> list[Message]:  # Phase 3: poll-based consumption
        rows = self.conn.execute(
            "SELECT * FROM messages WHERE consumed = 0 "
            "ORDER BY created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_message(r) for r in rows]

    def mark_consumed(self, message_id: int) -> None:  # Phase 3: poll-based consumption
        self.conn.execute(
            "UPDATE messages SET consumed = 1, consumed_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (message_id,),
        )
        self.conn.commit()

    # -- Locks --

    def acquire_lock(self, lock_name: str, execution_id: int) -> bool:
        """Try to acquire a named lock. Returns True if acquired, False if already held."""
        try:
            self.conn.execute(
                "INSERT INTO locks (lock_name, holder_execution_id) VALUES (?, ?)",
                (lock_name, execution_id),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def release_lock(self, lock_name: str) -> None:
        self.conn.execute("DELETE FROM locks WHERE lock_name = ?", (lock_name,))
        self.conn.commit()

    def get_lock(self, lock_name: str) -> Optional[Lock]:
        row = self.conn.execute(
            "SELECT * FROM locks WHERE lock_name = ?", (lock_name,)
        ).fetchone()
        if row is None:
            return None
        return Lock(
            id=row["id"],
            lock_name=row["lock_name"],
            holder_execution_id=row["holder_execution_id"],
            acquired_at=row["acquired_at"],
        )

    def release_stale_locks(self) -> int:
        """Release locks held by executions that are no longer running.
        Returns the number of stale locks released."""
        cur = self.conn.execute(
            "DELETE FROM locks WHERE holder_execution_id NOT IN "
            "(SELECT id FROM execution_history WHERE status = ?)",
            (ExecutionStatus.RUNNING.value,),
        )
        self.conn.commit()
        return cur.rowcount

    # -- Row converters --

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            name=row["name"],
            cron_expression=row["cron_expression"],
            command=row["command"],
            status=JobStatus(row["status"]),
            exclusive_lock=row["exclusive_lock"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_execution(row: sqlite3.Row) -> ExecutionRecord:
        return ExecutionRecord(
            id=row["id"],
            job_id=row["job_id"],
            status=ExecutionStatus(row["status"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            exit_code=row["exit_code"],
            stdout=row["stdout"],
            stderr=row["stderr"],
        )

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> Message:
        return Message(
            id=row["id"],
            source_job_id=row["source_job_id"],
            payload=row["payload"],
            created_at=row["created_at"],
            consumed=bool(row["consumed"]),
            consumed_at=row["consumed_at"],
        )
