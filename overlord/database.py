"""SQLite database layer for the Overlord repeatable tasks manager."""

import json
import os
import sqlite3
import threading
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

SCHEMA_VERSION = 5

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
    timeout_seconds INTEGER,
    max_retries INTEGER NOT NULL DEFAULT 0,
    retry_delay_seconds INTEGER NOT NULL DEFAULT 0,
    consumes TEXT NOT NULL DEFAULT '[]',
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
    consumer TEXT,
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
CREATE INDEX IF NOT EXISTS idx_messages_consumer ON messages(consumer);
"""

# Migrations from one schema version to the next.
MIGRATIONS = {
    2: [
        "ALTER TABLE jobs ADD COLUMN timeout_seconds INTEGER",
        "ALTER TABLE jobs ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE jobs ADD COLUMN retry_delay_seconds INTEGER NOT NULL DEFAULT 0",
    ],
    3: [
        "ALTER TABLE messages ADD COLUMN consumers TEXT NOT NULL DEFAULT '[]'",
    ],
    4: [
        "CREATE INDEX IF NOT EXISTS idx_messages_consumers ON messages(consumers)",
    ],
    5: [
        # Replace consumers JSON array with single nullable consumer column.
        "ALTER TABLE messages ADD COLUMN consumer TEXT",
        # Migrate existing data: pick first element from the JSON array.
        "UPDATE messages SET consumer = json_extract(consumers, '$[0]') "
        "WHERE consumers != '[]' AND consumers IS NOT NULL",
        # Add consumes column to jobs.
        "ALTER TABLE jobs ADD COLUMN consumes TEXT NOT NULL DEFAULT '[]'",
        # New index for consumer lookups.
        "CREATE INDEX IF NOT EXISTS idx_messages_consumer ON messages(consumer)",
    ],
}


class SchemaVersionError(Exception):
    """Raised when the database schema version doesn't match the expected version."""


class Database:
    """SQLite database for Overlord job management.

    Supports thread-local connections for safe concurrent access from the
    scheduler's executor threads.
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._all_conns: list[sqlite3.Connection] = []
        self._all_conns_lock = threading.Lock()

    def _make_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        """Return a thread-local database connection.

        If the thread's cached connection was closed (e.g. by close_all()),
        it is discarded and a fresh connection is created transparently.
        """
        c = getattr(self._local, "conn", None)
        if c is not None:
            try:
                c.execute("SELECT 1")
            except sqlite3.ProgrammingError:
                # Connection was closed (e.g. by close_all()); discard it.
                self._local.conn = None
                c = None
        if c is None:
            c = self._make_connection()
            self._local.conn = c
            with self._all_conns_lock:
                self._all_conns.append(c)
        return c

    def init_schema(self) -> None:
        """Create tables if they don't exist and record schema version.

        If the DB already has a schema version, run any pending migrations
        to bring it up to SCHEMA_VERSION.
        """
        self.conn.executescript(SCHEMA_SQL)
        row = self.conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        current = row[0]
        if current is None:
            self.conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
            )
            self.conn.commit()
        elif current < SCHEMA_VERSION:
            self._migrate(current)

    def _migrate(self, from_version: int) -> None:
        """Apply sequential migrations from from_version+1 to SCHEMA_VERSION."""
        for version in range(from_version + 1, SCHEMA_VERSION + 1):
            stmts = MIGRATIONS.get(version)
            if stmts is None:
                raise SchemaVersionError(
                    f"No migration defined for version {version}"
                )
            for stmt in stmts:
                self.conn.execute(stmt)
            self.conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (version,)
            )
        self.conn.commit()

    def check_schema_version(self) -> None:
        """Validate that the DB schema version matches SCHEMA_VERSION.

        Raises SchemaVersionError if the version is wrong or missing.
        Should be called at scheduler startup.
        """
        try:
            row = self.conn.execute(
                "SELECT MAX(version) FROM schema_version"
            ).fetchone()
        except sqlite3.OperationalError:
            raise SchemaVersionError("schema_version table does not exist")
        current = row[0] if row else None
        if current != SCHEMA_VERSION:
            raise SchemaVersionError(
                f"Schema version mismatch: DB has v{current}, expected v{SCHEMA_VERSION}"
            )

    def close(self) -> None:
        """Close the calling thread's connection."""
        c = getattr(self._local, "conn", None)
        if c is not None:
            c.close()
            with self._all_conns_lock:
                try:
                    self._all_conns.remove(c)
                except ValueError:
                    pass
            self._local.conn = None

    def close_all(self) -> None:
        """Close all tracked connections. Call only during final shutdown.

        Note: this does not reset each thread's ``_local.conn`` reference.
        The ``conn`` property detects stale (closed) connections and
        transparently replaces them, so post-shutdown access is safe but
        should be avoided in normal operation.
        """
        with self._all_conns_lock:
            for c in self._all_conns:
                try:
                    c.close()
                except Exception:
                    pass
            self._all_conns.clear()

    # -- Job CRUD --

    def create_job(self, job: Job) -> Job:
        consumes_json = json.dumps(job.consumes)
        cur = self.conn.execute(
            "INSERT INTO jobs (name, cron_expression, command, status, exclusive_lock, "
            "timeout_seconds, max_retries, retry_delay_seconds, consumes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (job.name, job.cron_expression, job.command, job.status.value,
             job.exclusive_lock, job.timeout_seconds, job.max_retries,
             job.retry_delay_seconds, consumes_json),
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
        consumes_json = json.dumps(job.consumes)
        self.conn.execute(
            "UPDATE jobs SET name=?, cron_expression=?, command=?, status=?, "
            "exclusive_lock=?, timeout_seconds=?, max_retries=?, "
            "retry_delay_seconds=?, consumes=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (job.name, job.cron_expression, job.command, job.status.value,
             job.exclusive_lock, job.timeout_seconds, job.max_retries,
             job.retry_delay_seconds, consumes_json, job.id),
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

    def get_execution(self, execution_id: int) -> Optional[ExecutionRecord]:
        row = self.conn.execute(
            "SELECT * FROM execution_history WHERE id = ?", (execution_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_execution(row)

    def get_execution_history(self, job_id: int, limit: int = 10) -> list[ExecutionRecord]:
        rows = self.conn.execute(
            "SELECT * FROM execution_history WHERE job_id = ? "
            "ORDER BY id DESC LIMIT ?",
            (job_id, limit),
        ).fetchall()
        return [self._row_to_execution(r) for r in rows]

    # -- Messages --

    def create_message(
        self, source_job_id: int, payload: str,
        consumer: Optional[str] = None,
    ) -> Message:
        cur = self.conn.execute(
            "INSERT INTO messages (source_job_id, payload, consumer) VALUES (?, ?, ?)",
            (source_job_id, payload, consumer),
        )
        self.conn.commit()
        return Message(
            id=cur.lastrowid, source_job_id=source_job_id,
            payload=payload, consumer=consumer,
        )

    def poll_messages(self, limit: int = 10) -> list[Message]:
        rows = self.conn.execute(
            "SELECT * FROM messages WHERE consumed = 0 "
            "ORDER BY created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_message(r) for r in rows]

    def mark_consumed(self, message_id: int) -> None:
        self.conn.execute(
            "UPDATE messages SET consumed = 1, consumed_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (message_id,),
        )
        self.conn.commit()

    def mark_consumed_bulk(self, message_ids: list[int]) -> None:
        """Mark multiple messages as consumed in a single transaction."""
        if not message_ids:
            return
        placeholders = ",".join("?" for _ in message_ids)
        self.conn.execute(
            f"UPDATE messages SET consumed = 1, consumed_at = CURRENT_TIMESTAMP "
            f"WHERE id IN ({placeholders})",
            message_ids,
        )
        self.conn.commit()

    def get_messages_by_job(self, source_job_id: int, limit: int = 10) -> list[Message]:
        """Return recent messages for a given job, newest first."""
        rows = self.conn.execute(
            "SELECT * FROM messages WHERE source_job_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (source_job_id, limit),
        ).fetchall()
        return [self._row_to_message(r) for r in rows]

    def query_messages(
        self,
        source_job_name: Optional[str] = None,
        consumer: Optional[str] = None,
        consumed: Optional[bool] = None,
        limit: int = 20,
    ) -> list[Message]:
        """Query messages with optional filters.

        Parameters
        ----------
        source_job_name : str, optional
            Filter by originating job name.
        consumer : str, optional
            Filter by consumer tag (exact match on the consumer column).
        consumed : bool, optional
            Filter consumed (True) vs unconsumed (False) messages.
        limit : int
            Maximum number of results (default 20).
        """
        clauses: list[str] = []
        params: list = []

        if source_job_name is not None:
            clauses.append(
                "source_job_id = (SELECT id FROM jobs WHERE name = ?)"
            )
            params.append(source_job_name)

        if consumer is not None:
            clauses.append("consumer = ?")
            params.append(consumer)

        if consumed is not None:
            clauses.append("consumed = ?")
            params.append(1 if consumed else 0)

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM messages{where} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_message(r) for r in rows]

    def fetch_unconsumed_for_consumers(
        self, consumer_names: list[str], limit: int = 50,
    ) -> list[Message]:
        """Fetch unconsumed messages matching consumer names.

        If consumer_names contains ``"*"``, matches messages where
        ``consumer IS NULL`` (unaddressed messages).  Otherwise matches
        messages whose ``consumer`` column equals one of the given names.
        """
        if not consumer_names:
            return []

        if "*" in consumer_names:
            rows = self.conn.execute(
                "SELECT * FROM messages WHERE consumed = 0 AND consumer IS NULL "
                "ORDER BY created_at ASC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            placeholders = ",".join("?" for _ in consumer_names)
            rows = self.conn.execute(
                f"SELECT * FROM messages WHERE consumed = 0 "
                f"AND consumer IN ({placeholders}) "
                "ORDER BY created_at ASC LIMIT ?",
                (*consumer_names, limit),
            ).fetchall()

        return [self._row_to_message(r) for r in rows]

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
        consumes_raw = row["consumes"]
        consumes = json.loads(consumes_raw) if consumes_raw else []
        return Job(
            id=row["id"],
            name=row["name"],
            cron_expression=row["cron_expression"],
            command=row["command"],
            status=JobStatus(row["status"]),
            exclusive_lock=row["exclusive_lock"],
            timeout_seconds=row["timeout_seconds"],
            max_retries=row["max_retries"],
            retry_delay_seconds=row["retry_delay_seconds"],
            consumes=consumes,
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
            consumer=row["consumer"],
            created_at=row["created_at"],
            consumed=bool(row["consumed"]),
            consumed_at=row["consumed_at"],
        )
