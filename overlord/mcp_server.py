"""MCP server exposing Overlord job registry for agent-driven job management."""

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional

from .database import DEFAULT_DB_PATH, Database
from .executor import run_job
from .maildir import CATCHALL, MaildirStore
from .models import ExecutionStatus, Job, JobStatus
from .spool import SpoolWriter

logger = logging.getLogger(__name__)


def _job_to_dict(job: Job) -> dict:
    """Convert a Job dataclass to a JSON-serialisable dictionary."""
    return {
        "id": job.id,
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
        "created_at": str(job.created_at) if job.created_at else None,
        "updated_at": str(job.updated_at) if job.updated_at else None,
    }


def _execution_to_dict(rec, db: Optional["Database"] = None) -> dict:
    """Convert an ExecutionRecord to a JSON-serialisable dictionary.

    When *db* is provided, structured output is retrieved from the already-
    parsed message in the message hub rather than re-parsing stdout.
    """
    result = {
        "id": rec.id,
        "job_id": rec.job_id,
        "status": rec.status.value,
        "started_at": str(rec.started_at) if rec.started_at else None,
        "finished_at": str(rec.finished_at) if rec.finished_at else None,
        "exit_code": rec.exit_code,
        "stdout": rec.stdout,
        "stderr": rec.stderr,
    }
    if rec.status == ExecutionStatus.SUCCESS and rec.stdout and db is not None:
        # Retrieve structured output from the message that was already
        # produced during execution, avoiding a redundant parse of stdout.
        for msg in db.get_messages_by_job(rec.job_id, limit=5):
            try:
                payload = json.loads(msg.payload)
                if payload.get("execution_id") == rec.id and "message" in payload:
                    result["structured_output"] = {
                        "consumer": msg.consumer,
                        "message": payload["message"],
                    }
                    break
            except (json.JSONDecodeError, KeyError):
                pass
    return result


def create_mcp_server(
    db_path: Optional[Path] = None,
    db: Optional["Database"] = None,
    host: str = "127.0.0.1",
    port: int = 8000,
    cwd: Optional[Path] = None,
    spool: Optional[SpoolWriter] = None,
):
    """Create and return a FastMCP server wired to the given database.

    Parameters
    ----------
    db_path : Path, optional
        Path to the SQLite database.  Falls back to DEFAULT_DB_PATH.
        Ignored when *db* is provided.
    db : Database, optional
        An existing Database instance to reuse (e.g. shared with the
        scheduler).  When provided, the caller is responsible for
        schema initialisation and lifecycle management.
    host : str
        Bind address for the streamable-HTTP transport (default ``127.0.0.1``).
    port : int
        Port for the streamable-HTTP transport (default ``8000``).

    Returns
    -------
    FastMCP
        The configured MCP server instance, ready to be run.
    """
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("overlord-job-registry", host=host, port=port)
    if db is None:
        db = Database(db_path=db_path)
        db.init_schema()
    if spool is None:
        spool = SpoolWriter(data_dir=db.db_path.parent)
    maildir_store = MaildirStore(data_dir=db.db_path.parent)

    @mcp.tool()
    def register_job(
        name: str,
        cron_expression: str,
        command: str,
        exclusive_lock: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        max_retries: int = 0,
        retry_delay_seconds: int = 0,
        consumes: Optional[str] = None,
        queue_name: str = "default",
    ) -> str:
        """Register a new repeatable job.

        Parameters
        ----------
        name : str
            Unique job name.
        cron_expression : str
            5-field cron schedule (minute hour dom month dow).
        command : str
            Shell command to execute.
        exclusive_lock : str, optional
            Named lock to prevent concurrent runs.
        timeout_seconds : int, optional
            Maximum execution time in seconds.
        max_retries : int
            Number of retries on failure (default 0).
        retry_delay_seconds : int
            Delay between retries in seconds (default 0).
        consumes : str, optional
            Comma-separated consumer names this job listens to, or ``"*"``
            for catch-all.  Empty or omitted means the job runs
            unconditionally on its cron schedule.
        queue_name : str
            Execution queue name (default ``"default"``).  Only one job
            runs per queue at a time; others are skipped until the queue
            is free.

        Returns
        -------
        str
            JSON object with the created job details.
        """
        consumes_list: list[str] = []
        if consumes:
            consumes_list = [c.strip() for c in consumes.split(",") if c.strip()]

        job = Job(
            name=name,
            cron_expression=cron_expression,
            command=command,
            exclusive_lock=exclusive_lock,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            consumes=consumes_list,
            queue_name=queue_name,
        )
        try:
            created = db.create_job(job)
        except sqlite3.IntegrityError:
            return json.dumps({"error": f"Job '{name}' already exists"})
        logger.info("Registered job %r (id=%s)", created.name, created.id)
        return json.dumps(_job_to_dict(created))

    @mcp.tool()
    def unregister_job(name: str) -> str:
        """Remove a job by name.  This also deletes its execution history and messages.

        Parameters
        ----------
        name : str
            The unique name of the job to remove.

        Returns
        -------
        str
            JSON confirmation or error message.
        """
        job = db.get_job_by_name(name)
        if job is None:
            return json.dumps({"error": f"Job '{name}' not found"})
        db.delete_job(job.id)
        logger.info("Unregistered job %r (id=%s)", name, job.id)
        return json.dumps({"status": "deleted", "name": name, "id": job.id})

    @mcp.tool()
    def update_job(
        name: str,
        cron_expression: Optional[str] = None,
        command: Optional[str] = None,
        exclusive_lock: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        max_retries: Optional[int] = None,
        retry_delay_seconds: Optional[int] = None,
        consumes: Optional[str] = None,
        queue_name: Optional[str] = None,
    ) -> str:
        """Update an existing job's parameters.

        Only the provided fields are updated; omitted fields remain unchanged.

        Parameters
        ----------
        name : str
            The unique name of the job to update (required, used to look up the job).
        cron_expression : str, optional
            New 5-field cron schedule.
        command : str, optional
            New shell command to execute.
        exclusive_lock : str, optional
            New named lock. Pass empty string to clear.
        timeout_seconds : int, optional
            New timeout in seconds.
        max_retries : int, optional
            New max retries.
        retry_delay_seconds : int, optional
            New retry delay in seconds.
        consumes : str, optional
            New comma-separated consumer names, or ``"*"`` for catch-all.
            Pass empty string to clear.
        queue_name : str, optional
            New execution queue name.

        Returns
        -------
        str
            JSON object with the updated job details or an error message.
        """
        job = db.get_job_by_name(name)
        if job is None:
            return json.dumps({"error": f"Job '{name}' not found"})

        if cron_expression is not None:
            job.cron_expression = cron_expression
        if command is not None:
            job.command = command
        if exclusive_lock is not None:
            job.exclusive_lock = exclusive_lock if exclusive_lock else None
        if timeout_seconds is not None:
            job.timeout_seconds = timeout_seconds
        if max_retries is not None:
            job.max_retries = max_retries
        if retry_delay_seconds is not None:
            job.retry_delay_seconds = retry_delay_seconds
        if consumes is not None:
            job.consumes = [c.strip() for c in consumes.split(",") if c.strip()] if consumes else []
        if queue_name is not None:
            job.queue_name = queue_name

        db.update_job(job)
        logger.info("Updated job %r (id=%s)", name, job.id)
        return json.dumps(_job_to_dict(job))

    @mcp.tool()
    def list_jobs(status: Optional[str] = None) -> str:
        """List all registered jobs, optionally filtered by status.

        Parameters
        ----------
        status : str, optional
            Filter by job status: "enabled", "disabled", or "paused".

        Returns
        -------
        str
            JSON array of job objects.
        """
        filter_status = None
        if status is not None:
            try:
                filter_status = JobStatus(status)
            except ValueError:
                return json.dumps(
                    {"error": f"Invalid status '{status}'. Use: enabled, disabled, paused"}
                )
        jobs = db.list_jobs(status=filter_status)
        return json.dumps([_job_to_dict(j) for j in jobs])

    @mcp.tool()
    def get_job_status(name: str) -> str:
        """Get a job's details and recent execution history.

        Parameters
        ----------
        name : str
            The unique name of the job.

        Returns
        -------
        str
            JSON object with job details and last 5 executions.
        """
        job = db.get_job_by_name(name)
        if job is None:
            return json.dumps({"error": f"Job '{name}' not found"})
        executions = db.get_execution_history(job.id, limit=5)
        result = _job_to_dict(job)
        result["recent_executions"] = [_execution_to_dict(e, db=db) for e in executions]
        return json.dumps(result)

    @mcp.tool()
    def query_messages(
        source_job_name: Optional[str] = None,
        consumer: Optional[str] = None,
        unconsumed: Optional[bool] = None,
        no_consumer: bool = False,
        limit: int = 20,
    ) -> str:
        """Query messages from Maildir with optional filters.

        Parameters
        ----------
        source_job_name : str, optional
            Filter by originating job name (from X-Overlord-Job header).
        consumer : str, optional
            Filter by consumer tag (reads from that consumer's Maildir).
        unconsumed : bool, optional
            If true, return only unconsumed messages. If false, only consumed
            (from the processed subfolder).
        no_consumer : bool
            If true, return only messages from the catchall mailbox.
        limit : int
            Maximum number of results (default 20).

        Returns
        -------
        str
            JSON array of message objects.
        """
        # Determine which mailboxes to read
        if no_consumer:
            targets = [CATCHALL]
        elif consumer:
            targets = [consumer]
        else:
            targets = maildir_store.list_mailboxes()

        # Determine whether to read unconsumed, consumed, or both
        show_unconsumed = unconsumed is None or unconsumed is True
        show_consumed = unconsumed is None or unconsumed is False

        raw_messages: list[tuple[dict, bool]] = []
        for target in targets:
            if show_unconsumed:
                for m in maildir_store.fetch_messages(target):
                    raw_messages.append((m, False))
            if show_consumed:
                for m in maildir_store.fetch_processed_messages(target):
                    raw_messages.append((m, True))

        # Filter by source job name if specified
        if source_job_name:
            filtered = []
            for m, consumed_flag in raw_messages:
                subject = m.get("subject", "")
                # Subject format: "job_name timestamp"
                job_from_subject = subject.split(" ", 1)[0] if subject else ""
                if job_from_subject == source_job_name:
                    filtered.append((m, consumed_flag))
            raw_messages = filtered

        # Apply limit
        raw_messages = raw_messages[:limit]

        result = []
        for m, consumed_flag in raw_messages:
            payload = m["payload"]
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                pass
            subject = m.get("subject", "")
            job_name = subject.split(" ", 1)[0] if subject else "(unknown)"
            result.append({
                "id": m["key"],
                "source_job_id": None,
                "source_job_name": job_name,
                "payload": payload,
                "consumer": m.get("consumer"),
                "consumed": consumed_flag,
                "created_at": m.get("date"),
                "consumed_at": None,
            })
        return json.dumps(result)

    @mcp.tool()
    def consume_messages(
        source_job_name: Optional[str] = None,
        consumer: Optional[str] = None,
        no_consumer: bool = False,
        limit: int = 20,
    ) -> str:
        """Consume matching unconsumed messages from Maildir.

        Fetches unconsumed messages, moves them to the processed subfolder,
        and returns the consumed messages.

        Parameters
        ----------
        source_job_name : str, optional
            Filter by originating job name (from X-Overlord-Job header).
        consumer : str, optional
            Filter by consumer tag (reads from that consumer's Maildir).
        no_consumer : bool
            If true, match only messages from the catchall mailbox.
        limit : int
            Maximum number of messages to consume (default 20).

        Returns
        -------
        str
            JSON array of the consumed message objects.
        """
        if no_consumer:
            targets = [CATCHALL]
        elif consumer:
            targets = [consumer]
        else:
            targets = maildir_store.list_mailboxes()

        raw_messages: list[tuple[dict, str]] = []
        for target in targets:
            for m in maildir_store.fetch_messages(target):
                raw_messages.append((m, target))

        # Filter by source job name if specified
        if source_job_name:
            filtered = []
            for m, target in raw_messages:
                subject = m.get("subject", "")
                job_from_subject = subject.split(" ", 1)[0] if subject else ""
                if job_from_subject == source_job_name:
                    filtered.append((m, target))
            raw_messages = filtered

        # Apply limit
        raw_messages = raw_messages[:limit]

        # Consume (move to processed) and build result
        result = []
        for m, target in raw_messages:
            maildir_store.consume(target, m["key"])
            payload = m["payload"]
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                pass
            subject = m.get("subject", "")
            job_name = subject.split(" ", 1)[0] if subject else "(unknown)"
            result.append({
                "id": m["key"],
                "source_job_id": None,
                "source_job_name": job_name,
                "payload": payload,
                "consumer": m.get("consumer"),
                "consumed": True,
                "created_at": m.get("date"),
                "consumed_at": None,
            })
        logger.info("Consumed %d messages", len(result))
        return json.dumps(result)

    @mcp.tool()
    def send_message(
        payload: str,
        consumer: Optional[str] = None,
    ) -> str:
        """Send a message via the file-based spool for delivery to Maildir.

        Parameters
        ----------
        payload : str
            Message payload (string or JSON).
        consumer : str, optional
            Consumer name the message is addressed to.  Omit for
            unaddressed messages picked up by catch-all jobs.

        Returns
        -------
        str
            JSON object confirming the spooled message.
        """
        email_msg = MaildirStore.build_message(
            payload=payload,
            consumer=consumer,
            job_name="cli",
        )
        spool_path = spool.write(email_msg)
        logger.info("Message spooled file=%s consumer=%r", spool_path.name, consumer)
        return json.dumps({
            "id": spool_path.stem,
            "source_job_id": None,
            "payload": payload,
            "consumer": consumer,
            "consumed": False,
            "created_at": None,
        })

    @mcp.tool()
    async def trigger_job(name: str) -> str:
        """Manually trigger a job to run immediately, ignoring its cron schedule.

        The job runs asynchronously in the background.  Use ``get_job_status``
        to check the result once it completes.

        Parameters
        ----------
        name : str
            The unique name of the job to trigger.

        Returns
        -------
        str
            JSON object with the execution id or an error message.
        """
        job = db.get_job_by_name(name)
        if job is None:
            return json.dumps({"error": f"Job '{name}' not found"})

        async def _run() -> None:
            try:
                await run_job(job, db, spool, cwd=cwd)
            except Exception:
                logger.exception("trigger_job: background execution failed for %r", name)

        task = asyncio.create_task(_run(), name=f"trigger-{name}")
        # Give the task a moment so the execution record is created.
        await asyncio.sleep(0.05)
        logger.info("Manually triggered job %r", name)
        return json.dumps({"status": "triggered", "job": _job_to_dict(job)})

    return mcp
