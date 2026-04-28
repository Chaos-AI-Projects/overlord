"""MCP server exposing Overlord job registry for agent-driven job management."""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

from .execution_log import ExecutionLog
from .executor import run_job
from .job_store import JobStore
from .lock_store import LockStore
from .maildir import CATCHALL, MaildirStore
from .models import ExecutionStatus, Job, JobStatus
from .spool import SpoolWriter

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord"


def _job_to_dict(job: Job) -> dict:
    """Convert a Job dataclass to a JSON-serialisable dictionary."""
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
        "created_at": str(job.created_at) if job.created_at else None,
        "updated_at": str(job.updated_at) if job.updated_at else None,
    }


def _execution_to_dict(rec) -> dict:
    """Convert an ExecutionRecord to a JSON-serialisable dictionary."""
    return {
        "id": rec.id,
        "job_name": rec.job_name,
        "status": rec.status.value,
        "started_at": str(rec.started_at) if rec.started_at else None,
        "finished_at": str(rec.finished_at) if rec.finished_at else None,
        "exit_code": rec.exit_code,
        "stdout": rec.stdout,
        "stderr": rec.stderr,
    }


def create_mcp_server(
    data_dir: Optional[Path] = None,
    job_store: Optional[JobStore] = None,
    execution_log: Optional[ExecutionLog] = None,
    lock_store: Optional[LockStore] = None,
    host: str = "127.0.0.1",
    port: int = 8000,
    cwd: Optional[Path] = None,
    spool: Optional[SpoolWriter] = None,
    shutdown_callback=None,
    rotate_log_callback=None,
    status_callback=None,
):
    """Create and return a FastMCP server wired to the given stores.

    Parameters
    ----------
    data_dir : Path, optional
        Root data directory.  Falls back to DEFAULT_DATA_DIR.
        Ignored when individual stores are provided.
    job_store : JobStore, optional
        An existing JobStore instance to reuse.
    execution_log : ExecutionLog, optional
        An existing ExecutionLog instance to reuse.
    lock_store : LockStore, optional
        An existing LockStore instance to reuse.
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
    _data_dir = data_dir or DEFAULT_DATA_DIR
    if job_store is None:
        job_store = JobStore(data_dir=_data_dir)
    if execution_log is None:
        execution_log = ExecutionLog(data_dir=_data_dir)
    if lock_store is None:
        lock_store = LockStore(data_dir=_data_dir)
    if spool is None:
        spool = SpoolWriter(data_dir=_data_dir)
    maildir_store = MaildirStore(data_dir=_data_dir)

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
            created = job_store.create_job(job)
        except FileExistsError:
            return json.dumps({"error": f"Job '{name}' already exists"})
        logger.info("Registered job %r", created.name)
        return json.dumps(_job_to_dict(created))

    @mcp.tool()
    def unregister_job(name: str) -> str:
        """Remove a job by name.

        Parameters
        ----------
        name : str
            The unique name of the job to remove.

        Returns
        -------
        str
            JSON confirmation or error message.
        """
        job = job_store.get_job_by_name(name)
        if job is None:
            return json.dumps({"error": f"Job '{name}' not found"})
        job_store.delete_job(name)
        logger.info("Unregistered job %r", name)
        return json.dumps({"status": "deleted", "name": name})

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
        job = job_store.get_job_by_name(name)
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

        job_store.update_job(job)
        logger.info("Updated job %r", name)
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
        jobs = job_store.list_jobs(status=filter_status)
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
        job = job_store.get_job_by_name(name)
        if job is None:
            return json.dumps({"error": f"Job '{name}' not found"})
        executions = execution_log.get_execution_history(job.name, limit=5)
        result = _job_to_dict(job)
        result["recent_executions"] = [_execution_to_dict(e) for e in executions]
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
        job = job_store.get_job_by_name(name)
        if job is None:
            return json.dumps({"error": f"Job '{name}' not found"})

        async def _run() -> None:
            try:
                await run_job(job, execution_log, lock_store, spool, cwd=cwd)
            except Exception:
                logger.exception("trigger_job: background execution failed for %r", name)

        task = asyncio.create_task(_run(), name=f"trigger-{name}")
        # Give the task a moment so the execution record is created.
        await asyncio.sleep(0.05)
        logger.info("Manually triggered job %r", name)
        return json.dumps({"status": "triggered", "job": _job_to_dict(job)})

    @mcp.tool()
    def get_daemon_status() -> str:
        """Get the daemon's overall running status.

        Returns job list, running queue status, log file location,
        and maildir mailbox list.

        Returns
        -------
        str
            JSON object with daemon status information.
        """
        # Jobs
        jobs = job_store.list_jobs()
        job_summaries = []
        for j in jobs:
            job_summaries.append({
                "name": j.name,
                "cron_expression": j.cron_expression,
                "status": j.status.value,
                "queue_name": j.queue_name,
            })

        # Maildir mailboxes
        mailboxes = maildir_store.list_mailboxes()

        # Log file path
        log_path = None
        root_logger = logging.getLogger("overlord")
        for handler in root_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                log_path = handler.baseFilename
                break

        # Scheduler runtime status (running tasks, queues)
        scheduler_status = {}
        if status_callback is not None:
            scheduler_status = status_callback()

        result = {
            "daemon": "running",
            "jobs": job_summaries,
            "scheduler": scheduler_status,
            "log_file": log_path,
            "mailboxes": mailboxes,
        }
        return json.dumps(result)

    @mcp.tool()
    async def shutdown_daemon() -> str:
        """Request the scheduler daemon to shut down gracefully.

        Returns
        -------
        str
            JSON confirmation or error message.
        """
        if shutdown_callback is None:
            return json.dumps({"error": "shutdown not available (no callback registered)"})
        logger.info("shutdown_daemon tool invoked, requesting graceful shutdown")
        asyncio.get_running_loop().call_soon(asyncio.ensure_future, shutdown_callback())
        return json.dumps({"status": "shutdown initiated"})

    @mcp.tool()
    def rotate_log() -> str:
        """Rotate the daemon's log file.

        Closes and reopens the file-based log handler so that external
        tools can rename the current log file beforehand and the daemon
        will start writing to a fresh file at the original path.

        Returns
        -------
        str
            JSON object with a status message.
        """
        if rotate_log_callback is None:
            return json.dumps({"error": "rotate_log not available (no callback registered)"})
        result = rotate_log_callback()
        logger.info("rotate_log tool invoked: %s", result)
        return json.dumps({"status": result})

    return mcp
