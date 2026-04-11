"""MCP server exposing Overlord job registry for agent-driven job management."""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional

from .database import DEFAULT_DB_PATH, Database
from .models import Job, JobStatus

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
        "created_at": str(job.created_at) if job.created_at else None,
        "updated_at": str(job.updated_at) if job.updated_at else None,
    }


def _execution_to_dict(rec) -> dict:
    """Convert an ExecutionRecord to a JSON-serialisable dictionary."""
    return {
        "id": rec.id,
        "job_id": rec.job_id,
        "status": rec.status.value,
        "started_at": str(rec.started_at) if rec.started_at else None,
        "finished_at": str(rec.finished_at) if rec.finished_at else None,
        "exit_code": rec.exit_code,
        "stdout": rec.stdout,
        "stderr": rec.stderr,
    }


def create_mcp_server(db_path: Optional[Path] = None):
    """Create and return a FastMCP server wired to the given database.

    Parameters
    ----------
    db_path : Path, optional
        Path to the SQLite database.  Falls back to DEFAULT_DB_PATH.

    Returns
    -------
    FastMCP
        The configured MCP server instance, ready to be run.
    """
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("overlord-job-registry")
    db = Database(db_path=db_path)
    db.init_schema()

    @mcp.tool()
    def register_job(
        name: str,
        cron_expression: str,
        command: str,
        exclusive_lock: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        max_retries: int = 0,
        retry_delay_seconds: int = 0,
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

        Returns
        -------
        str
            JSON object with the created job details.
        """
        job = Job(
            name=name,
            cron_expression=cron_expression,
            command=command,
            exclusive_lock=exclusive_lock,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
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
        result["recent_executions"] = [_execution_to_dict(e) for e in executions]
        return json.dumps(result)

    return mcp
