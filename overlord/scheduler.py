"""Cron-based job scheduler with graceful shutdown.

The scheduler runs an asyncio loop that, once per minute, evaluates which
enabled jobs are due and dispatches them via the executor.  It handles:

- SIGTERM / SIGINT for graceful shutdown
- Stale lock cleanup on startup
- Schema version validation on startup
- Concurrent job execution via asyncio tasks
- Consumer jobs: jobs with a non-empty ``consumes`` list only run when
  matching unconsumed messages exist.  Messages are passed to the job
  via stdin and auto-marked consumed on success.
"""

import asyncio
import logging
import signal
from datetime import datetime
from pathlib import Path
from typing import Optional

from .cron import CronExpression
from .database import Database
from .executor import run_job
from .mcp_server import create_mcp_server
from .models import JobStatus

logger = logging.getLogger("overlord.scheduler")


class Scheduler:
    """Cron-based job scheduler.

    Usage::

        scheduler = Scheduler(db_path=Path("/path/to/overlord.db"))
        await scheduler.run()
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        tick_seconds: int = 60,
        mcp_host: Optional[str] = None,
        mcp_port: int = 8000,
    ):
        self._db = Database(db_path)
        self._tick_seconds = tick_seconds
        self._stop_event = asyncio.Event()
        self._cancel_event = asyncio.Event()
        self._running_tasks: dict[int, asyncio.Task] = {}
        self._cwd = Path.cwd()
        self._mcp_server = None
        self._mcp_task: Optional[asyncio.Task] = None
        if mcp_host is not None:
            self._mcp_server = create_mcp_server(
                db=self._db, host=mcp_host, port=mcp_port,
                cwd=self._cwd,
            )

    async def run(self) -> None:
        """Start the scheduler loop.  Blocks until shutdown signal received."""
        self._db.init_schema()
        self._db.check_schema_version()

        failed = self._db.fail_running_executions()
        if failed:
            logger.info("Marked %d orphaned running execution(s) as failed", failed)

        released = self._db.release_stale_locks()
        if released:
            logger.info("Released %d stale lock(s) from previous run", released)

        self._install_signal_handlers()

        if self._mcp_server is not None:
            self._mcp_task = asyncio.create_task(
                self._run_mcp_server(), name="mcp-server"
            )
            logger.info(
                "MCP server co-started with scheduler on %s:%d",
                self._mcp_server.settings.host,
                self._mcp_server.settings.port,
            )

        logger.info("Scheduler started (tick=%ds)", self._tick_seconds)

        try:
            while not self._stop_event.is_set():
                await self._tick()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._tick_seconds,
                    )
                except asyncio.TimeoutError:
                    pass  # normal: tick interval elapsed
        finally:
            await self._shutdown()

    async def stop(self) -> None:
        """Request a graceful stop."""
        logger.info("Stop requested")
        self._stop_event.set()

    # -- internals --

    async def _run_mcp_server(self) -> None:
        """Run the MCP streamable-HTTP server."""
        await self._mcp_server.run_streamable_http_async()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._on_signal, sig)

    def _on_signal(self, sig: signal.Signals) -> None:
        logger.info("Received %s, initiating shutdown", sig.name)
        self._stop_event.set()

    async def _tick(self) -> None:
        """Evaluate all enabled jobs and launch those that are due."""
        now = datetime.now()
        jobs = self._db.list_jobs(status=JobStatus.ENABLED)

        # Clean up finished tasks.
        finished = [jid for jid, t in self._running_tasks.items() if t.done()]
        for jid in finished:
            task = self._running_tasks.pop(jid)
            if task.exception():
                logger.error(
                    "job_id=%d task raised: %s", jid, task.exception(),
                )

        for job in jobs:
            try:
                cron = CronExpression(job.cron_expression)
            except Exception:
                logger.error(
                    "job=%s invalid cron expression: %s",
                    job.name, job.cron_expression,
                )
                continue

            if not cron.matches(now):
                continue

            # Skip if this job already has a running task.
            if job.id in self._running_tasks and not self._running_tasks[job.id].done():
                logger.debug("job=%s already running, skipping", job.name)
                continue

            # Consumer gate: if the job has a non-empty consumes list, check
            # for matching unconsumed messages.  Skip if none exist.
            input_messages = None
            if job.consumes:
                input_messages = self._db.fetch_unconsumed_for_consumers(
                    job.consumes,
                )
                if not input_messages:
                    logger.debug(
                        "job=%s consumes=%s but no unconsumed messages, skipping",
                        job.name, job.consumes,
                    )
                    continue

            logger.info("job=%s is due, launching", job.name)
            task = asyncio.create_task(
                self._run_consumer_job(job, input_messages),
                name=f"job-{job.name}",
            )
            self._running_tasks[job.id] = task

    async def _run_consumer_job(self, job, input_messages) -> None:
        """Run a job and auto-mark consumed messages on success."""
        record = await run_job(
            job, self._db,
            cancel_event=self._cancel_event,
            input_messages=input_messages,
            cwd=self._cwd,
        )
        # Auto-consume delivered messages when the job succeeds.
        if input_messages and record.status.value == "success":
            msg_ids = [m.id for m in input_messages]
            self._db.mark_consumed_bulk(msg_ids)
            logger.info(
                "job=%s auto-consumed %d message(s)", job.name, len(msg_ids),
            )

    async def _shutdown(self) -> None:
        """Wait for running jobs to finish, with a grace period."""
        # Stop the MCP server.
        if self._mcp_task is not None and not self._mcp_task.done():
            self._mcp_task.cancel()
            try:
                await asyncio.wait_for(self._mcp_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            logger.info("MCP server stopped")

        active = [t for t in self._running_tasks.values() if not t.done()]
        if not active:
            logger.info("No running jobs, shutdown complete")
            self._db.close_all()
            return

        logger.info("Waiting for %d running job(s) to finish…", len(active))
        self._cancel_event.set()  # signal executors to stop retrying

        done, pending = await asyncio.wait(active, timeout=30)
        for task in pending:
            logger.warning("Force-cancelling task %s", task.get_name())
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=5)

        self._db.release_stale_locks()
        self._db.close_all()
        logger.info("Shutdown complete")
