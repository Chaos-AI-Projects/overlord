"""Cron-based job scheduler with graceful shutdown.

The scheduler runs an asyncio loop that, once per minute, evaluates which
enabled jobs are due and dispatches them via the executor.  It handles:

- SIGTERM / SIGINT for graceful shutdown
- Stale lock cleanup on startup
- Concurrent job execution via asyncio tasks
- Consumer jobs: jobs with a non-empty ``consumes`` list only run when
  matching unconsumed messages exist.  Messages are passed to the job
  via stdin and auto-marked consumed on success.
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime
from pathlib import Path
from typing import Optional

from .cron import CronExpression
from .execution_log import ExecutionLog
from .executor import run_job
from .job_store import JobStore
from .lock_store import LockStore
from .maildir import CATCHALL, MaildirStore
from .logging_config import rotate_log_handler
from .mcp_server import create_mcp_server
from .models import Job, JobStatus
from .spool import SpoolProcessor, SpoolWriter

logger = logging.getLogger("overlord.scheduler")

DEFAULT_DATA_DIR = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord"


class Scheduler:
    """Cron-based job scheduler.

    Usage::

        scheduler = Scheduler(data_dir=Path("/path/to/overlord"))
        await scheduler.run()
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        tick_seconds: int = 60,
        mcp_host: Optional[str] = None,
        mcp_port: int = 8000,
    ):
        self._data_dir = data_dir or DEFAULT_DATA_DIR
        self._job_store = JobStore(data_dir=self._data_dir)
        self._execution_log = ExecutionLog(data_dir=self._data_dir)
        self._lock_store = LockStore(data_dir=self._data_dir)
        self._spool = SpoolWriter(data_dir=self._data_dir)
        self._spool_processor = SpoolProcessor(data_dir=self._data_dir)
        self._spool_task: Optional[asyncio.Task] = None
        self._maildir = MaildirStore(data_dir=self._data_dir)
        self._tick_seconds = tick_seconds
        self._stop_event = asyncio.Event()
        self._cancel_event = asyncio.Event()
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._queue_tasks: dict[str, tuple[str, asyncio.Task]] = {}
        self._pending_queues: dict[str, list[tuple[Job, Optional[list]]]] = {}
        self._cwd = Path.cwd()
        self._mcp_server = None
        self._mcp_task: Optional[asyncio.Task] = None
        if mcp_host is not None:
            self._mcp_server = create_mcp_server(
                data_dir=self._data_dir, host=mcp_host, port=mcp_port,
                cwd=self._cwd, spool=self._spool,
                shutdown_callback=self.stop,
                rotate_log_callback=rotate_log_handler,
            )

    async def run(self) -> None:
        """Start the scheduler loop.  Blocks until shutdown signal received."""
        failed = self._execution_log.fail_running_executions()
        if failed:
            logger.info("Marked %d orphaned running execution(s) as failed", failed)

        released = self._lock_store.release_stale_locks(self._execution_log)
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

        self._spool_task = asyncio.create_task(
            self._spool_processor.run(), name="spool-processor"
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
        try:
            await self._mcp_server.run_streamable_http_async()
        except asyncio.CancelledError:
            pass

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._on_signal, sig)

    def _on_signal(self, sig: signal.Signals) -> None:
        logger.info("Received %s, initiating shutdown", sig.name)
        self._stop_event.set()

    def _active_queue_names(self) -> set[str]:
        """Return the set of queue names that currently have a running task."""
        active: set[str] = set()
        for name, (queue, task) in self._queue_tasks.items():
            if not task.done():
                active.add(queue)
        return active

    def _pending_names(self, queue_name: str) -> set[str]:
        """Return the set of job names already waiting in a pending queue."""
        return {job.name for job, _ in self._pending_queues.get(queue_name, [])}

    def _enqueue(self, job: Job, input_messages: Optional[list]) -> None:
        """Add a job to its queue's pending FIFO (with same-name dedup)."""
        queue_name = job.queue_name
        pending = self._pending_queues.setdefault(queue_name, [])
        if job.name in {j.name for j, _ in pending}:
            logger.info(
                "job=%s queue=%s already pending, dedup skip",
                job.name, queue_name,
            )
            self._emit_skipped_message(job, reason="already pending in queue")
            return
        pending.append((job, input_messages))
        logger.info(
            "job=%s queue=%s busy, enqueued (position=%d)",
            job.name, queue_name, len(pending),
        )

    def _launch_job(self, job: Job, input_messages: Optional[list]) -> None:
        """Create an asyncio task for a job and register it in tracking dicts."""
        logger.info("job=%s is due, launching (queue=%s)", job.name, job.queue_name)
        task = asyncio.create_task(
            self._run_consumer_job(job, input_messages),
            name=f"job-{job.name}",
        )
        self._running_tasks[job.name] = task
        self._queue_tasks[job.name] = (job.queue_name, task)

    def _drain_pending_queue(self, queue_name: str) -> None:
        """Launch the next pending job for a queue, if any."""
        pending = self._pending_queues.get(queue_name)
        while pending:
            job, input_messages = pending.pop(0)
            # Re-check consumer gate: messages may have been consumed since enqueue.
            if job.consumes:
                input_messages = self._maildir.fetch_unconsumed_for_consumers(job.consumes)
                if not input_messages:
                    logger.debug(
                        "job=%s dequeued but no unconsumed messages, dropping",
                        job.name,
                    )
                    continue
            self._launch_job(job, input_messages)
            return

    async def _tick(self) -> None:
        """Evaluate all enabled jobs and launch those that are due."""
        now = datetime.now()
        jobs = self._job_store.list_jobs(status=JobStatus.ENABLED)

        # Clean up finished tasks and drain their queues.
        finished = [name for name, t in self._running_tasks.items() if t.done()]
        queues_freed: set[str] = set()
        for name in finished:
            task = self._running_tasks.pop(name)
            if not task.cancelled() and task.exception():
                logger.error(
                    "job=%s task raised: %s", name, task.exception(),
                )
            if name in self._queue_tasks:
                queue_name, _ = self._queue_tasks.pop(name)
                queues_freed.add(queue_name)

        # Drain pending jobs for freed queues.
        for queue_name in queues_freed:
            if queue_name not in self._active_queue_names():
                self._drain_pending_queue(queue_name)

        busy_queues = self._active_queue_names()

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
            if job.name in self._running_tasks and not self._running_tasks[job.name].done():
                logger.debug("job=%s already running, skipping", job.name)
                continue

            # Consumer gate: if the job has a non-empty consumes list, check
            # for matching unconsumed messages in Maildir.  Skip if none exist.
            input_messages = None
            if job.consumes:
                input_messages = self._maildir.fetch_unconsumed_for_consumers(
                    job.consumes,
                )
                if not input_messages:
                    logger.debug(
                        "job=%s consumes=%s but no unconsumed messages, skipping",
                        job.name, job.consumes,
                    )
                    continue

            # Queue gate: if the job's queue is busy, enqueue (FIFO) with
            # same-name dedup instead of skipping outright.
            if job.queue_name in busy_queues:
                self._enqueue(job, input_messages)
                continue

            self._launch_job(job, input_messages)
            busy_queues.add(job.queue_name)

    def _emit_skipped_message(self, job: Job, reason: Optional[str] = None) -> None:
        """Emit a message with no consumer indicating the job was skipped."""
        payload = json.dumps({
            "job_name": job.name,
            "status": "skipped",
            "reason": reason or f"queue '{job.queue_name}' is busy",
        })
        msg = MaildirStore.build_message(
            payload=payload, consumer=None, job_name=job.name,
        )
        self._spool.write(msg)
        logger.debug("job=%s emitted skipped message", job.name)

    async def _run_consumer_job(self, job, input_messages) -> None:
        """Run a job and auto-consume Maildir messages on success."""
        record = await run_job(
            job, self._execution_log, self._lock_store, self._spool,
            cancel_event=self._cancel_event,
            input_messages=input_messages,
            cwd=self._cwd,
        )
        # Auto-consume delivered messages by moving them to the processed
        # subfolder in Maildir when the job succeeds.
        if input_messages and record.status.value == "success":
            for m in input_messages:
                target = m.get("consumer") or CATCHALL
                self._maildir.consume(target, m["key"])
            logger.info(
                "job=%s auto-consumed %d message(s)", job.name, len(input_messages),
            )

    async def _shutdown(self) -> None:
        """Graceful shutdown: drain queues, wait for jobs, then clean up.

        1. Block new task scheduling (already done — the run() loop has exited).
        2. Wait for all running and queued tasks to finish.
        3. Clean up infrastructure (MCP server, spool processor).
        """
        # Wait for all running jobs to finish and drain pending queues
        # as their queue slots free up.
        while True:
            active = [t for t in self._running_tasks.values() if not t.done()]
            has_pending = any(self._pending_queues.values())

            if not active and not has_pending:
                break

            if active:
                logger.info(
                    "Waiting for %d running job(s) to finish "
                    "(pending queued: %s)…",
                    len(active),
                    {q: len(p) for q, p in self._pending_queues.items() if p}
                    or "none",
                )
                await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)

                # Clean up finished tasks and drain freed queues.
                finished = [
                    name for name, t in self._running_tasks.items() if t.done()
                ]
                queues_freed: set[str] = set()
                for name in finished:
                    task = self._running_tasks.pop(name)
                    if not task.cancelled() and task.exception():
                        logger.error(
                            "job=%s task raised: %s", name, task.exception(),
                        )
                    if name in self._queue_tasks:
                        queue_name, _ = self._queue_tasks.pop(name)
                        queues_freed.add(queue_name)

                for queue_name in queues_freed:
                    if queue_name not in self._active_queue_names():
                        self._drain_pending_queue(queue_name)
            elif has_pending:
                # Pending items with no active tasks — drain all idle queues.
                for queue_name in list(self._pending_queues.keys()):
                    if queue_name not in self._active_queue_names():
                        self._drain_pending_queue(queue_name)

        logger.info("All jobs finished")

        # Stop the spool processor (drains remaining messages before exiting).
        if self._spool_task is not None and not self._spool_task.done():
            self._spool_processor.stop()
            try:
                await asyncio.wait_for(self._spool_task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            logger.info("Spool processor stopped")

        # Stop the MCP server.
        # Grace delay lets the in-flight shutdown response complete before
        # we cancel the task, avoiding noisy CancelledError tracebacks (#167).
        if self._mcp_task is not None and not self._mcp_task.done():
            await asyncio.sleep(0.5)
            self._mcp_task.cancel()
            try:
                await asyncio.wait_for(self._mcp_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            logger.info("MCP server stopped")

        self._lock_store.release_stale_locks(self._execution_log)
        logger.info("Shutdown complete")
