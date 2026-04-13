"""Async job executor — runs shell commands as subprocesses.

Handles timeout enforcement, stdout/stderr capture, lock acquisition/release,
retry logic, and message production for the message hub.
"""

import asyncio
import json
import logging
from typing import Optional

from .database import Database
from .models import ExecutionRecord, ExecutionStatus, Job, JobOutput, JobOutputError

logger = logging.getLogger("overlord.executor")


async def run_job(
    job: Job,
    db: Database,
    cancel_event: Optional[asyncio.Event] = None,
) -> ExecutionRecord:
    """Execute a job's shell command, respecting locks, timeout, and retries.

    Args:
        job: The job definition to execute.
        db: Database handle (thread-local connections are safe here).
        cancel_event: If set, the executor will abort before starting or
            between retries.  Used for graceful shutdown.

    Returns:
        The final ExecutionRecord for this run.
    """
    max_attempts = job.max_retries + 1
    last_record: Optional[ExecutionRecord] = None

    for attempt in range(max_attempts):
        if cancel_event and cancel_event.is_set():
            logger.info("job=%s cancelled before attempt %d", job.name, attempt + 1)
            break

        # Wait before retry (skip for the first attempt).
        if attempt > 0 and job.retry_delay_seconds > 0:
            logger.info(
                "job=%s retry %d/%d in %ds",
                job.name, attempt, job.max_retries, job.retry_delay_seconds,
            )
            await asyncio.sleep(job.retry_delay_seconds)
            if cancel_event and cancel_event.is_set():
                break

        record = db.create_execution(job.id)
        lock_acquired = False

        try:
            # Acquire exclusive lock if required.
            if job.exclusive_lock:
                lock_acquired = db.acquire_lock(job.exclusive_lock, record.id)
                if not lock_acquired:
                    logger.warning(
                        "job=%s execution=%d lock=%s held, skipping",
                        job.name, record.id, job.exclusive_lock,
                    )
                    db.finish_execution(
                        record.id, ExecutionStatus.FAILED,
                        stderr=f"Could not acquire lock: {job.exclusive_lock}",
                    )
                    last_record = _reload_record(db, record)
                    break  # lock contention is not retryable

            last_record = await _run_subprocess(job, record, db, cancel_event)

            if last_record.status == ExecutionStatus.SUCCESS:
                break  # no retry needed
        finally:
            if lock_acquired and job.exclusive_lock:
                db.release_lock(job.exclusive_lock)

    if last_record is None:
        # Cancelled before any attempt ran — create a record to reflect that.
        record = db.create_execution(job.id)
        db.finish_execution(record.id, ExecutionStatus.FAILED,
                            stderr="Cancelled before execution")
        last_record = db.get_execution(record.id)

    last_record = _produce_message(job, last_record, db)
    return last_record


async def _run_subprocess(
    job: Job,
    record: ExecutionRecord,
    db: Database,
    cancel_event: Optional[asyncio.Event],
) -> ExecutionRecord:
    """Spawn the shell command and wait for it to finish (or timeout)."""
    logger.info("job=%s execution=%d starting: %s", job.name, record.id, job.command)

    proc = await asyncio.create_subprocess_shell(
        job.command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    timed_out = False
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(),
            timeout=job.timeout_seconds,  # None means no timeout
        )
    except asyncio.TimeoutError:
        timed_out = True
        logger.warning(
            "job=%s execution=%d timed out after %ds, killing",
            job.name, record.id, job.timeout_seconds,
        )
        proc.kill()
        stdout_bytes, stderr_bytes = await proc.communicate()

    stdout = stdout_bytes.decode(errors="replace") if stdout_bytes else None
    stderr = stderr_bytes.decode(errors="replace") if stderr_bytes else None
    exit_code = proc.returncode

    if timed_out:
        status = ExecutionStatus.TIMEOUT
    elif exit_code == 0:
        status = ExecutionStatus.SUCCESS
    else:
        status = ExecutionStatus.FAILED

    db.finish_execution(record.id, status, exit_code=exit_code,
                        stdout=stdout, stderr=stderr)

    logger.info(
        "job=%s execution=%d status=%s exit_code=%s",
        job.name, record.id, status.value, exit_code,
    )
    return _reload_record(db, record)


def _reload_record(db: Database, record: ExecutionRecord) -> ExecutionRecord:
    """Re-read the execution record from the DB to pick up finish timestamp."""
    reloaded = db.get_execution(record.id)
    return reloaded if reloaded is not None else record


def _produce_message(
    job: Job, record: ExecutionRecord, db: Database,
) -> ExecutionRecord:
    """Create a message from a completed job execution for the message hub.

    For successful executions, stdout is parsed as a structured JobOutput
    (with ``consumers`` and ``message`` fields).  If parsing fails, the
    execution is retroactively marked as FAILED and the validation error
    is recorded.

    For non-successful executions (failed/timeout), the raw execution
    details are used as the message payload without schema validation.

    Returns the (possibly updated) execution record.
    """
    if record.status == ExecutionStatus.SUCCESS:
        try:
            output = JobOutput.from_stdout(record.stdout or "")
        except JobOutputError as exc:
            logger.warning(
                "job=%s execution=%d output schema validation failed: %s",
                job.name, record.id, exc,
            )
            db.finish_execution(
                record.id, ExecutionStatus.FAILED,
                exit_code=record.exit_code,
                stdout=record.stdout,
                stderr=f"Output schema validation failed: {exc}",
            )
            payload = json.dumps({
                "job_name": job.name,
                "job_id": job.id,
                "execution_id": record.id,
                "status": ExecutionStatus.FAILED.value,
                "exit_code": record.exit_code,
                "error": f"Output schema validation failed: {exc}",
            })
            db.create_message(job.id, payload)
            logger.debug("job=%s execution=%d produced error message", job.name, record.id)
            return db.get_execution(record.id)

        payload = json.dumps({
            "job_name": job.name,
            "job_id": job.id,
            "execution_id": record.id,
            "status": record.status.value,
            "exit_code": record.exit_code,
            "message": output.message,
        })
        db.create_message(job.id, payload, consumers=output.consumers)
        logger.debug(
            "job=%s execution=%d produced message consumers=%s",
            job.name, record.id, output.consumers,
        )
    else:
        payload = json.dumps({
            "job_name": job.name,
            "job_id": job.id,
            "execution_id": record.id,
            "status": record.status.value,
            "exit_code": record.exit_code,
            "stdout": record.stdout,
            "stderr": record.stderr,
        })
        db.create_message(job.id, payload)
        logger.debug("job=%s execution=%d produced message", job.name, record.id)
    return record
