"""Async job executor — runs shell commands as subprocesses.

Handles timeout enforcement, stdout/stderr capture, lock acquisition/release,
retry logic, and message production.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from .execution_log import ExecutionLog
from .lock_store import LockStore
from .maildir import MaildirStore
from .models import ExecutionRecord, ExecutionStatus, Job, JobOutput, JobOutputError
from .spool import SpoolWriter

logger = logging.getLogger("overlord.executor")


async def run_job(
    job: Job,
    execution_log: ExecutionLog,
    lock_store: LockStore,
    spool: SpoolWriter,
    cancel_event: Optional[asyncio.Event] = None,
    input_messages: Optional[list[dict]] = None,
    cwd: Optional[Path] = None,
) -> ExecutionRecord:
    """Execute a job's shell command, respecting locks, timeout, and retries.

    Args:
        job: The job definition to execute.
        execution_log: Execution history log.
        lock_store: File-based named lock store.
        spool: Spool writer for message delivery.
        cancel_event: If set, the executor will abort before starting or
            between retries.  Used for graceful shutdown.
        input_messages: Maildir message dicts to pass to the job via stdin
            (for consumer jobs whose ``consumes`` list is non-empty).
            Each dict has keys: ``key``, ``consumer``, ``payload``,
            ``subject``, ``date``.

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

        record = execution_log.create_execution(job.name)
        lock_acquired = False

        try:
            # Acquire exclusive lock if required.
            if job.exclusive_lock:
                lock_acquired = lock_store.acquire_lock(job.exclusive_lock, record.id)
                if not lock_acquired:
                    logger.warning(
                        "job=%s execution=%d lock=%s held, skipping",
                        job.name, record.id, job.exclusive_lock,
                    )
                    execution_log.finish_execution(
                        record.id, ExecutionStatus.FAILED,
                        stderr=f"Could not acquire lock: {job.exclusive_lock}",
                    )
                    last_record = _reload_record(execution_log, record)
                    break  # lock contention is not retryable

            last_record = await _run_subprocess(
                job, record, execution_log, cancel_event, input_messages, cwd=cwd,
            )

            if last_record.status == ExecutionStatus.SUCCESS:
                break  # no retry needed
        finally:
            if lock_acquired and job.exclusive_lock:
                lock_store.release_lock(job.exclusive_lock)

    if last_record is None:
        # Cancelled before any attempt ran — create a record to reflect that.
        record = execution_log.create_execution(job.name)
        execution_log.finish_execution(record.id, ExecutionStatus.FAILED,
                            stderr="Cancelled before execution")
        last_record = execution_log.get_execution(record.id)

    last_record = _produce_message(job, last_record, execution_log, spool=spool)
    return last_record


async def _run_subprocess(
    job: Job,
    record: ExecutionRecord,
    execution_log: ExecutionLog,
    cancel_event: Optional[asyncio.Event],
    input_messages: Optional[list[dict]] = None,
    cwd: Optional[Path] = None,
) -> ExecutionRecord:
    """Spawn the shell command and wait for it to finish (or timeout)."""
    logger.info("job=%s execution=%d starting: %s", job.name, record.id, job.command)

    stdin_mode = asyncio.subprocess.PIPE if input_messages else asyncio.subprocess.DEVNULL

    proc = await asyncio.create_subprocess_shell(
        job.command,
        stdin=stdin_mode,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )

    stdin_data = None
    if input_messages:
        # Extract payload.json content from each Maildir message and
        # concatenate into a JSON array for the consumer job's stdin.
        payloads = []
        for m in input_messages:
            raw = m.get("payload", "")
            try:
                payloads.append(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                payloads.append(raw)
        stdin_data = json.dumps(payloads).encode()

    timed_out = False
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(input=stdin_data),
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

    execution_log.finish_execution(record.id, status, exit_code=exit_code,
                        stdout=stdout, stderr=stderr)

    logger.info(
        "job=%s execution=%d status=%s exit_code=%s",
        job.name, record.id, status.value, exit_code,
    )
    return _reload_record(execution_log, record)


def _reload_record(execution_log: ExecutionLog, record: ExecutionRecord) -> ExecutionRecord:
    """Re-read the execution record to pick up finish timestamp."""
    reloaded = execution_log.get_execution(record.id)
    return reloaded if reloaded is not None else record


def _produce_message(
    job: Job,
    record: ExecutionRecord,
    execution_log: ExecutionLog,
    spool: SpoolWriter,
) -> ExecutionRecord:
    """Create a message from a completed job execution.

    For successful executions, stdout is parsed as a structured JobOutput
    (with ``consumer`` and ``message`` fields).  If parsing fails, the
    execution is retroactively marked as FAILED and the validation error
    is recorded.

    For non-successful executions (failed/timeout), the raw execution
    details are used as the message payload without schema validation.

    Messages are written to the file-based spool for async Maildir delivery.

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
            execution_log.finish_execution(
                record.id, ExecutionStatus.FAILED,
                exit_code=record.exit_code,
                stdout=record.stdout,
                stderr=f"Output schema validation failed: {exc}",
            )
            payload = json.dumps({
                "job_name": job.name,
                "execution_id": record.id,
                "status": ExecutionStatus.FAILED.value,
                "exit_code": record.exit_code,
                "error": f"Output schema validation failed: {exc}",
            })
            _emit_message(payload, job.name, spool=spool)
            logger.debug("job=%s execution=%d produced error message", job.name, record.id)
            return execution_log.get_execution(record.id)

        payload = json.dumps({
            "job_name": job.name,
            "execution_id": record.id,
            "status": record.status.value,
            "exit_code": record.exit_code,
            "message": output.message,
        })
        _emit_message(payload, job.name, spool=spool, consumer=output.consumer)
        logger.debug(
            "job=%s execution=%d produced message consumer=%s",
            job.name, record.id, output.consumer,
        )
    else:
        payload = json.dumps({
            "job_name": job.name,
            "execution_id": record.id,
            "status": record.status.value,
            "exit_code": record.exit_code,
            "stdout": record.stdout,
            "stderr": record.stderr,
        })
        _emit_message(payload, job.name, spool=spool)
        logger.debug("job=%s execution=%d produced message", job.name, record.id)
    # Always return a fresh record so both success and failure
    # paths behave consistently.
    refreshed = execution_log.get_execution(record.id)
    return refreshed if refreshed is not None else record


def _emit_message(
    payload: str,
    job_name: str,
    spool: SpoolWriter,
    consumer: Optional[str] = None,
) -> None:
    """Write a message to the spool for async Maildir delivery.

    The message is built as an RFC 822 email and written atomically to the
    spool directory.
    """
    msg = MaildirStore.build_message(
        payload=payload, consumer=consumer, job_name=job_name,
    )
    spool.write(msg)
