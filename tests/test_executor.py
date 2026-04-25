"""Tests for the async job executor."""

import asyncio
import json

import pytest

from overlord.database import Database
from overlord.executor import run_job
from overlord.models import ExecutionStatus, Job, JobStatus
from overlord.spool import SpoolWriter


# Valid structured output for jobs that need to succeed.
_VALID_OUTPUT = json.dumps({"consumer": None, "message": "ok"})


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    d.init_schema()
    yield d
    d.close()


@pytest.fixture
def spool(tmp_path):
    return SpoolWriter(data_dir=tmp_path)


def make_job(db, **kwargs) -> Job:
    defaults = dict(
        name="test-job",
        cron_expression="* * * * *",
        command="echo hello",
    )
    defaults.update(kwargs)
    return db.create_job(Job(**defaults))


class TestExecutor:
    @pytest.mark.asyncio
    async def test_successful_command(self, db, spool):
        job = make_job(db, command=f"echo '{_VALID_OUTPUT}'")
        record = await run_job(job, db, spool)
        assert record.status == ExecutionStatus.SUCCESS
        assert record.exit_code == 0

    @pytest.mark.asyncio
    async def test_failed_command(self, db, spool):
        job = make_job(db, command="exit 42")
        record = await run_job(job, db, spool)
        assert record.status == ExecutionStatus.FAILED
        assert record.exit_code == 42

    @pytest.mark.asyncio
    async def test_stderr_capture(self, db, spool):
        job = make_job(db, command=f"echo '{_VALID_OUTPUT}' && echo err >&2")
        record = await run_job(job, db, spool)
        assert record.status == ExecutionStatus.SUCCESS
        assert "err" in record.stderr

    @pytest.mark.asyncio
    async def test_timeout(self, db, spool):
        job = make_job(db, command="sleep 60", timeout_seconds=1)
        record = await run_job(job, db, spool)
        assert record.status == ExecutionStatus.TIMEOUT

    @pytest.mark.asyncio
    async def test_exclusive_lock(self, db, spool):
        job = make_job(db, command=f"echo '{_VALID_OUTPUT}'", exclusive_lock="deploy")
        record = await run_job(job, db, spool)
        assert record.status == ExecutionStatus.SUCCESS
        # Lock should be released after execution.
        assert db.get_lock("deploy") is None

    @pytest.mark.asyncio
    async def test_lock_contention(self, db, spool):
        job = make_job(db, command="echo hello", exclusive_lock="deploy")
        # Hold the lock with a fake execution.
        blocker = db.create_execution(job.id)
        db.acquire_lock("deploy", blocker.id)

        record = await run_job(job, db, spool)
        assert record.status == ExecutionStatus.FAILED
        assert "lock" in record.stderr.lower()

        # Clean up.
        db.release_lock("deploy")

    @pytest.mark.asyncio
    async def test_retry_on_failure(self, db, spool):
        # Command fails on every attempt — should see max_retries+1 executions.
        job = make_job(
            db, command="exit 1",
            max_retries=2, retry_delay_seconds=0,
        )
        record = await run_job(job, db, spool)
        assert record.status == ExecutionStatus.FAILED

        history = db.get_execution_history(job.id, limit=10)
        assert len(history) == 3  # 1 initial + 2 retries

    @pytest.mark.asyncio
    async def test_cancel_event_stops_retries(self, db, spool):
        cancel = asyncio.Event()
        cancel.set()  # already cancelled

        job = make_job(
            db, command="exit 1",
            max_retries=5, retry_delay_seconds=0,
        )
        record = await run_job(job, db, spool, cancel_event=cancel)
        # Should have stopped early — far fewer than 6 executions.
        history = db.get_execution_history(job.id, limit=10)
        assert len(history) <= 1

    @pytest.mark.asyncio
    async def test_cwd_passed_to_subprocess(self, db, spool, tmp_path):
        """Jobs run in the specified working directory."""
        from pathlib import Path

        target_dir = tmp_path / "workdir"
        target_dir.mkdir()
        job = make_job(db, command=f"echo '{{\"consumer\": null, \"message\": \"'$(pwd)'\"}}' ")
        record = await run_job(job, db, spool, cwd=target_dir)
        assert record.status == ExecutionStatus.SUCCESS
        assert str(target_dir) in record.stdout

    @pytest.mark.asyncio
    async def test_input_messages_passed_via_stdin(self, db, spool):
        """Consumer jobs receive Maildir message payloads on stdin."""
        job = make_job(db, command="cat")
        # Simulate Maildir message dicts as returned by MaildirStore.fetch_messages().
        input_messages = [
            {
                "key": "fake-key-1",
                "consumer": "test-job",
                "payload": '{"data": "hello"}',
                "subject": "test-job 2026-01-01T00:00:00+00:00",
                "date": "2026-01-01T00:00:00+00:00",
            },
        ]

        record = await run_job(job, db, spool, input_messages=input_messages)
        # cat echoes stdin back — stdout should contain the payload JSON,
        # but the output won't be valid JobOutput schema so it'll be marked failed.
        reloaded = db.get_execution(record.id)
        assert reloaded.stdout is not None
        assert "hello" in reloaded.stdout
