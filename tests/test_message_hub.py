"""Tests for message production and scheduler-driven consumption."""

import asyncio
import json

import pytest

from email import policy
from email.parser import BytesParser

from overlord.execution_log import ExecutionLog
from overlord.executor import run_job
from overlord.lock_store import LockStore
from overlord.models import Job
from overlord.spool import SpoolWriter


@pytest.fixture
def execution_log(tmp_path):
    return ExecutionLog(data_dir=tmp_path)


@pytest.fixture
def lock_store(tmp_path):
    return LockStore(data_dir=tmp_path)


@pytest.fixture
def spool(tmp_path):
    return SpoolWriter(data_dir=tmp_path)


def read_spool_messages(tmp_path):
    """Read all .eml files from the spool directory, return dicts with consumer/payload."""
    spool_dir = tmp_path / "spool"
    if not spool_dir.exists():
        return []
    parser = BytesParser(policy=policy.default)
    results = []
    for f in sorted(spool_dir.iterdir()):
        if f.suffix == ".eml":
            msg = parser.parsebytes(f.read_bytes())
            payload_str = None
            for part in msg.iter_attachments():
                if part.get_content_type() == "application/json":
                    payload_str = part.get_content().decode("utf-8")
                    break
            results.append({
                "consumer": msg.get("X-Overlord-Consumer"),
                "payload": payload_str,
            })
    return results


def make_job(**kwargs) -> Job:
    defaults = dict(
        name="test-job",
        cron_expression="* * * * *",
        command="echo hello",
    )
    defaults.update(kwargs)
    return Job(**defaults)


class TestMessageProduction:
    """Verify that the executor produces messages after job execution."""

    @pytest.mark.asyncio
    async def test_successful_job_produces_message(self, execution_log, lock_store, spool, tmp_path):
        output = json.dumps({"consumer": None, "message": "produced"})
        job = make_job(command=f"echo '{output}'")
        await run_job(job, execution_log, lock_store, spool)

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1

        payload = json.loads(messages[0]["payload"])
        assert payload["job_name"] == "test-job"
        assert payload["status"] == "success"
        assert payload["message"] == "produced"

    @pytest.mark.asyncio
    async def test_failed_job_produces_message(self, execution_log, lock_store, spool, tmp_path):
        job = make_job(command="exit 1")
        await run_job(job, execution_log, lock_store, spool)

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1

        payload = json.loads(messages[0]["payload"])
        assert payload["status"] == "failed"
        assert payload["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_retried_job_produces_single_message(self, execution_log, lock_store, spool, tmp_path):
        job = make_job(command="exit 1", max_retries=2, retry_delay_seconds=0)
        await run_job(job, execution_log, lock_store, spool)

        messages = read_spool_messages(tmp_path)
        # Only the final execution result produces a message.
        assert len(messages) == 1

        payload = json.loads(messages[0]["payload"])
        assert payload["status"] == "failed"
