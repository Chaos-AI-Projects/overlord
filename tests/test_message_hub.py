"""Tests for message production and scheduler-driven consumption."""

import asyncio
import json

import pytest

from email import policy
from email.parser import BytesParser

from overlord.database import Database
from overlord.executor import run_job
from overlord.models import Job
from overlord.spool import SpoolWriter


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    d.init_schema()
    yield d
    d.close()


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


def make_job(db, **kwargs) -> Job:
    defaults = dict(
        name="test-job",
        cron_expression="* * * * *",
        command="echo hello",
    )
    defaults.update(kwargs)
    return db.create_job(Job(**defaults))


class TestMessageProduction:
    """Verify that the executor produces messages after job execution."""

    @pytest.mark.asyncio
    async def test_successful_job_produces_message(self, db, spool, tmp_path):
        output = json.dumps({"consumer": None, "message": "produced"})
        job = make_job(db, command=f"echo '{output}'")
        await run_job(job, db, spool)

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1

        payload = json.loads(messages[0]["payload"])
        assert payload["job_name"] == "test-job"
        assert payload["status"] == "success"
        assert payload["message"] == "produced"

    @pytest.mark.asyncio
    async def test_failed_job_produces_message(self, db, spool, tmp_path):
        job = make_job(db, command="exit 1")
        await run_job(job, db, spool)

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1

        payload = json.loads(messages[0]["payload"])
        assert payload["status"] == "failed"
        assert payload["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_retried_job_produces_single_message(self, db, spool, tmp_path):
        job = make_job(db, command="exit 1", max_retries=2, retry_delay_seconds=0)
        await run_job(job, db, spool)

        messages = read_spool_messages(tmp_path)
        # Only the final execution result produces a message.
        assert len(messages) == 1

        payload = json.loads(messages[0]["payload"])
        assert payload["status"] == "failed"


class TestFetchUnconsumedForConsumers:
    """Test the fetch_unconsumed_for_consumers database method."""

    def test_fetch_by_consumer_name(self, db):
        job = make_job(db)
        db.create_message(job.id, '{"event": "test"}', consumer="agent")
        db.create_message(job.id, '{"event": "test2"}', consumer="logger")

        messages = db.fetch_unconsumed_for_consumers(["agent"])
        assert len(messages) == 1
        assert messages[0].consumer == "agent"

    def test_fetch_multiple_consumer_names(self, db):
        job = make_job(db)
        db.create_message(job.id, "a", consumer="agent")
        db.create_message(job.id, "b", consumer="logger")
        db.create_message(job.id, "c", consumer="other")

        messages = db.fetch_unconsumed_for_consumers(["agent", "logger"])
        assert len(messages) == 2
        consumers = {m.consumer for m in messages}
        assert consumers == {"agent", "logger"}

    def test_fetch_catchall(self, db):
        job = make_job(db)
        db.create_message(job.id, "unaddressed")
        db.create_message(job.id, "addressed", consumer="specific")

        messages = db.fetch_unconsumed_for_consumers(["*"])
        assert len(messages) == 1
        assert messages[0].consumer is None

    def test_fetch_empty_consumes_returns_nothing(self, db):
        job = make_job(db)
        db.create_message(job.id, "data")

        messages = db.fetch_unconsumed_for_consumers([])
        assert len(messages) == 0

    def test_fetch_skips_consumed(self, db):
        job = make_job(db)
        msg = db.create_message(job.id, "consumed", consumer="agent")
        db.mark_consumed(msg.id)
        db.create_message(job.id, "fresh", consumer="agent")

        messages = db.fetch_unconsumed_for_consumers(["agent"])
        assert len(messages) == 1
        assert messages[0].payload == "fresh"


class TestMarkConsumedBulk:
    """Test bulk consumption marking."""

    def test_mark_consumed_bulk(self, db):
        job = make_job(db)
        m1 = db.create_message(job.id, "a", consumer="x")
        m2 = db.create_message(job.id, "b", consumer="x")
        db.create_message(job.id, "c", consumer="x")

        db.mark_consumed_bulk([m1.id, m2.id])

        remaining = db.fetch_unconsumed_for_consumers(["x"])
        assert len(remaining) == 1
        assert remaining[0].payload == "c"

    def test_mark_consumed_bulk_empty(self, db):
        # Should not raise.
        db.mark_consumed_bulk([])
