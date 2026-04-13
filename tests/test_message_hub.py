"""Tests for the message hub — poll-based consumption and agent invocation."""

import asyncio
import json

import pytest

from overlord.database import Database
from overlord.executor import run_job
from overlord.message_hub import MessageHub, _message_to_dict
from overlord.models import Job


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    d.init_schema()
    yield d
    d.close()


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
    async def test_successful_job_produces_message(self, db):
        output = json.dumps({"consumers": [], "message": "produced"})
        job = make_job(db, command=f"echo '{output}'")
        await run_job(job, db)

        messages = db.poll_messages()
        assert len(messages) == 1

        payload = json.loads(messages[0].payload)
        assert payload["job_name"] == "test-job"
        assert payload["status"] == "success"
        assert payload["message"] == "produced"

    @pytest.mark.asyncio
    async def test_failed_job_produces_message(self, db):
        job = make_job(db, command="exit 1")
        await run_job(job, db)

        messages = db.poll_messages()
        assert len(messages) == 1

        payload = json.loads(messages[0].payload)
        assert payload["status"] == "failed"
        assert payload["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_retried_job_produces_single_message(self, db):
        job = make_job(db, command="exit 1", max_retries=2, retry_delay_seconds=0)
        await run_job(job, db)

        messages = db.poll_messages()
        # Only the final execution result produces a message.
        assert len(messages) == 1

        payload = json.loads(messages[0].payload)
        assert payload["status"] == "failed"


class TestMessageHub:
    """Test the MessageHub poll loop and handler invocation."""

    @pytest.mark.asyncio
    async def test_poll_consumes_messages(self, db):
        job = make_job(db)
        db.create_message(job.id, '{"event": "test"}')
        db.create_message(job.id, '{"event": "test2"}')

        hub = MessageHub(db=db, poll_seconds=1)

        # Run one poll cycle directly.
        await hub._poll_cycle()

        # Both messages should now be consumed.
        remaining = db.poll_messages()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_handler_receives_message_on_stdin(self, db):
        job = make_job(db)
        db.create_message(job.id, "test-payload")

        # Handler echoes stdin back to stdout — verifies the message arrived.
        hub = MessageHub(db=db, handler_command="cat", poll_seconds=1)
        await hub._poll_cycle()

        remaining = db.poll_messages()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_handler_failure_leaves_message_unconsumed(self, db):
        job = make_job(db)
        db.create_message(job.id, "important-payload")

        # Handler that always fails.
        hub = MessageHub(db=db, handler_command="exit 1", poll_seconds=1)
        await hub._poll_cycle()

        # Message should remain unconsumed on handler failure.
        remaining = db.poll_messages()
        assert len(remaining) == 1
        assert remaining[0].payload == "important-payload"

    @pytest.mark.asyncio
    async def test_no_handler_still_consumes(self, db):
        job = make_job(db)
        db.create_message(job.id, "log-only-payload")

        # No handler configured — messages are logged and consumed.
        hub = MessageHub(db=db, handler_command=None, poll_seconds=1)
        await hub._poll_cycle()

        remaining = db.poll_messages()
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_stop_terminates_run_loop(self, db):
        hub = MessageHub(db=db, poll_seconds=60)

        async def stop_after_delay():
            await asyncio.sleep(0.1)
            await hub.stop()

        stop_task = asyncio.create_task(stop_after_delay())
        await hub.run()  # should return promptly after stop
        await stop_task

    @pytest.mark.asyncio
    async def test_batch_size_limits_poll(self, db):
        job = make_job(db)
        for i in range(5):
            db.create_message(job.id, f"msg-{i}")

        hub = MessageHub(db=db, batch_size=2, poll_seconds=1)
        await hub._poll_cycle()

        # Only 2 consumed per cycle.
        remaining = db.poll_messages()
        assert len(remaining) == 3


class TestMessageToDict:
    def test_serialisation(self, db):
        job = make_job(db)
        msg = db.create_message(job.id, '{"key": "value"}')
        # Re-read to get created_at from DB.
        messages = db.poll_messages()
        d = _message_to_dict(messages[0])

        assert d["message_id"] == msg.id
        assert d["source_job_id"] == job.id
        assert d["payload"] == '{"key": "value"}'
        assert d["created_at"] is not None
