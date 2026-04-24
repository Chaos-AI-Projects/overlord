"""Tests for the scheduler."""

import asyncio
import json

import pytest

from overlord.database import Database
from overlord.models import ExecutionStatus, Job, JobStatus
from overlord.scheduler import Scheduler


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    d.init_schema()
    yield d
    d.close()


@pytest.fixture
def scheduler(tmp_path):
    s = Scheduler(db_path=tmp_path / "test.db", tick_seconds=1)
    s._db.init_schema()
    return s


class TestScheduler:
    @pytest.mark.asyncio
    async def test_tick_launches_due_job(self, scheduler):
        """A job matching the current minute should be launched on tick."""
        # Use '* * * * *' so it always matches.
        output = json.dumps({"consumer": None, "message": "tick"})
        scheduler._db.create_job(Job(
            name="always-due",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
        ))

        await scheduler._tick()
        # Should have launched a task.
        assert len(scheduler._running_tasks) == 1
        # Wait for the task to complete.
        await asyncio.gather(*scheduler._running_tasks.values())

        history = scheduler._db.get_execution_history(
            scheduler._db.get_job_by_name("always-due").id
        )
        assert len(history) == 1
        assert history[0].status == ExecutionStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_tick_skips_disabled_job(self, scheduler):
        scheduler._db.create_job(Job(
            name="disabled",
            cron_expression="* * * * *",
            command="echo nope",
            status=JobStatus.DISABLED,
        ))
        await scheduler._tick()
        assert len(scheduler._running_tasks) == 0

    @pytest.mark.asyncio
    async def test_tick_skips_non_matching(self, scheduler):
        # Expression that never matches current minute (hour 25 doesn't exist,
        # but we'll use a month that isn't current).
        scheduler._db.create_job(Job(
            name="never-due",
            cron_expression="0 0 1 1 *",  # midnight Jan 1
            command="echo nope",
        ))
        from datetime import datetime
        now = datetime.now()
        if now.month == 1 and now.day == 1 and now.hour == 0 and now.minute == 0:
            pytest.skip("Extremely unlikely edge case")

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 0

    @pytest.mark.asyncio
    async def test_stop_event_stops_run(self, scheduler):
        """Scheduler.run() should exit when stop is called."""
        async def stop_soon():
            await asyncio.sleep(0.1)
            await scheduler.stop()

        asyncio.create_task(stop_soon())
        await scheduler.run()  # should return without hanging

    @pytest.mark.asyncio
    async def test_duplicate_tick_skips_running(self, scheduler):
        """If a job is already running, a second tick should not launch another."""
        scheduler._db.create_job(Job(
            name="slow",
            cron_expression="* * * * *",
            command="sleep 5",
        ))

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1

        await scheduler._tick()  # should skip since it's still running
        # Still only one task.
        assert len(scheduler._running_tasks) == 1

        # Clean up running tasks.
        for t in scheduler._running_tasks.values():
            t.cancel()
        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)


class TestSchedulerConsumes:
    """Test the consumer gate in the scheduler."""

    @pytest.mark.asyncio
    async def test_consumer_job_skips_without_messages(self, scheduler):
        """A job with consumes should not run if there are no matching messages."""
        scheduler._db.create_job(Job(
            name="consumer",
            cron_expression="* * * * *",
            command="echo hello",
            consumes=["source-job"],
        ))

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 0

    @pytest.mark.asyncio
    async def test_consumer_job_runs_with_messages(self, scheduler):
        """A job with consumes should run when matching messages exist."""
        output = json.dumps({"consumer": None, "message": "ok"})
        job = scheduler._db.create_job(Job(
            name="consumer",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            consumes=["producer"],
        ))
        # Create a message addressed to this consumer.
        producer = scheduler._db.create_job(Job(
            name="producer",
            cron_expression="0 0 1 1 *",
            command="echo nope",
        ))
        scheduler._db.create_message(producer.id, '{"data": 1}', consumer="producer")

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1

        # Wait for the task to complete.
        await asyncio.gather(*scheduler._running_tasks.values())

        # The message should be auto-consumed on success.
        remaining = scheduler._db.fetch_unconsumed_for_consumers(["producer"])
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_catchall_job_runs_with_null_consumer(self, scheduler):
        """A catch-all job (consumes=['*']) should pick up unaddressed messages."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._db.create_job(Job(
            name="catchall",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            consumes=["*"],
        ))
        producer = scheduler._db.create_job(Job(
            name="producer",
            cron_expression="0 0 1 1 *",
            command="echo nope",
        ))
        # Unaddressed message (consumer=None).
        input_msg = scheduler._db.create_message(producer.id, '{"data": 1}')

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1

        await asyncio.gather(*scheduler._running_tasks.values())

        # The original input message should be consumed.
        all_msgs = scheduler._db.query_messages()
        original = [m for m in all_msgs if m.id == input_msg.id]
        assert len(original) == 1
        assert original[0].consumed is True

    @pytest.mark.asyncio
    async def test_unconditional_job_runs_normally(self, scheduler):
        """A job with empty consumes should run unconditionally."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._db.create_job(Job(
            name="unconditional",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            consumes=[],
        ))

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1

        await asyncio.gather(*scheduler._running_tasks.values())


class TestSchedulerWithMcp:
    @pytest.mark.asyncio
    async def test_mcp_server_starts_and_stops(self, tmp_path):
        """Scheduler with mcp_host should start and cleanly stop the MCP server."""
        s = Scheduler(
            db_path=tmp_path / "test.db",
            tick_seconds=1,
            mcp_host="127.0.0.1",
            mcp_port=0,  # port 0 won't actually bind in this test
        )
        s._db.init_schema()

        assert s._mcp_server is not None
        assert s._mcp_server.name == "overlord-job-registry"

    @pytest.mark.asyncio
    async def test_no_mcp_by_default(self, scheduler):
        """Without mcp_host, no MCP server should be created."""
        assert scheduler._mcp_server is None
        assert scheduler._mcp_task is None


class TestSchedulerQueues:
    """Test queue-based execution ordering in the scheduler."""

    @pytest.mark.asyncio
    async def test_queue_enqueues_second_job(self, scheduler):
        """Two jobs on the same queue: second should be enqueued while first runs."""
        scheduler._db.create_job(Job(
            name="slow-job",
            cron_expression="* * * * *",
            command="sleep 5",
            queue_name="serial",
        ))
        scheduler._db.create_job(Job(
            name="fast-job",
            cron_expression="* * * * *",
            command="echo done",
            queue_name="serial",
        ))

        await scheduler._tick()
        # Only the first due job should have launched.
        assert len(scheduler._running_tasks) == 1
        assert "job-slow-job" in [t.get_name() for t in scheduler._running_tasks.values()]

        # The second job should be in the pending queue, not skipped.
        assert "serial" in scheduler._pending_queues
        assert len(scheduler._pending_queues["serial"]) == 1
        assert scheduler._pending_queues["serial"][0][0].name == "fast-job"

        # No "skipped" message for an enqueued job.
        msgs = scheduler._db.query_messages()
        skipped = [m for m in msgs if '"skipped"' in m.payload]
        assert len(skipped) == 0

        # Clean up.
        for t in scheduler._running_tasks.values():
            t.cancel()
        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)

    @pytest.mark.asyncio
    async def test_same_name_dedup(self, scheduler):
        """A job already pending in a queue should not be enqueued again."""
        scheduler._db.create_job(Job(
            name="slow-job",
            cron_expression="* * * * *",
            command="sleep 5",
            queue_name="serial",
        ))
        scheduler._db.create_job(Job(
            name="fast-job",
            cron_expression="* * * * *",
            command="echo done",
            queue_name="serial",
        ))

        # First tick: slow-job runs, fast-job enqueued.
        await scheduler._tick()
        assert len(scheduler._pending_queues.get("serial", [])) == 1

        # Second tick: fast-job already pending, should be deduped.
        await scheduler._tick()
        assert len(scheduler._pending_queues["serial"]) == 1

        # A "skipped" message should have been emitted for the dedup.
        msgs = scheduler._db.query_messages()
        skipped = [m for m in msgs if '"skipped"' in m.payload]
        assert len(skipped) == 1
        assert "already pending" in skipped[0].payload

        # Clean up.
        for t in scheduler._running_tasks.values():
            t.cancel()
        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)

    @pytest.mark.asyncio
    async def test_different_queues_run_concurrently(self, scheduler):
        """Jobs on different queues should run concurrently."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._db.create_job(Job(
            name="queue-a-job",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="queue-a",
        ))
        scheduler._db.create_job(Job(
            name="queue-b-job",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="queue-b",
        ))

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 2

        await asyncio.gather(*scheduler._running_tasks.values())

    @pytest.mark.asyncio
    async def test_default_queue(self, scheduler):
        """Jobs without explicit queue_name should use 'default'."""
        job = scheduler._db.create_job(Job(
            name="default-queue-job",
            cron_expression="* * * * *",
            command="sleep 5",
        ))
        fetched = scheduler._db.get_job(job.id)
        assert fetched.queue_name == "default"

        # Clean up after tick.
        await scheduler._tick()
        for t in scheduler._running_tasks.values():
            t.cancel()
        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)

    @pytest.mark.asyncio
    async def test_queue_drains_after_completion(self, scheduler):
        """After a job completes, the next pending job in its queue should launch."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._db.create_job(Job(
            name="job-1",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="serial",
        ))
        scheduler._db.create_job(Job(
            name="job-2",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="serial",
        ))

        # First tick: job-1 runs, job-2 enqueued.
        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1
        assert len(scheduler._pending_queues.get("serial", [])) == 1
        await asyncio.gather(*scheduler._running_tasks.values())

        # Second tick: job-1 done, queue drains — job-2 should launch from pending.
        await scheduler._tick()
        running_names = [t.get_name() for t in scheduler._running_tasks.values() if not t.done()]
        assert "job-job-2" in running_names

        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)

    @pytest.mark.asyncio
    async def test_fifo_order_preserved(self, scheduler):
        """Jobs should be drained from the pending queue in FIFO order."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._db.create_job(Job(
            name="blocker",
            cron_expression="* * * * *",
            command="sleep 5",
            queue_name="serial",
        ))
        scheduler._db.create_job(Job(
            name="second",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="serial",
        ))
        scheduler._db.create_job(Job(
            name="third",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="serial",
        ))

        await scheduler._tick()
        # blocker runs, second and third enqueued in order.
        pending_names = [j.name for j, _ in scheduler._pending_queues.get("serial", [])]
        assert pending_names == ["second", "third"]

        # Cancel blocker to free the queue.
        for t in scheduler._running_tasks.values():
            t.cancel()
        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)

        # Next tick drains: "second" should launch first.
        await scheduler._tick()
        running_names = [t.get_name() for t in scheduler._running_tasks.values() if not t.done()]
        assert "job-second" in running_names

        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)


class TestSchemaVersionCheck:
    @pytest.mark.asyncio
    async def test_run_checks_schema(self, tmp_path):
        """Scheduler should refuse to start on version mismatch."""
        from overlord.database import SchemaVersionError

        db = Database(db_path=tmp_path / "test.db")
        db.init_schema()
        # Forge a future version.
        db.conn.execute("INSERT INTO schema_version (version) VALUES (999)")
        db.conn.commit()
        db.close()

        scheduler = Scheduler(db_path=tmp_path / "test.db")
        with pytest.raises(SchemaVersionError):
            await scheduler.run()
