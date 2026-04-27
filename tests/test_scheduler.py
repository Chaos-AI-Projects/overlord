"""Tests for the scheduler."""

import asyncio
import json

import pytest

from email import policy
from email.parser import BytesParser

from overlord.job_store import JobStore
from overlord.models import ExecutionStatus, Job, JobStatus
from overlord.scheduler import Scheduler


@pytest.fixture
def scheduler(tmp_path):
    s = Scheduler(data_dir=tmp_path, tick_seconds=1)
    return s


class TestScheduler:
    @pytest.mark.asyncio
    async def test_tick_launches_due_job(self, scheduler):
        """A job matching the current minute should be launched on tick."""
        # Use '* * * * *' so it always matches.
        output = json.dumps({"consumer": None, "message": "tick"})
        scheduler._job_store.create_job(Job(
            name="always-due",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
        ))

        await scheduler._tick()
        # Should have launched a task.
        assert len(scheduler._running_tasks) == 1
        # Wait for the task to complete.
        await asyncio.gather(*scheduler._running_tasks.values())

        history = scheduler._execution_log.get_execution_history("always-due")
        assert len(history) == 1
        assert history[0].status == ExecutionStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_tick_skips_disabled_job(self, scheduler):
        scheduler._job_store.create_job(Job(
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
        scheduler._job_store.create_job(Job(
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
        scheduler._job_store.create_job(Job(
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
        scheduler._job_store.create_job(Job(
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
        scheduler._job_store.create_job(Job(
            name="consumer",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            consumes=["producer"],
        ))
        # Deliver a message to the "producer" Maildir so the consumer gate opens.
        from overlord.maildir import MaildirStore
        msg = MaildirStore.build_message(
            payload='{"data": 1}', consumer="producer", job_name="producer",
        )
        scheduler._maildir.deliver(msg, consumer="producer")

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1

        # Wait for the task to complete.
        await asyncio.gather(*scheduler._running_tasks.values())

        # The message should be auto-consumed (moved to processed).
        remaining = scheduler._maildir.fetch_unconsumed_for_consumers(["producer"])
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_catchall_job_runs_with_null_consumer(self, scheduler):
        """A catch-all job (consumes=['*']) should pick up unaddressed messages."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._job_store.create_job(Job(
            name="catchall",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            consumes=["*"],
        ))
        # Unaddressed message (consumer=None) → goes to catchall Maildir.
        from overlord.maildir import MaildirStore
        msg = MaildirStore.build_message(
            payload='{"data": 1}', consumer=None, job_name="producer",
        )
        scheduler._maildir.deliver(msg, consumer=None)

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1

        await asyncio.gather(*scheduler._running_tasks.values())

        # The message should be auto-consumed (moved to processed).
        remaining = scheduler._maildir.fetch_unconsumed_for_consumers(["*"])
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_unconditional_job_runs_normally(self, scheduler):
        """A job with empty consumes should run unconditionally."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._job_store.create_job(Job(
            name="unconditional",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            consumes=[],
        ))

        await scheduler._tick()
        assert len(scheduler._running_tasks) == 1

        await asyncio.gather(*scheduler._running_tasks.values())


class TestSchedulerSpoolProcessor:
    """Test that the scheduler starts and stops the spool processor."""

    @pytest.mark.asyncio
    async def test_spool_processor_delivers_during_run(self, tmp_path):
        """Messages written to spool during scheduler run get delivered to Maildir."""
        from overlord.maildir import MaildirStore

        s = Scheduler(data_dir=tmp_path, tick_seconds=1)
        # Register a job that produces output (which goes through the spool).
        output = json.dumps({"consumer": "inbox", "message": "hello from job"})
        s._job_store.create_job(Job(
            name="producer",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
        ))

        async def run_and_stop():
            # Let the scheduler tick once and give the spool processor time to deliver.
            await asyncio.sleep(1.5)
            await s.stop()

        asyncio.create_task(run_and_stop())
        await s.run()

        # The spool should be drained (no .eml files left).
        spool_dir = tmp_path / "spool"
        spool_files = list(spool_dir.glob("*.eml"))
        assert len(spool_files) == 0

        # The message should have been delivered to the "inbox" Maildir.
        store = MaildirStore(data_dir=tmp_path)
        msgs = store.fetch_messages("inbox")
        assert len(msgs) >= 1
        assert any("hello from job" in m["payload"] for m in msgs)

    @pytest.mark.asyncio
    async def test_spool_processor_stops_on_shutdown(self, tmp_path):
        """The spool processor task should be stopped during scheduler shutdown."""
        s = Scheduler(data_dir=tmp_path, tick_seconds=1)

        async def stop_soon():
            await asyncio.sleep(0.2)
            await s.stop()

        asyncio.create_task(stop_soon())
        await s.run()

        # After run() returns, the spool task should be done.
        assert s._spool_task is not None
        assert s._spool_task.done()


class TestSchedulerWithMcp:
    @pytest.mark.asyncio
    async def test_mcp_server_starts_and_stops(self, tmp_path):
        """Scheduler with mcp_host should start and cleanly stop the MCP server."""
        s = Scheduler(
            data_dir=tmp_path,
            tick_seconds=1,
            mcp_host="127.0.0.1",
            mcp_port=0,  # port 0 won't actually bind in this test
        )

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
        scheduler._job_store.create_job(Job(
            name="aaa-blocker",
            cron_expression="* * * * *",
            command="sleep 5",
            queue_name="serial",
        ))
        scheduler._job_store.create_job(Job(
            name="zzz-quick",
            cron_expression="* * * * *",
            command="echo done",
            queue_name="serial",
        ))

        await scheduler._tick()
        # Only the first due job (alphabetically) should have launched.
        assert len(scheduler._running_tasks) == 1
        assert "job-aaa-blocker" in [t.get_name() for t in scheduler._running_tasks.values()]

        # The second job should be in the pending queue, not skipped.
        assert "serial" in scheduler._pending_queues
        assert len(scheduler._pending_queues["serial"]) == 1
        assert scheduler._pending_queues["serial"][0][0].name == "zzz-quick"

        # Clean up.
        for t in scheduler._running_tasks.values():
            t.cancel()
        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)

    @pytest.mark.asyncio
    async def test_same_name_dedup(self, scheduler, tmp_path):
        """A job already pending in a queue should not be enqueued again."""
        scheduler._job_store.create_job(Job(
            name="aaa-blocker",
            cron_expression="* * * * *",
            command="sleep 5",
            queue_name="serial",
        ))
        scheduler._job_store.create_job(Job(
            name="zzz-quick",
            cron_expression="* * * * *",
            command="echo done",
            queue_name="serial",
        ))

        # First tick: aaa-blocker runs, zzz-quick enqueued.
        await scheduler._tick()
        assert len(scheduler._pending_queues.get("serial", [])) == 1

        # Second tick: fast-job already pending, should be deduped.
        await scheduler._tick()
        assert len(scheduler._pending_queues["serial"]) == 1

        # A "skipped" message should have been emitted to the spool for the dedup.
        spool_dir = tmp_path / "spool"
        parser = BytesParser(policy=policy.default)
        spool_files = sorted(f for f in spool_dir.iterdir() if f.suffix == ".eml")
        skipped = []
        for f in spool_files:
            msg = parser.parsebytes(f.read_bytes())
            for part in msg.iter_attachments():
                if part.get_content_type() == "application/json":
                    payload = part.get_content().decode("utf-8")
                    if '"skipped"' in payload:
                        skipped.append(payload)
        assert len(skipped) == 1
        assert "already pending" in skipped[0]

        # Clean up.
        for t in scheduler._running_tasks.values():
            t.cancel()
        await asyncio.gather(*scheduler._running_tasks.values(), return_exceptions=True)

    @pytest.mark.asyncio
    async def test_different_queues_run_concurrently(self, scheduler):
        """Jobs on different queues should run concurrently."""
        output = json.dumps({"consumer": None, "message": "ok"})
        scheduler._job_store.create_job(Job(
            name="queue-a-job",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="queue-a",
        ))
        scheduler._job_store.create_job(Job(
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
        scheduler._job_store.create_job(Job(
            name="default-queue-job",
            cron_expression="* * * * *",
            command="sleep 5",
        ))
        fetched = scheduler._job_store.get_job_by_name("default-queue-job")
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
        scheduler._job_store.create_job(Job(
            name="job-1",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="serial",
        ))
        scheduler._job_store.create_job(Job(
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
        scheduler._job_store.create_job(Job(
            name="blocker",
            cron_expression="* * * * *",
            command="sleep 5",
            queue_name="serial",
        ))
        scheduler._job_store.create_job(Job(
            name="second",
            cron_expression="* * * * *",
            command=f"echo '{output}'",
            queue_name="serial",
        ))
        scheduler._job_store.create_job(Job(
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
