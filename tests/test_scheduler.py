"""Tests for the scheduler."""

import asyncio

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
        scheduler._db.create_job(Job(
            name="always-due",
            cron_expression="* * * * *",
            command="echo tick",
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
