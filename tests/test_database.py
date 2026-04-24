"""Tests for the Overlord database layer."""

import tempfile
from pathlib import Path

import pytest

from overlord.database import Database
from overlord.models import ExecutionStatus, Job, JobStatus


@pytest.fixture
def db(tmp_path):
    """Create a temporary database for testing."""
    db = Database(db_path=tmp_path / "test.db")
    db.init_schema()
    yield db
    db.close()


def make_job(**kwargs) -> Job:
    defaults = dict(
        name="test-job",
        cron_expression="*/5 * * * *",
        command="echo hello",
    )
    defaults.update(kwargs)
    return Job(**defaults)


class TestJobCRUD:
    def test_create_and_get(self, db):
        job = db.create_job(make_job())
        assert job.id is not None

        fetched = db.get_job(job.id)
        assert fetched is not None
        assert fetched.name == "test-job"
        assert fetched.cron_expression == "*/5 * * * *"
        assert fetched.command == "echo hello"
        assert fetched.status == JobStatus.ENABLED
        assert fetched.consumes == []

    def test_get_by_name(self, db):
        db.create_job(make_job())
        fetched = db.get_job_by_name("test-job")
        assert fetched is not None
        assert fetched.name == "test-job"

    def test_get_nonexistent(self, db):
        assert db.get_job(999) is None
        assert db.get_job_by_name("nope") is None

    def test_list_jobs(self, db):
        db.create_job(make_job(name="job-1"))
        db.create_job(make_job(name="job-2", status=JobStatus.DISABLED))
        db.create_job(make_job(name="job-3"))

        all_jobs = db.list_jobs()
        assert len(all_jobs) == 3

        enabled = db.list_jobs(status=JobStatus.ENABLED)
        assert len(enabled) == 2

        disabled = db.list_jobs(status=JobStatus.DISABLED)
        assert len(disabled) == 1
        assert disabled[0].name == "job-2"

    def test_update_job(self, db):
        job = db.create_job(make_job())
        job.command = "echo updated"
        job.status = JobStatus.PAUSED
        db.update_job(job)

        fetched = db.get_job(job.id)
        assert fetched.command == "echo updated"
        assert fetched.status == JobStatus.PAUSED

    def test_delete_job(self, db):
        job = db.create_job(make_job())
        db.delete_job(job.id)
        assert db.get_job(job.id) is None

    def test_unique_name_constraint(self, db):
        db.create_job(make_job(name="unique"))
        with pytest.raises(Exception):
            db.create_job(make_job(name="unique"))

    def test_exclusive_lock_field(self, db):
        job = db.create_job(make_job(exclusive_lock="deploy-lock"))
        fetched = db.get_job(job.id)
        assert fetched.exclusive_lock == "deploy-lock"

    def test_consumes_field(self, db):
        job = db.create_job(make_job(consumes=["job-a", "job-b"]))
        fetched = db.get_job(job.id)
        assert fetched.consumes == ["job-a", "job-b"]

    def test_consumes_default_empty(self, db):
        job = db.create_job(make_job())
        fetched = db.get_job(job.id)
        assert fetched.consumes == []

    def test_update_consumes(self, db):
        job = db.create_job(make_job())
        job.consumes = ["*"]
        db.update_job(job)
        fetched = db.get_job(job.id)
        assert fetched.consumes == ["*"]

    def test_queue_name_default(self, db):
        job = db.create_job(make_job())
        fetched = db.get_job(job.id)
        assert fetched.queue_name == "default"

    def test_queue_name_custom(self, db):
        job = db.create_job(make_job(queue_name="serial"))
        fetched = db.get_job(job.id)
        assert fetched.queue_name == "serial"

    def test_update_queue_name(self, db):
        job = db.create_job(make_job())
        job.queue_name = "priority"
        db.update_job(job)
        fetched = db.get_job(job.id)
        assert fetched.queue_name == "priority"


class TestExecutionHistory:
    def test_create_and_finish(self, db):
        job = db.create_job(make_job())
        execution = db.create_execution(job.id)
        assert execution.id is not None
        assert execution.status == ExecutionStatus.RUNNING

        db.finish_execution(execution.id, ExecutionStatus.SUCCESS, exit_code=0,
                            stdout="hello\n", stderr="")

        history = db.get_execution_history(job.id)
        assert len(history) == 1
        assert history[0].status == ExecutionStatus.SUCCESS
        assert history[0].exit_code == 0
        assert history[0].stdout == "hello\n"

    def test_execution_history_ordering(self, db):
        job = db.create_job(make_job())
        e1 = db.create_execution(job.id)
        db.finish_execution(e1.id, ExecutionStatus.SUCCESS)
        e2 = db.create_execution(job.id)
        db.finish_execution(e2.id, ExecutionStatus.FAILED)

        history = db.get_execution_history(job.id)
        assert len(history) == 2
        # Most recent first
        assert history[0].id == e2.id

    def test_history_limit(self, db):
        job = db.create_job(make_job())
        for _ in range(5):
            e = db.create_execution(job.id)
            db.finish_execution(e.id, ExecutionStatus.SUCCESS)

        history = db.get_execution_history(job.id, limit=3)
        assert len(history) == 3


class TestMessages:
    def test_create_and_poll(self, db):
        job = db.create_job(make_job())
        msg = db.create_message(job.id, '{"event": "build_done"}')
        assert msg.id is not None

        messages = db.poll_messages()
        assert len(messages) == 1
        assert messages[0].payload == '{"event": "build_done"}'
        assert messages[0].consumed is False

    def test_mark_consumed(self, db):
        job = db.create_job(make_job())
        msg = db.create_message(job.id, "test payload")
        db.mark_consumed(msg.id)

        messages = db.poll_messages()
        assert len(messages) == 0

    def test_poll_ordering(self, db):
        job = db.create_job(make_job())
        db.create_message(job.id, "first")
        db.create_message(job.id, "second")

        messages = db.poll_messages()
        assert messages[0].payload == "first"
        assert messages[1].payload == "second"


class TestLocks:
    def test_acquire_and_release(self, db):
        job = db.create_job(make_job())
        execution = db.create_execution(job.id)

        assert db.acquire_lock("deploy", execution.id) is True
        lock = db.get_lock("deploy")
        assert lock is not None
        assert lock.holder_execution_id == execution.id

        db.release_lock("deploy")
        assert db.get_lock("deploy") is None

    def test_acquire_already_held(self, db):
        job = db.create_job(make_job())
        e1 = db.create_execution(job.id)
        e2 = db.create_execution(job.id)

        assert db.acquire_lock("deploy", e1.id) is True
        assert db.acquire_lock("deploy", e2.id) is False

    def test_release_stale_locks(self, db):
        job = db.create_job(make_job())
        execution = db.create_execution(job.id)
        db.acquire_lock("stale-lock", execution.id)

        # Finish the execution so its lock becomes stale
        db.finish_execution(execution.id, ExecutionStatus.SUCCESS)

        released = db.release_stale_locks()
        assert released == 1
        assert db.get_lock("stale-lock") is None

    def test_release_stale_keeps_active(self, db):
        job = db.create_job(make_job())
        e_running = db.create_execution(job.id)
        e_done = db.create_execution(job.id)

        db.acquire_lock("active", e_running.id)
        db.acquire_lock("stale", e_done.id)
        db.finish_execution(e_done.id, ExecutionStatus.FAILED)

        released = db.release_stale_locks()
        assert released == 1
        assert db.get_lock("active") is not None
        assert db.get_lock("stale") is None


class TestFailRunningExecutions:
    def test_marks_running_as_failed(self, db):
        job = db.create_job(make_job())
        e1 = db.create_execution(job.id)
        e2 = db.create_execution(job.id)

        count = db.fail_running_executions()
        assert count == 2

        rec1 = db.get_execution(e1.id)
        rec2 = db.get_execution(e2.id)
        assert rec1.status == ExecutionStatus.FAILED
        assert rec2.status == ExecutionStatus.FAILED
        assert "scheduler restarted" in rec1.stderr

    def test_does_not_affect_finished_executions(self, db):
        job = db.create_job(make_job())
        e_ok = db.create_execution(job.id)
        db.finish_execution(e_ok.id, ExecutionStatus.SUCCESS)
        e_fail = db.create_execution(job.id)
        db.finish_execution(e_fail.id, ExecutionStatus.FAILED)

        count = db.fail_running_executions()
        assert count == 0

        assert db.get_execution(e_ok.id).status == ExecutionStatus.SUCCESS
        assert db.get_execution(e_fail.id).status == ExecutionStatus.FAILED

    def test_fail_then_release_cleans_orphaned_locks(self, db):
        """Simulates unclean shutdown: execution still RUNNING with held lock."""
        job = db.create_job(make_job())
        execution = db.create_execution(job.id)
        db.acquire_lock("orphaned", execution.id)

        # Before fix: release_stale_locks alone would NOT clear this
        assert db.get_lock("orphaned") is not None

        # The startup sequence: fail running, then release stale locks
        db.fail_running_executions()
        released = db.release_stale_locks()

        assert released == 1
        assert db.get_lock("orphaned") is None


class TestQueryMessages:
    def test_query_all(self, db):
        job = db.create_job(make_job())
        db.create_message(job.id, "msg1")
        db.create_message(job.id, "msg2")
        results = db.query_messages()
        assert len(results) == 2

    def test_query_by_job_name(self, db):
        j1 = db.create_job(make_job(name="job-a"))
        j2 = db.create_job(make_job(name="job-b"))
        db.create_message(j1.id, "from-a")
        db.create_message(j2.id, "from-b")
        results = db.query_messages(source_job_name="job-a")
        assert len(results) == 1
        assert results[0].payload == "from-a"

    def test_query_by_consumer(self, db):
        job = db.create_job(make_job())
        db.create_message(job.id, "for-agent", consumer="agent")
        db.create_message(job.id, "for-logger", consumer="logger")
        results = db.query_messages(consumer="agent")
        assert len(results) == 1
        assert results[0].payload == "for-agent"

    def test_query_by_consumed(self, db):
        job = db.create_job(make_job())
        m1 = db.create_message(job.id, "consumed")
        db.create_message(job.id, "unconsumed")
        db.mark_consumed(m1.id)
        unconsumed = db.query_messages(consumed=False)
        assert len(unconsumed) == 1
        assert unconsumed[0].payload == "unconsumed"
        consumed = db.query_messages(consumed=True)
        assert len(consumed) == 1
        assert consumed[0].payload == "consumed"

    def test_query_combined_filters(self, db):
        j1 = db.create_job(make_job(name="job-x"))
        j2 = db.create_job(make_job(name="job-y"))
        db.create_message(j1.id, "match", consumer="agent")
        db.create_message(j1.id, "wrong-consumer", consumer="logger")
        db.create_message(j2.id, "wrong-job", consumer="agent")
        results = db.query_messages(source_job_name="job-x", consumer="agent")
        assert len(results) == 1
        assert results[0].payload == "match"

    def test_query_limit(self, db):
        job = db.create_job(make_job())
        for i in range(5):
            db.create_message(job.id, f"msg-{i}")
        results = db.query_messages(limit=3)
        assert len(results) == 3

    def test_query_nonexistent_job(self, db):
        results = db.query_messages(source_job_name="ghost")
        assert len(results) == 0

    def test_query_no_consumer(self, db):
        job = db.create_job(make_job())
        db.create_message(job.id, "unaddressed")
        db.create_message(job.id, "addressed", consumer="agent")
        results = db.query_messages(no_consumer=True)
        assert len(results) == 1
        assert results[0].consumer is None

    def test_query_no_consumer_with_other_filters(self, db):
        j1 = db.create_job(make_job(name="job-a"))
        j2 = db.create_job(make_job(name="job-b"))
        db.create_message(j1.id, "match")
        db.create_message(j1.id, "has-consumer", consumer="x")
        db.create_message(j2.id, "wrong-job")
        results = db.query_messages(source_job_name="job-a", no_consumer=True)
        assert len(results) == 1
        assert results[0].payload == "match"


class TestCascadeDeletes:
    def test_delete_job_cascades_execution(self, db):
        job = db.create_job(make_job())
        db.create_execution(job.id)
        db.delete_job(job.id)

        history = db.get_execution_history(job.id)
        assert len(history) == 0

    def test_delete_job_cascades_messages(self, db):
        job = db.create_job(make_job())
        db.create_message(job.id, "will be deleted")
        db.delete_job(job.id)

        messages = db.poll_messages()
        assert len(messages) == 0
