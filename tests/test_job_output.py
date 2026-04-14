"""Tests for the structured job output schema and its integration."""

import json

import pytest

from overlord.database import Database
from overlord.executor import run_job
from overlord.models import (
    ExecutionStatus,
    Job,
    JobOutput,
    JobOutputError,
)


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


class TestJobOutputParsing:
    """Unit tests for JobOutput.from_stdout()."""

    def test_valid_output_with_consumer(self):
        stdout = json.dumps({
            "consumer": "analytics",
            "message": {"summary": "build passed"},
        })
        output = JobOutput.from_stdout(stdout)
        assert output.consumer == "analytics"
        assert output.message == {"summary": "build passed"}

    def test_valid_output_with_null_consumer(self):
        stdout = json.dumps({
            "consumer": None,
            "message": "simple text message",
        })
        output = JobOutput.from_stdout(stdout)
        assert output.consumer is None
        assert output.message == "simple text message"

    def test_valid_output_without_consumer_field(self):
        stdout = json.dumps({
            "message": "no consumer specified",
        })
        output = JobOutput.from_stdout(stdout)
        assert output.consumer is None
        assert output.message == "no consumer specified"

    def test_valid_output_with_string_message(self):
        stdout = json.dumps({
            "consumer": "handler",
            "message": "plain string",
        })
        output = JobOutput.from_stdout(stdout)
        assert output.message == "plain string"

    def test_valid_output_with_dict_message(self):
        stdout = json.dumps({
            "message": {"key": "value", "nested": {"a": 1}},
        })
        output = JobOutput.from_stdout(stdout)
        assert output.message["key"] == "value"

    def test_empty_stdout_fails(self):
        with pytest.raises(JobOutputError, match="no output"):
            JobOutput.from_stdout("")

    def test_whitespace_only_fails(self):
        with pytest.raises(JobOutputError, match="no output"):
            JobOutput.from_stdout("   \n  ")

    def test_invalid_json_fails(self):
        with pytest.raises(JobOutputError, match="not valid JSON"):
            JobOutput.from_stdout("not json at all")

    def test_json_array_fails(self):
        with pytest.raises(JobOutputError, match="must be a JSON object"):
            JobOutput.from_stdout("[1, 2, 3]")

    def test_missing_message_field(self):
        with pytest.raises(JobOutputError, match="message"):
            JobOutput.from_stdout(json.dumps({"consumer": "x"}))

    def test_consumer_not_a_string(self):
        with pytest.raises(JobOutputError, match="must be a string or null"):
            JobOutput.from_stdout(json.dumps({
                "consumer": 123,
                "message": "hi",
            }))

    def test_message_invalid_type(self):
        with pytest.raises(JobOutputError, match="must be a string or object"):
            JobOutput.from_stdout(json.dumps({
                "consumer": None,
                "message": [1, 2, 3],
            }))

    def test_extra_fields_ignored(self):
        stdout = json.dumps({
            "consumer": "x",
            "message": "m",
            "extra": "ignored",
        })
        output = JobOutput.from_stdout(stdout)
        assert output.consumer == "x"
        assert output.message == "m"


class TestExecutorStructuredOutput:
    """Integration tests: executor + structured output validation."""

    @pytest.mark.asyncio
    async def test_valid_structured_output_success(self, db):
        output = json.dumps({"consumer": "watcher", "message": "done"})
        job = make_job(db, command=f"echo '{output}'")
        record = await run_job(job, db)
        assert record.status == ExecutionStatus.SUCCESS

        messages = db.poll_messages()
        assert len(messages) == 1
        assert messages[0].consumer == "watcher"

        payload = json.loads(messages[0].payload)
        assert payload["status"] == "success"
        assert payload["message"] == "done"

    @pytest.mark.asyncio
    async def test_null_consumer_success(self, db):
        output = json.dumps({"consumer": None, "message": {"key": "val"}})
        job = make_job(db, command=f"echo '{output}'")
        record = await run_job(job, db)
        assert record.status == ExecutionStatus.SUCCESS

        messages = db.poll_messages()
        assert len(messages) == 1
        assert messages[0].consumer is None

    @pytest.mark.asyncio
    async def test_invalid_output_marks_execution_failed(self, db):
        """A job that exits 0 but produces non-schema stdout is marked FAILED."""
        job = make_job(db, command="echo 'not json'")
        record = await run_job(job, db)

        # The execution should be retroactively marked as failed.
        reloaded = db.get_execution(record.id)
        assert reloaded.status == ExecutionStatus.FAILED
        assert "schema validation failed" in reloaded.stderr.lower()

        messages = db.poll_messages()
        assert len(messages) == 1
        payload = json.loads(messages[0].payload)
        assert payload["status"] == "failed"
        assert "error" in payload

    @pytest.mark.asyncio
    async def test_plain_text_output_marks_failed(self, db):
        job = make_job(db, command="echo hello world")
        record = await run_job(job, db)

        reloaded = db.get_execution(record.id)
        assert reloaded.status == ExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_failed_job_no_schema_validation(self, db):
        """A job that exits non-zero does NOT get schema-validated."""
        job = make_job(db, command="echo 'raw output' && exit 1")
        record = await run_job(job, db)
        assert record.status == ExecutionStatus.FAILED

        messages = db.poll_messages()
        assert len(messages) == 1
        payload = json.loads(messages[0].payload)
        assert payload["status"] == "failed"
        assert payload["stdout"] == "raw output\n"

    @pytest.mark.asyncio
    async def test_consumer_stored(self, db):
        output = json.dumps({
            "consumer": "target-job",
            "message": "routed",
        })
        job = make_job(db, command=f"echo '{output}'")
        await run_job(job, db)

        messages = db.poll_messages()
        assert messages[0].consumer == "target-job"


class TestDatabaseConsumer:
    """Test the consumer column in the messages table."""

    def test_create_message_with_consumer(self, db):
        job = make_job(db)
        msg = db.create_message(job.id, '{"data": 1}', consumer="x")
        assert msg.consumer == "x"

    def test_create_message_without_consumer(self, db):
        job = make_job(db)
        msg = db.create_message(job.id, '{"data": 1}')
        assert msg.consumer is None

    def test_poll_returns_consumer(self, db):
        job = make_job(db)
        db.create_message(job.id, '{"data": 1}', consumer="consumer-a")

        messages = db.poll_messages()
        assert len(messages) == 1
        assert messages[0].consumer == "consumer-a"

    def test_consumer_roundtrip(self, db):
        job = make_job(db)
        db.create_message(job.id, "payload", consumer="alpha")

        messages = db.poll_messages()
        assert messages[0].consumer == "alpha"

    def test_null_consumer_roundtrip(self, db):
        job = make_job(db)
        db.create_message(job.id, "payload")

        messages = db.poll_messages()
        assert messages[0].consumer is None
