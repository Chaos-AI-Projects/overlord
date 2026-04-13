"""Tests for the structured job output schema and its integration."""

import json

import pytest

from overlord.database import Database
from overlord.executor import run_job
from overlord.message_hub import _message_to_dict
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

    def test_valid_output_with_consumers(self):
        stdout = json.dumps({
            "consumers": ["analytics", "alerts"],
            "message": {"summary": "build passed"},
        })
        output = JobOutput.from_stdout(stdout)
        assert output.consumers == ["analytics", "alerts"]
        assert output.message == {"summary": "build passed"}

    def test_valid_output_with_empty_consumers(self):
        stdout = json.dumps({
            "consumers": [],
            "message": "simple text message",
        })
        output = JobOutput.from_stdout(stdout)
        assert output.consumers == []
        assert output.message == "simple text message"

    def test_valid_output_with_string_message(self):
        stdout = json.dumps({
            "consumers": ["handler"],
            "message": "plain string",
        })
        output = JobOutput.from_stdout(stdout)
        assert output.message == "plain string"

    def test_valid_output_with_dict_message(self):
        stdout = json.dumps({
            "consumers": [],
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

    def test_missing_consumers_field(self):
        with pytest.raises(JobOutputError, match="consumers"):
            JobOutput.from_stdout(json.dumps({"message": "hi"}))

    def test_missing_message_field(self):
        with pytest.raises(JobOutputError, match="message"):
            JobOutput.from_stdout(json.dumps({"consumers": []}))

    def test_consumers_not_a_list(self):
        with pytest.raises(JobOutputError, match="must be a list"):
            JobOutput.from_stdout(json.dumps({
                "consumers": "not-a-list",
                "message": "hi",
            }))

    def test_consumers_element_not_string(self):
        with pytest.raises(JobOutputError, match="must be a string"):
            JobOutput.from_stdout(json.dumps({
                "consumers": [123],
                "message": "hi",
            }))

    def test_message_invalid_type(self):
        with pytest.raises(JobOutputError, match="must be a string or object"):
            JobOutput.from_stdout(json.dumps({
                "consumers": [],
                "message": [1, 2, 3],
            }))

    def test_extra_fields_ignored(self):
        stdout = json.dumps({
            "consumers": ["x"],
            "message": "m",
            "extra": "ignored",
        })
        output = JobOutput.from_stdout(stdout)
        assert output.consumers == ["x"]
        assert output.message == "m"


class TestExecutorStructuredOutput:
    """Integration tests: executor + structured output validation."""

    @pytest.mark.asyncio
    async def test_valid_structured_output_success(self, db):
        output = json.dumps({"consumers": ["watcher"], "message": "done"})
        job = make_job(db, command=f"echo '{output}'")
        record = await run_job(job, db)
        assert record.status == ExecutionStatus.SUCCESS

        messages = db.poll_messages()
        assert len(messages) == 1
        assert messages[0].consumers == ["watcher"]

        payload = json.loads(messages[0].payload)
        assert payload["status"] == "success"
        assert payload["message"] == "done"

    @pytest.mark.asyncio
    async def test_empty_consumers_success(self, db):
        output = json.dumps({"consumers": [], "message": {"key": "val"}})
        job = make_job(db, command=f"echo '{output}'")
        record = await run_job(job, db)
        assert record.status == ExecutionStatus.SUCCESS

        messages = db.poll_messages()
        assert len(messages) == 1
        assert messages[0].consumers == []

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
    async def test_multi_consumer_stored(self, db):
        output = json.dumps({
            "consumers": ["a", "b", "c"],
            "message": "multi",
        })
        job = make_job(db, command=f"echo '{output}'")
        await run_job(job, db)

        messages = db.poll_messages()
        assert messages[0].consumers == ["a", "b", "c"]


class TestDatabaseConsumers:
    """Test the consumers column in the messages table."""

    def test_create_message_with_consumers(self, db):
        job = make_job(db)
        msg = db.create_message(job.id, '{"data": 1}', consumers=["x", "y"])
        assert msg.consumers == ["x", "y"]

    def test_create_message_without_consumers(self, db):
        job = make_job(db)
        msg = db.create_message(job.id, '{"data": 1}')
        assert msg.consumers == []

    def test_poll_returns_consumers(self, db):
        job = make_job(db)
        db.create_message(job.id, '{"data": 1}', consumers=["consumer-a"])

        messages = db.poll_messages()
        assert len(messages) == 1
        assert messages[0].consumers == ["consumer-a"]

    def test_consumers_roundtrip(self, db):
        job = make_job(db)
        db.create_message(job.id, "payload", consumers=["alpha", "beta"])

        messages = db.poll_messages()
        assert messages[0].consumers == ["alpha", "beta"]


class TestMessageHubConsumers:
    """Test that message_to_dict includes consumers."""

    def test_message_to_dict_includes_consumers(self, db):
        job = make_job(db)
        db.create_message(job.id, '{"k": "v"}', consumers=["handler-1"])

        messages = db.poll_messages()
        d = _message_to_dict(messages[0])
        assert d["consumers"] == ["handler-1"]

    def test_message_to_dict_empty_consumers(self, db):
        job = make_job(db)
        db.create_message(job.id, '{"k": "v"}')

        messages = db.poll_messages()
        d = _message_to_dict(messages[0])
        assert d["consumers"] == []
