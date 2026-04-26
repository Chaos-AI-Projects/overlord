"""Tests for the structured job output schema and its integration."""

import json

import pytest

from email import policy
from email.parser import BytesParser

from overlord.execution_log import ExecutionLog
from overlord.executor import run_job
from overlord.lock_store import LockStore
from overlord.models import (
    ExecutionStatus,
    Job,
    JobOutput,
    JobOutputError,
)
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
    """Read all .eml files from the spool directory, return (headers, payload) tuples."""
    spool_dir = tmp_path / "spool"
    if not spool_dir.exists():
        return []
    parser = BytesParser(policy=policy.default)
    results = []
    for f in sorted(spool_dir.iterdir()):
        if f.suffix == ".eml":
            msg = parser.parsebytes(f.read_bytes())
            # Extract JSON payload from the attachment.
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
    async def test_valid_structured_output_success(self, execution_log, lock_store, spool, tmp_path):
        output = json.dumps({"consumer": "watcher", "message": "done"})
        job = make_job(command=f"echo '{output}'")
        record = await run_job(job, execution_log, lock_store, spool)
        assert record.status == ExecutionStatus.SUCCESS

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1
        assert messages[0]["consumer"] == "watcher"

        payload = json.loads(messages[0]["payload"])
        assert payload["status"] == "success"
        assert payload["message"] == "done"

    @pytest.mark.asyncio
    async def test_null_consumer_success(self, execution_log, lock_store, spool, tmp_path):
        output = json.dumps({"consumer": None, "message": {"key": "val"}})
        job = make_job(command=f"echo '{output}'")
        record = await run_job(job, execution_log, lock_store, spool)
        assert record.status == ExecutionStatus.SUCCESS

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1
        assert messages[0]["consumer"] is None

    @pytest.mark.asyncio
    async def test_invalid_output_marks_execution_failed(self, execution_log, lock_store, spool, tmp_path):
        """A job that exits 0 but produces non-schema stdout is marked FAILED."""
        job = make_job(command="echo 'not json'")
        record = await run_job(job, execution_log, lock_store, spool)

        # The execution should be retroactively marked as failed.
        reloaded = execution_log.get_execution(record.id)
        assert reloaded.status == ExecutionStatus.FAILED
        assert "schema validation failed" in reloaded.stderr.lower()

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1
        payload = json.loads(messages[0]["payload"])
        assert payload["status"] == "failed"
        assert "error" in payload

    @pytest.mark.asyncio
    async def test_plain_text_output_marks_failed(self, execution_log, lock_store, spool):
        job = make_job(command="echo hello world")
        record = await run_job(job, execution_log, lock_store, spool)

        reloaded = execution_log.get_execution(record.id)
        assert reloaded.status == ExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_failed_job_no_schema_validation(self, execution_log, lock_store, spool, tmp_path):
        """A job that exits non-zero does NOT get schema-validated."""
        job = make_job(command="echo 'raw output' && exit 1")
        record = await run_job(job, execution_log, lock_store, spool)
        assert record.status == ExecutionStatus.FAILED

        messages = read_spool_messages(tmp_path)
        assert len(messages) == 1
        payload = json.loads(messages[0]["payload"])
        assert payload["status"] == "failed"
        assert payload["stdout"] == "raw output\n"

    @pytest.mark.asyncio
    async def test_consumer_stored(self, execution_log, lock_store, spool, tmp_path):
        output = json.dumps({
            "consumer": "target-job",
            "message": "routed",
        })
        job = make_job(command=f"echo '{output}'")
        await run_job(job, execution_log, lock_store, spool)

        messages = read_spool_messages(tmp_path)
        assert messages[0]["consumer"] == "target-job"
