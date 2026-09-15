"""Tests for the JSON-lines execution history log."""

import json
import threading

import pytest

from overlord import execution_log
from overlord.execution_log import ExecutionLog
from overlord.models import ExecutionRecord, ExecutionStatus


@pytest.fixture
def log(tmp_path):
    """Create an ExecutionLog backed by a temporary directory."""
    return ExecutionLog(data_dir=tmp_path)


class TestCreateExecution:
    def test_returns_record_with_id(self, log):
        rec = log.create_execution("my-job")
        assert rec.id == 1
        assert rec.job_name == "my-job"
        assert rec.status == ExecutionStatus.RUNNING
        assert rec.started_at is not None

    def test_ids_are_monotonic(self, log):
        r1 = log.create_execution("job-a")
        r2 = log.create_execution("job-b")
        r3 = log.create_execution("job-a")
        assert r1.id == 1
        assert r2.id == 2
        assert r3.id == 3

    def test_appends_to_log_file(self, log, tmp_path):
        log.create_execution("my-job")
        log_path = tmp_path / "execution.log"
        assert log_path.exists()
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["job_name"] == "my-job"
        assert data["status"] == "running"


class TestFinishExecution:
    def test_finish_appends_completion_line(self, log, tmp_path):
        rec = log.create_execution("my-job")
        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0,
                             stdout='{"consumer":null,"message":"ok"}')
        log_path = tmp_path / "execution.log"
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 2
        completion = json.loads(lines[1])
        assert completion["status"] == "success"
        assert completion["exit_code"] == 0
        assert completion["finished_at"] is not None

    def test_finish_preserves_started_at(self, log):
        rec = log.create_execution("my-job")
        original_started = rec.started_at
        log.finish_execution(rec.id, ExecutionStatus.FAILED, exit_code=1,
                             stderr="boom")
        finished = log.get_execution(rec.id)
        assert finished.started_at == original_started
        assert finished.status == ExecutionStatus.FAILED

    def test_finish_records_stderr(self, log):
        rec = log.create_execution("my-job")
        log.finish_execution(rec.id, ExecutionStatus.TIMEOUT, stderr="timed out")
        finished = log.get_execution(rec.id)
        assert finished.stderr == "timed out"
        assert finished.status == ExecutionStatus.TIMEOUT


class TestGetExecution:
    def test_get_returns_latest_state(self, log):
        rec = log.create_execution("my-job")
        before = log.get_execution(rec.id)
        assert before.status == ExecutionStatus.RUNNING

        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
        after = log.get_execution(rec.id)
        assert after.status == ExecutionStatus.SUCCESS

    def test_get_nonexistent_returns_none(self, log):
        assert log.get_execution(999) is None


def _pad_log(log, line_count, padding=4000):
    """Append `line_count` bulky unrelated records to the log file."""
    with log._log_path.open("a", encoding="utf-8") as fh:
        for i in range(line_count):
            fh.write(json.dumps({
                "id": 10_000 + i,
                "job_name": "filler",
                "status": "success",
                "started_at": "2026-01-01T00:00:00+00:00",
                "finished_at": "2026-01-01T00:00:01+00:00",
                "exit_code": 0,
                "stdout": "x" * padding,
                "stderr": None,
            }) + "\n")


class TestBoundedLookup:
    """The finish path must not re-parse the whole log (MS-610).

    `finish_execution` reads the start record back to recover `started_at`
    and `job_name`.  Doing that with a full forward parse cost 2.94 s and
    1.7 GB on the live 370 MB log, on every single job finish.
    """

    def test_get_execution_bypasses_read_lines(self, log, monkeypatch):
        rec = log.create_execution("my-job")
        _pad_log(log, 200)

        def explode():
            raise AssertionError("get_execution must not call _read_lines")

        monkeypatch.setattr(ExecutionLog, "_read_lines", lambda self: explode())

        found = log.get_execution(rec.id)
        assert found is not None
        assert found.id == rec.id
        assert found.job_name == "my-job"
        assert found.started_at == rec.started_at

    def test_finish_preserves_start_fields_without_full_parse(self, log, monkeypatch):
        rec = log.create_execution("my-job")
        _pad_log(log, 200)

        def explode():
            raise AssertionError("finish_execution must not call _read_lines")

        monkeypatch.setattr(ExecutionLog, "_read_lines", lambda self: explode())
        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)

        monkeypatch.undo()
        finished = log.get_execution(rec.id)
        assert finished.status == ExecutionStatus.SUCCESS
        assert finished.job_name == "my-job"
        assert finished.started_at == rec.started_at
        assert finished.exit_code == 0

    def test_reads_far_less_than_the_whole_file(self, log):
        rec = log.create_execution("my-job")
        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
        _pad_log(log, 500)
        # The target record is the newest one, so append it last.
        tail = log.create_execution("tail-job")

        total = log._log_path.stat().st_size
        assert total > 1_000_000

        read_bytes = 0
        real_open = type(log._log_path).open

        def counting_open(self, *args, **kwargs):
            fh = real_open(self, *args, **kwargs)
            real_read = fh.read

            def read(*a, **k):
                nonlocal read_bytes
                chunk = real_read(*a, **k)
                read_bytes += len(chunk)
                return chunk

            fh.read = read
            return fh

        try:
            type(log._log_path).open = counting_open
            found = log.get_execution(tail.id)
        finally:
            type(log._log_path).open = real_open

        assert found.job_name == "tail-job"
        assert read_bytes < total // 10

    def test_falls_back_when_start_record_is_absent(self, log):
        """An unknown id keeps today's behaviour: no job_name, fresh start time."""
        log.finish_execution(4242, ExecutionStatus.FAILED, exit_code=1)

        record = log.get_execution(4242)
        assert record.job_name is None
        assert record.started_at is not None
        assert record.exit_code == 1

    @pytest.mark.parametrize("chunk", [1, 2, 7, 16, 64, 997])
    def test_finds_records_across_every_chunk_boundary(self, log, monkeypatch, chunk):
        """Every record must resolve regardless of where the chunk boundaries land.

        Shrinking the chunk below the record size is what makes this bite:
        at the default 64 KB almost nothing straddles a boundary, so a
        carry bug reads as green while losing records in production. Small
        chunks force every record to span several, and also cover the case
        of a line longer than one whole chunk.
        """
        monkeypatch.setattr(execution_log, "_REVERSE_CHUNK_BYTES", chunk)

        targets = []
        for _ in range(20):
            targets.append(log.create_execution("boundary-job"))
            _pad_log(log, 1, padding=50)

        for rec in targets:
            found = log.get_execution(rec.id)
            assert found is not None, f"lost record {rec.id} at chunk={chunk}"
            assert found.job_name == "boundary-job"
            assert found.started_at == rec.started_at

        assert log.get_execution(999_999) is None

    def test_missing_log_file_returns_none(self, log):
        assert log.get_execution(1) is None

    @pytest.mark.parametrize("fragment", [b"1234567890", b"null", b"true", b"[1,2]", b'"str"'])
    def test_parse_entry_rejects_non_dict_json(self, fragment):
        """A torn fragment can be valid JSON without being a record.

        A run of digits parses as an int, and `entry.get("id")` would
        raise AttributeError on it rather than skipping the line.
        """
        assert execution_log._parse_entry(fragment) is None

    def test_torn_non_dict_line_does_not_crash_the_scan(self, log):
        rec = log.create_execution("my-job")
        with log._log_path.open("a", encoding="utf-8") as fh:
            fh.write("1234567890\n")

        assert log.get_execution(999) is None
        assert log.get_execution(rec.id).job_name == "my-job"


class TestGetExecutionHistory:
    def test_returns_recent_for_job(self, log):
        for i in range(5):
            rec = log.create_execution("my-job")
            log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)

        history = log.get_execution_history("my-job")
        assert len(history) == 5
        # Newest first.
        assert history[0].id > history[-1].id

    def test_filters_by_job_name(self, log):
        r1 = log.create_execution("job-a")
        log.finish_execution(r1.id, ExecutionStatus.SUCCESS, exit_code=0)
        r2 = log.create_execution("job-b")
        log.finish_execution(r2.id, ExecutionStatus.SUCCESS, exit_code=0)

        history_a = log.get_execution_history("job-a")
        assert len(history_a) == 1
        assert history_a[0].job_name == "job-a"

    def test_respects_limit(self, log):
        for i in range(10):
            rec = log.create_execution("my-job")
            log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)

        history = log.get_execution_history("my-job", limit=3)
        assert len(history) == 3

    def test_deduplicates_by_execution_id(self, log):
        """Completion records should supersede start records."""
        rec = log.create_execution("my-job")
        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)

        history = log.get_execution_history("my-job")
        assert len(history) == 1
        assert history[0].status == ExecutionStatus.SUCCESS

    def test_empty_history(self, log):
        assert log.get_execution_history("no-such-job") == []


class TestFailRunningExecutions:
    def test_marks_running_as_failed(self, log):
        r1 = log.create_execution("job-a")
        r2 = log.create_execution("job-b")
        log.finish_execution(r2.id, ExecutionStatus.SUCCESS, exit_code=0)

        count = log.fail_running_executions()
        assert count == 1

        failed = log.get_execution(r1.id)
        assert failed.status == ExecutionStatus.FAILED
        assert "scheduler restarted" in failed.stderr

        # Already-finished execution should be unaffected.
        still_ok = log.get_execution(r2.id)
        assert still_ok.status == ExecutionStatus.SUCCESS

    def test_no_running_returns_zero(self, log):
        rec = log.create_execution("my-job")
        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
        assert log.fail_running_executions() == 0

    def test_empty_log_returns_zero(self, log):
        assert log.fail_running_executions() == 0


class TestConcurrency:
    def test_concurrent_creates(self, log):
        """Multiple threads creating executions should produce unique IDs."""
        results = []
        errors = []

        def create(name):
            try:
                rec = log.create_execution(name)
                results.append(rec.id)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=create, args=(f"job-{i}",))
            for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert len(results) == 20
        assert len(set(results)) == 20  # All IDs unique.

    def test_concurrent_create_and_finish(self, log):
        """Concurrent start + finish should not corrupt the log."""
        errors = []

        def run_job(name):
            try:
                rec = log.create_execution(name)
                log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=run_job, args=(f"job-{i}",))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        # Each job should have exactly one completed execution.
        for i in range(10):
            history = log.get_execution_history(f"job-{i}")
            assert len(history) == 1
            assert history[0].status == ExecutionStatus.SUCCESS


class TestLogFileFormat:
    def test_each_line_is_valid_json(self, log, tmp_path):
        rec = log.create_execution("my-job")
        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0,
                             stdout="output")
        log_path = tmp_path / "execution.log"
        for line in log_path.read_text().strip().splitlines():
            data = json.loads(line)
            assert "id" in data
            assert "job_name" in data
            assert "status" in data

    def test_counter_file_persists(self, tmp_path):
        """Counter should survive across ExecutionLog instances."""
        log1 = ExecutionLog(data_dir=tmp_path)
        log1.create_execution("job-a")
        log1.create_execution("job-b")

        log2 = ExecutionLog(data_dir=tmp_path)
        rec = log2.create_execution("job-c")
        assert rec.id == 3
