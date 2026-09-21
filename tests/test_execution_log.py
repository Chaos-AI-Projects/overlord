"""Tests for the JSON-lines execution history log."""

import inspect
import json
import threading
from contextlib import closing, contextmanager
from types import SimpleNamespace

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


@contextmanager
def _count_read_bytes(log):
    """Count bytes pulled out of the log file inside the block."""
    counter = SimpleNamespace(total=0)
    path_type = type(log._log_path)
    real_open = path_type.open

    def counting_open(self, *args, **kwargs):
        handle = real_open(self, *args, **kwargs)
        if self != log._log_path:
            return handle
        real_read = handle.read

        def read(*a, **k):
            chunk = real_read(*a, **k)
            counter.total += len(chunk)
            return chunk

        handle.read = read
        return handle

    path_type.open = counting_open
    try:
        yield counter
    finally:
        path_type.open = real_open


@contextmanager
def _forbid_whole_file_reads(log):
    """Fail if anything pulls the log into memory in one gulp.

    This is the property MS-610 and MS-614 are about, and it outlives any
    one helper: asserting that a particular private method went unused
    stops guarding anything the moment that method is renamed or deleted.

    Watching `Path.read_text`/`read_bytes` is not enough, because the
    reader works on an open handle and never calls either.  So cap what a
    single `read()` may return.  The bound is generous at twice the chunk
    -- the point is to separate chunked reads from a slurp of the whole
    file, not to pin the exact chunk size.
    """
    limit = 2 * execution_log._REVERSE_CHUNK_BYTES
    path_type = type(log._log_path)
    real_open = path_type.open
    originals = {name: getattr(path_type, name) for name in ("read_text", "read_bytes")}

    def forbid(name):
        def wrapper(self, *args, **kwargs):
            if self == log._log_path:
                raise AssertionError(f"the whole log was read in one {name}() call")
            return originals[name](self, *args, **kwargs)

        return wrapper

    def guarded_open(self, *args, **kwargs):
        handle = real_open(self, *args, **kwargs)
        if self != log._log_path:
            return handle
        real_read = handle.read

        def read(*a, **k):
            chunk = real_read(*a, **k)
            if len(chunk) > limit:
                raise AssertionError(
                    f"one read() returned {len(chunk)} bytes, over the "
                    f"{limit}-byte chunked-read bound"
                )
            return chunk

        handle.read = read
        return handle

    for name in originals:
        setattr(path_type, name, forbid(name))
    path_type.open = guarded_open
    try:
        yield
    finally:
        path_type.open = real_open
        for name, original in originals.items():
            setattr(path_type, name, original)


class TestBoundedLookup:
    """The finish path must not re-parse the whole log (MS-610).

    `finish_execution` reads the start record back to recover `started_at`
    and `job_name`.  Doing that with a full forward parse cost 2.94 s and
    1.7 GB on the live 370 MB log, on every single job finish.
    """

    def test_get_execution_never_reads_the_whole_log(self, log):
        rec = log.create_execution("my-job")
        _pad_log(log, 200)

        with _forbid_whole_file_reads(log):
            found = log.get_execution(rec.id)

        assert found is not None
        assert found.id == rec.id
        assert found.job_name == "my-job"
        assert found.started_at == rec.started_at

    def test_finish_preserves_start_fields_without_full_parse(self, log):
        rec = log.create_execution("my-job")
        _pad_log(log, 200)

        with _forbid_whole_file_reads(log):
            log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)

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

        with _count_read_bytes(log) as counter:
            found = log.get_execution(tail.id)

        assert found.job_name == "tail-job"
        assert counter.total < total // 10

    def test_falls_back_when_start_record_is_absent(self, log):
        """An unknown id keeps today's behaviour: no job_name, fresh start time."""
        log.finish_execution(4242, ExecutionStatus.FAILED, exit_code=1)

        record = log.get_execution(4242)
        assert record.job_name is None
        assert record.started_at is not None
        assert record.exit_code == 1

    def test_reset_counter_resolves_to_the_newest_line_not_the_highest_id(self, log):
        """Position, not ID order, decides which line for an ID wins.

        The live log's ID counter restarted 155901 -> 1 on 2026-06-09, so
        an ID can occur twice in one file.  Every other test builds a
        monotonic log, where selecting the highest ID and selecting the
        newest line agree; only a reset separates them.  Newest-by-position
        is the deliberate choice, because the append-only log makes the
        last line for an ID authoritative.
        """
        with log._log_path.open("a", encoding="utf-8") as fh:
            for entry in (
                {"id": 7, "job_name": "before-reset", "status": "success"},
                {"id": 900, "job_name": "pre-reset-high-water", "status": "success"},
                {"id": 7, "job_name": "after-reset", "status": "running"},
            ):
                entry.update(started_at="2026-06-09T00:00:00+00:00", finished_at=None,
                             exit_code=None, stdout=None, stderr=None)
                fh.write(json.dumps(entry) + "\n")

        assert log.get_execution(7).job_name == "after-reset"

        # The sweep resolves each ID once, newest line first, so the
        # post-reset RUNNING line is the one it acts on.
        assert log.fail_running_executions() == 1
        assert log.get_execution(7).status == ExecutionStatus.FAILED

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


class TestReverseIteration:
    """Direct tests for the backwards generator.

    Every other test reaches it through a consumer, and all three
    consumers dedupe by ID, so a duplicated yield is invisible to them.
    That hid a real defect for the whole of MS-614: copying the carry
    instead of consuming it re-emits every row sitting on a chunk
    boundary, and all 53 tests still passed.
    """

    def _write_fixed_width_rows(self, log, count):
        """Append *count* rows of identical byte width; return their IDs and that width."""
        ids = list(range(1, count + 1))
        widths = set()
        with log._log_path.open("a", encoding="utf-8") as fh:
            for entry_id in ids:
                line = json.dumps({
                    "id": entry_id,
                    "job_name": "fixed-width-job",
                    "status": "success",
                    "started_at": "2026-09-21T00:00:00+00:00",
                    "finished_at": "2026-09-21T00:00:01+00:00",
                    "exit_code": 0,
                    "stdout": None,
                    "stderr": None,
                }) + "\n"
                widths.add(len(line.encode("utf-8")))
                fh.write(line)
        # Equal widths are what let the caller place a boundary exactly on
        # a newline; a wider ID would silently move it off.
        assert len(widths) == 1, f"fixture rows must be equal width, got {widths}"
        return ids, widths.pop()

    def test_yields_each_entry_exactly_once_on_a_chunk_boundary(self, log, monkeypatch):
        """A chunk ending exactly at a newline must not re-emit that row.

        Sizing the chunk to a whole number of rows is the alignment that
        bites: the carry comes back empty, so a handover that copies
        rather than consumes leaves the boundary row in the list it also
        hands forward, and the row is yielded twice.  Unaligned chunks
        never expose it, which is why the parametrised boundary test
        above stays green against the same mutant.
        """
        ids, width = self._write_fixed_width_rows(log, 6)
        monkeypatch.setattr(execution_log, "_REVERSE_CHUNK_BYTES", width * 2)

        with closing(log._iter_entries_reverse()) as entries:
            seen = [entry["id"] for entry in entries]

        assert seen == sorted(ids, reverse=True)


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

    def test_reset_counter_orders_history_by_position_not_by_id(self, log):
        """Newest first must mean newest line, not highest ID.

        History selects newest-by-position, the same deliberate choice
        `get_execution` and the sweep make, because the append-only log
        makes the last line for an ID authoritative.  Selecting and
        ordering only disagree after an ID counter reset, and the live
        log has had one (155901 -> 1 on 2026-06-09), which can float a
        pre-reset high-water ID above a genuinely newer post-reset line.
        """
        rows = [
            {"id": 7, "status": "success"},
            {"id": 900, "status": "success"},
            {"id": 7, "status": "running"},
        ]
        with log._log_path.open("a", encoding="utf-8") as fh:
            for row in rows:
                row.update(job_name="reset-job",
                           started_at="2026-06-09T00:00:00+00:00",
                           finished_at=None, exit_code=None,
                           stdout=None, stderr=None)
                fh.write(json.dumps(row) + "\n")

        history = log.get_execution_history("reset-job")

        assert [rec.id for rec in history] == [7, 900]
        assert history[0].status == ExecutionStatus.RUNNING


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


class TestBoundedHistoryAndSweep:
    """History and the startup sweep must stream the log, not list it (MS-614).

    Both read and JSON-parsed the whole file into a list, which is ~2 GB of
    peak RSS on the live 370 MB log.  `fail_running_executions` runs at
    daemon startup, the one moment the box is least able to absorb that,
    and the log has no rotation so the ceiling only rises.
    """

    def test_history_never_reads_the_whole_log(self, log):
        _pad_log(log, 50)

        with _forbid_whole_file_reads(log):
            assert log.get_execution_history("absent-job") == []

    def test_history_reads_far_less_than_the_whole_file(self, log):
        _pad_log(log, 500)
        wanted = []
        for _ in range(3):
            rec = log.create_execution("tail-job")
            log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
            wanted.append(rec.id)

        total = log._log_path.stat().st_size
        assert total > 1_000_000

        with _count_read_bytes(log) as counter:
            history = log.get_execution_history("tail-job", limit=3)

        assert [rec.id for rec in history] == sorted(wanted, reverse=True)
        assert all(rec.status == ExecutionStatus.SUCCESS for rec in history)
        assert counter.total < total // 10

    def test_history_stops_at_the_limit_rather_than_the_file_start(self, log):
        """Older matching records beyond the limit must go unread.

        A busy job has thousands of records, so the cost has to scale with
        the limit and not with the history.
        """
        for _ in range(20):
            rec = log.create_execution("busy-job")
            log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
            _pad_log(log, 5)
        head_size = log._log_path.stat().st_size
        assert head_size > 4 * execution_log._REVERSE_CHUNK_BYTES
        newest = []
        for _ in range(2):
            rec = log.create_execution("busy-job")
            log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
            newest.append(rec.id)

        with _count_read_bytes(log) as counter:
            history = log.get_execution_history("busy-job", limit=2)

        assert [rec.id for rec in history] == sorted(newest, reverse=True)
        assert counter.total < head_size

    @pytest.mark.parametrize("limit", [0, -1])
    def test_history_non_positive_limit_returns_nothing(self, log, limit):
        """A negative limit must not fall through to an unbounded scan.

        The dedupe loop stops at ``len(latest) == limit``, which a negative
        limit never reaches, so without the guard it would read the whole
        log and return everything.
        """
        rec = log.create_execution("my-job")
        log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
        assert log.get_execution_history("my-job", limit=limit) == []

    def test_sweep_never_reads_the_whole_log(self, log):
        running = log.create_execution("job-a")
        _pad_log(log, 50)

        with _forbid_whole_file_reads(log):
            assert log.fail_running_executions() == 1

        assert log.get_execution(running.id).status == ExecutionStatus.FAILED

    def test_sweep_finds_every_running_id_among_finished_ones(self, log):
        """Interleaving matters: the start line of a running id is its only line."""
        expected_running = []
        for i in range(6):
            rec = log.create_execution(f"job-{i}")
            if i % 2 == 0:
                log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
            else:
                expected_running.append(rec.id)

        assert log.fail_running_executions() == len(expected_running)
        for exec_id in expected_running:
            assert log.get_execution(exec_id).status == ExecutionStatus.FAILED
        assert log.fail_running_executions() == 0

    def test_sweep_reaches_a_running_record_far_from_the_tail(self, log, monkeypatch):
        """The sweep is unbounded, and a missed RUNNING row is not recoverable.

        `scheduler.start` calls this sweep and then
        `lock_store.release_stale_locks`, which keeps any lock whose holder
        still reads RUNNING.  So a RUNNING row the sweep skips pins its
        `--lock` file forever, and every later startup appends more lines
        and pushes that row further from the tail.  A window cannot be
        re-widened after the fact, which is why there is no window.
        """
        monkeypatch.setattr(execution_log, "_REVERSE_CHUNK_BYTES", 1024)
        stale = log.create_execution("stale-job")
        _pad_log(log, 200)
        total = log._log_path.stat().st_size
        assert total > 64 * 1024

        with _count_read_bytes(log) as counter:
            assert log.fail_running_executions() == 1

        assert log.get_execution(stale.id).status == ExecutionStatus.FAILED
        # Reading every byte is the property, not merely finding this row.
        assert counter.total >= total

        # A window sized for production is inert on a test-sized fixture,
        # so byte-counting alone cannot see one come back.  Pin the shape
        # as well: the scan takes no argument that could bound it.
        assert list(inspect.signature(log._iter_entries_reverse).parameters) == []

    def test_sweep_ignores_lines_without_an_id(self, log):
        running = log.create_execution("my-job")
        with log._log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"job_name": "torn", "status": "running"}) + "\n")

        assert log.fail_running_executions() == 1
        assert log.get_execution(running.id).status == ExecutionStatus.FAILED

    @pytest.mark.parametrize("chunk", [1, 2, 7, 16, 64, 997])
    def test_survive_every_chunk_boundary(self, log, monkeypatch, chunk):
        """Shrinking the chunk below the record size is what makes this bite.

        At the default 64 KB almost nothing straddles a boundary, so a
        carry bug reads as green while losing records in production.
        """
        monkeypatch.setattr(execution_log, "_REVERSE_CHUNK_BYTES", chunk)

        running = []
        for i in range(8):
            rec = log.create_execution("boundary-job")
            if i % 2 == 0:
                log.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0)
            else:
                running.append(rec.id)
            _pad_log(log, 1, padding=50)

        history = log.get_execution_history("boundary-job", limit=10)
        assert len(history) == 8, f"lost history records at chunk={chunk}"
        assert [rec.id for rec in history] == sorted(
            (rec.id for rec in history), reverse=True
        )

        assert log.fail_running_executions() == len(running), f"chunk={chunk}"


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
