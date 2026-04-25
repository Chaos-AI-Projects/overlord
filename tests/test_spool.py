"""Tests for file-based spool delivery."""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from overlord.spool import SpoolProcessor, SpoolWriter


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path


@pytest.fixture
def writer(data_dir):
    return SpoolWriter(data_dir=data_dir)


@pytest.fixture
def processor(data_dir):
    return SpoolProcessor(data_dir=data_dir, poll_interval=0.1)


class TestSpoolWriter:
    def test_write_creates_file_in_spool(self, writer, data_dir):
        path = writer.write(
            payload=json.dumps({"msg": "hello"}),
            consumer="worker",
            job_name="test-job",
        )
        assert path.exists()
        assert path.parent == data_dir / "spool"
        assert path.suffix == ".json"

    def test_write_file_contains_envelope(self, writer):
        t = datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc)
        path = writer.write(
            payload=json.dumps({"data": 1}),
            consumer="w",
            job_name="j",
            execution_time=t,
        )
        envelope = json.loads(path.read_text())
        assert envelope["payload"] == json.dumps({"data": 1})
        assert envelope["consumer"] == "w"
        assert envelope["job_name"] == "j"
        assert "2026-04-25" in envelope["execution_time"]

    def test_write_no_consumer(self, writer):
        path = writer.write(payload=json.dumps({"x": 1}))
        envelope = json.loads(path.read_text())
        assert envelope["consumer"] is None

    def test_write_atomic_no_tmp_leftover(self, writer, data_dir):
        writer.write(payload=json.dumps({"a": 1}))
        tmp_dir = data_dir / "spool" / "tmp"
        leftover = list(tmp_dir.iterdir())
        assert len(leftover) == 0

    def test_write_multiple_unique_files(self, writer, data_dir):
        paths = set()
        for i in range(5):
            p = writer.write(payload=json.dumps({"i": i}), consumer="c")
            paths.add(p)
        assert len(paths) == 5
        spool_files = list((data_dir / "spool").glob("*.json"))
        assert len(spool_files) == 5


class TestSpoolProcessor:
    def test_process_spool_delivers_to_maildir(self, writer, processor, data_dir):
        writer.write(
            payload=json.dumps({"msg": "hello"}),
            consumer="worker",
            job_name="test-job",
        )
        # Manually trigger processing (synchronous helper).
        processor._process_spool()

        # Spool file should be removed.
        spool_files = list((data_dir / "spool").glob("*.json"))
        assert len(spool_files) == 0

        # Message should be in the Maildir.
        msgs = processor._store.fetch_messages("worker")
        assert len(msgs) == 1
        assert json.loads(msgs[0]["payload"]) == {"msg": "hello"}
        assert "test-job" in msgs[0]["subject"]

    def test_process_spool_catchall(self, writer, processor, data_dir):
        writer.write(payload=json.dumps({"x": 1}), consumer=None)
        processor._process_spool()

        msgs = processor._store.fetch_messages("catchall")
        assert len(msgs) == 1

    def test_process_spool_fifo_order(self, writer, processor, data_dir):
        for i in range(3):
            writer.write(
                payload=json.dumps({"order": i}),
                consumer="ordered",
                job_name=f"job-{i}",
            )

        # Verify spool files are sorted by filename (timestamp-prefixed).
        spool_dir = data_dir / "spool"
        spool_files = sorted(f for f in spool_dir.iterdir() if f.suffix == ".json")
        file_payloads = [
            json.loads(f.read_text())["payload"] for f in spool_files
        ]
        assert [json.loads(p)["order"] for p in file_payloads] == [0, 1, 2]

        processor._process_spool()

        # All files delivered.
        msgs = processor._store.fetch_messages("ordered")
        assert len(msgs) == 3
        # All payloads present (Maildir doesn't guarantee retrieval order).
        payloads = {json.loads(m["payload"])["order"] for m in msgs}
        assert payloads == {0, 1, 2}

    def test_process_spool_preserves_execution_time(self, writer, processor):
        t = datetime(2026, 1, 15, 8, 30, 0, tzinfo=timezone.utc)
        writer.write(
            payload=json.dumps({"d": 1}),
            consumer="timed",
            job_name="j",
            execution_time=t,
        )
        processor._process_spool()

        msgs = processor._store.fetch_messages("timed")
        assert len(msgs) == 1
        assert "2026-01-15" in msgs[0]["subject"]

    def test_process_empty_spool(self, processor):
        # Should not raise.
        processor._process_spool()

    def test_malformed_file_skipped(self, processor, data_dir):
        spool_dir = data_dir / "spool"
        spool_dir.mkdir(parents=True, exist_ok=True)
        bad_file = spool_dir / "bad.json"
        bad_file.write_text("not valid json {{{")

        # Write a valid file after the bad one.
        writer = SpoolWriter(data_dir=data_dir)
        writer.write(payload=json.dumps({"ok": True}), consumer="c")

        processor._process_spool()

        # Bad file remains (delivery failed), good file is delivered.
        msgs = processor._store.fetch_messages("c")
        assert len(msgs) == 1
        assert bad_file.exists()

    @pytest.mark.asyncio
    async def test_run_and_stop(self, writer, processor):
        writer.write(payload=json.dumps({"async": True}), consumer="async-test")

        task = asyncio.create_task(processor.run())
        # Give the processor time to pick up the file.
        await asyncio.sleep(0.3)
        processor.stop()
        await task

        msgs = processor._store.fetch_messages("async-test")
        assert len(msgs) == 1
        assert json.loads(msgs[0]["payload"]) == {"async": True}

    @pytest.mark.asyncio
    async def test_final_drain_on_stop(self, data_dir):
        """Stopping the processor triggers a final drain of the spool."""
        writer = SpoolWriter(data_dir=data_dir)
        proc = SpoolProcessor(data_dir=data_dir, poll_interval=10.0)

        # Start processor with a very long poll interval.
        task = asyncio.create_task(proc.run())
        await asyncio.sleep(0.05)

        # Write after start — the long poll interval means it won't be
        # picked up by normal polling, but the final drain should get it.
        writer.write(payload=json.dumps({"drain": True}), consumer="drain-test")
        proc.stop()
        await task

        msgs = proc._store.fetch_messages("drain-test")
        assert len(msgs) == 1
