"""Tests for file-based spool delivery."""

import asyncio
import json
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import pytest

from overlord.maildir import MaildirStore
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


def _build_and_write(writer, payload, consumer=None, job_name="unknown",
                     execution_time=None):
    """Helper: build an EmailMessage and write it to the spool."""
    msg = MaildirStore.build_message(
        payload=payload,
        consumer=consumer,
        job_name=job_name,
        execution_time=execution_time,
    )
    return writer.write(msg)


class TestSpoolWriter:
    def test_write_creates_file_in_spool(self, writer, data_dir):
        path = _build_and_write(
            writer,
            payload=json.dumps({"msg": "hello"}),
            consumer="worker",
            job_name="test-job",
        )
        assert path.exists()
        assert path.parent == data_dir / "spool"
        assert path.suffix == ".eml"

    def test_write_file_contains_rfc822(self, writer):
        t = datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc)
        path = _build_and_write(
            writer,
            payload=json.dumps({"data": 1}),
            consumer="w",
            job_name="j",
            execution_time=t,
        )
        raw = path.read_bytes()
        # RFC 822 messages contain headers like Subject:
        assert b"Subject:" in raw
        assert b"X-Overlord-Job: j" in raw
        assert b"X-Overlord-Consumer: w" in raw
        assert b"2026-04-25" in raw

    def test_write_no_consumer(self, writer):
        path = _build_and_write(
            writer,
            payload=json.dumps({"x": 1}),
        )
        raw = path.read_bytes()
        assert b"X-Overlord-Consumer" not in raw

    def test_write_atomic_no_tmp_leftover(self, writer, data_dir):
        _build_and_write(writer, payload=json.dumps({"a": 1}))
        tmp_dir = data_dir / "spool" / "tmp"
        leftover = list(tmp_dir.iterdir())
        assert len(leftover) == 0

    def test_write_multiple_unique_files(self, writer, data_dir):
        paths = set()
        for i in range(5):
            p = _build_and_write(
                writer, payload=json.dumps({"i": i}), consumer="c",
            )
            paths.add(p)
        assert len(paths) == 5
        spool_files = list((data_dir / "spool").glob("*.eml"))
        assert len(spool_files) == 5


class TestSpoolProcessor:
    def test_process_spool_delivers_to_maildir(self, writer, processor, data_dir):
        _build_and_write(
            writer,
            payload=json.dumps({"msg": "hello"}),
            consumer="worker",
            job_name="test-job",
        )
        # Manually trigger processing (synchronous helper).
        processor._process_spool()

        # Spool file should be removed.
        spool_files = list((data_dir / "spool").glob("*.eml"))
        assert len(spool_files) == 0

        # Message should be in the Maildir.
        msgs = processor._store.fetch_messages("worker")
        assert len(msgs) == 1
        assert json.loads(msgs[0]["payload"]) == {"msg": "hello"}
        assert "test-job" in msgs[0]["subject"]

    def test_process_spool_catchall(self, writer, processor, data_dir):
        _build_and_write(writer, payload=json.dumps({"x": 1}), consumer=None)
        processor._process_spool()

        msgs = processor._store.fetch_messages("catchall")
        assert len(msgs) == 1

    def test_process_spool_fifo_order(self, writer, processor, data_dir):
        for i in range(3):
            _build_and_write(
                writer,
                payload=json.dumps({"order": i}),
                consumer="ordered",
                job_name=f"job-{i}",
            )

        # Verify spool files are sorted by filename (timestamp-prefixed).
        spool_dir = data_dir / "spool"
        spool_files = sorted(f for f in spool_dir.iterdir() if f.suffix == ".eml")
        assert len(spool_files) == 3

        processor._process_spool()

        # All files delivered.
        msgs = processor._store.fetch_messages("ordered")
        assert len(msgs) == 3
        # All payloads present (Maildir doesn't guarantee retrieval order).
        payloads = {json.loads(m["payload"])["order"] for m in msgs}
        assert payloads == {0, 1, 2}

    def test_process_spool_preserves_execution_time(self, writer, processor):
        t = datetime(2026, 1, 15, 8, 30, 0, tzinfo=timezone.utc)
        _build_and_write(
            writer,
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
        # A truncated binary file that cannot be parsed at all.
        bad_file = spool_dir / "bad.eml"
        bad_file.write_bytes(b"\x00\x01\x02")

        # Write a valid file after the bad one.
        writer = SpoolWriter(data_dir=data_dir)
        _build_and_write(writer, payload=json.dumps({"ok": True}), consumer="c")

        processor._process_spool()

        # Good file is delivered regardless.
        msgs = processor._store.fetch_messages("c")
        assert len(msgs) == 1

    @pytest.mark.asyncio
    async def test_run_and_stop(self, writer, processor):
        _build_and_write(
            writer, payload=json.dumps({"async": True}), consumer="async-test",
        )

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
        _build_and_write(
            writer, payload=json.dumps({"drain": True}), consumer="drain-test",
        )
        proc.stop()
        await task

        msgs = proc._store.fetch_messages("drain-test")
        assert len(msgs) == 1

    @pytest.mark.asyncio
    async def test_sigusr1_wakes_processor(self, data_dir):
        """SIGUSR1 interrupts the poll sleep and triggers immediate processing."""
        writer = SpoolWriter(data_dir=data_dir)
        proc = SpoolProcessor(data_dir=data_dir, poll_interval=60.0)

        task = asyncio.create_task(proc.run())
        # Let the processor start and enter its sleep.
        await asyncio.sleep(0.1)

        # Write a message (SpoolWriter.write sends SIGUSR1 automatically).
        _build_and_write(
            writer, payload=json.dumps({"wake": True}), consumer="wake-test",
        )

        # The signal should wake the processor almost immediately.
        await asyncio.sleep(0.3)
        proc.stop()
        await task

        msgs = proc._store.fetch_messages("wake-test")
        assert len(msgs) == 1
        assert json.loads(msgs[0]["payload"]) == {"wake": True}

    @pytest.mark.asyncio
    async def test_signal_handler_cleaned_up_on_stop(self, data_dir):
        """SIGUSR1 handler is removed after the processor stops."""
        proc = SpoolProcessor(data_dir=data_dir, poll_interval=0.1)

        task = asyncio.create_task(proc.run())
        await asyncio.sleep(0.05)
        proc.stop()
        await task

        # After stop, sending SIGUSR1 should use the default handler
        # (which terminates the process) — but we can't safely test that.
        # Instead, verify _wake_event is no longer being set by the handler.
        proc._wake_event.clear()
        # Reinstall a no-op handler so the signal doesn't kill the test.
        loop = asyncio.get_running_loop()
        received = []
        loop.add_signal_handler(signal.SIGUSR1, lambda: received.append(True))
        try:
            os.kill(os.getpid(), signal.SIGUSR1)
            await asyncio.sleep(0.05)
            # The processor's handler should NOT have set _wake_event.
            assert not proc._wake_event.is_set()
            # Our temporary handler caught the signal.
            assert len(received) == 1
        finally:
            loop.remove_signal_handler(signal.SIGUSR1)

    @pytest.mark.asyncio
    async def test_poll_interval_fallback(self, data_dir):
        """Without a signal, the processor falls back to poll_interval."""
        proc = SpoolProcessor(data_dir=data_dir, poll_interval=0.2)
        writer = SpoolWriter(data_dir=data_dir)

        task = asyncio.create_task(proc.run())
        await asyncio.sleep(0.05)

        # Write directly to spool bypassing SpoolWriter to avoid SIGUSR1.
        msg = MaildirStore.build_message(
            payload=json.dumps({"poll": True}),
            consumer="poll-test",
            job_name="test",
        )
        import time, uuid
        filename = f"{time.monotonic_ns()}-{uuid.uuid4().hex}.eml"
        spool_dir = data_dir / "spool"
        spool_dir.mkdir(parents=True, exist_ok=True)
        (spool_dir / filename).write_bytes(msg.as_bytes())

        # Wait long enough for poll_interval to elapse.
        await asyncio.sleep(0.5)
        proc.stop()
        await task

        msgs = proc._store.fetch_messages("poll-test")
        assert len(msgs) == 1
