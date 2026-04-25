"""Tests for Maildir-backed message delivery."""

import json
import mailbox
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from overlord.maildir import CATCHALL, MaildirStore


@pytest.fixture
def store(tmp_path):
    """Create a MaildirStore backed by a temporary directory."""
    return MaildirStore(data_dir=tmp_path)


class TestDeliver:
    def test_deliver_to_consumer(self, store):
        payload = json.dumps({"msg": "hello"})
        key = store.deliver(payload, consumer="worker", job_name="test-job")
        assert key is not None

        # Message should be in the worker mailbox
        msgs = store.fetch_messages("worker")
        assert len(msgs) == 1
        assert json.loads(msgs[0]["payload"]) == {"msg": "hello"}
        assert msgs[0]["consumer"] == "worker"
        assert "test-job" in msgs[0]["subject"]

    def test_deliver_to_catchall(self, store):
        payload = json.dumps({"msg": "unaddressed"})
        store.deliver(payload, consumer=None, job_name="bcast")

        msgs = store.fetch_messages(CATCHALL)
        assert len(msgs) == 1
        assert msgs[0]["consumer"] is None
        assert json.loads(msgs[0]["payload"]) == {"msg": "unaddressed"}

    def test_deliver_with_execution_time(self, store):
        t = datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc)
        payload = json.dumps({"data": 1})
        store.deliver(payload, consumer="w", job_name="j", execution_time=t)

        msgs = store.fetch_messages("w")
        assert len(msgs) == 1
        assert "2026-04-25" in msgs[0]["subject"]

    def test_deliver_multiple_messages(self, store):
        for i in range(3):
            store.deliver(json.dumps({"i": i}), consumer="multi")

        msgs = store.fetch_messages("multi")
        assert len(msgs) == 3


class TestConsume:
    def test_consume_moves_to_processed(self, store):
        payload = json.dumps({"data": "test"})
        key = store.deliver(payload, consumer="c1", job_name="j1")

        # Before consume
        assert store.count_messages("c1") == 1

        store.consume("c1", key)

        # After consume — main mailbox should be empty
        assert store.count_messages("c1") == 0

        # Processed subfolder should have the message
        processed = store._get_processed_maildir("c1")
        assert len(processed) == 1

    def test_consume_nonexistent_key(self, store):
        # Should not raise
        store.consume("c1", "nonexistent-key")

    def test_consume_bulk(self, store):
        keys = []
        for i in range(3):
            k = store.deliver(json.dumps({"i": i}), consumer="bulk")
            keys.append(k)

        assert store.count_messages("bulk") == 3

        store.consume_bulk("bulk", keys)

        assert store.count_messages("bulk") == 0
        processed = store._get_processed_maildir("bulk")
        assert len(processed) == 3


class TestFetchUnconsumed:
    def test_fetch_by_consumer_name(self, store):
        store.deliver(json.dumps({"a": 1}), consumer="alpha")
        store.deliver(json.dumps({"b": 2}), consumer="beta")

        msgs = store.fetch_unconsumed_for_consumers(["alpha"])
        assert len(msgs) == 1
        assert json.loads(msgs[0]["payload"]) == {"a": 1}

    def test_fetch_wildcard_returns_catchall(self, store):
        store.deliver(json.dumps({"c": 3}), consumer=None)

        msgs = store.fetch_unconsumed_for_consumers(["*"])
        assert len(msgs) == 1
        assert json.loads(msgs[0]["payload"]) == {"c": 3}

    def test_fetch_multiple_consumers(self, store):
        store.deliver(json.dumps({"a": 1}), consumer="alpha")
        store.deliver(json.dumps({"b": 2}), consumer="beta")

        msgs = store.fetch_unconsumed_for_consumers(["alpha", "beta"])
        assert len(msgs) == 2


class TestListMailboxes:
    def test_list_empty(self, store):
        assert store.list_mailboxes() == []

    def test_list_after_delivery(self, store):
        store.deliver(json.dumps({"x": 1}), consumer="foo")
        store.deliver(json.dumps({"x": 2}), consumer="bar")
        store.deliver(json.dumps({"x": 3}), consumer=None)

        boxes = store.list_mailboxes()
        assert "foo" in boxes
        assert "bar" in boxes
        assert CATCHALL in boxes


class TestPayloadExtraction:
    def test_payload_is_valid_json(self, store):
        original = {"nested": {"key": "value"}, "list": [1, 2, 3]}
        store.deliver(json.dumps(original), consumer="jsontest")

        msgs = store.fetch_messages("jsontest")
        assert json.loads(msgs[0]["payload"]) == original

    def test_rfc822_envelope_structure(self, store):
        store.deliver(json.dumps({"d": 1}), consumer="rfc", job_name="myjob")

        md = mailbox.Maildir(str(store.mailboxes_dir / "rfc"))
        for key, msg in md.iteritems():
            assert "myjob" in msg["Subject"]
            assert msg["X-Overlord-Job"] == "myjob"
            assert msg["X-Overlord-Consumer"] == "rfc"

            # Check attachment exists
            attachments = [
                p for p in msg.walk() if p.get_filename() == "payload.json"
            ]
            assert len(attachments) == 1
