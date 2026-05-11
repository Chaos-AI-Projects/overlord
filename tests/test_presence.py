"""Tests for the presence system helper."""

import json
from datetime import datetime, timezone

import pytest

from overlord.maildir import MaildirStore
from overlord.scripts.presence import (
    PRESENCE_CONSUMER,
    _parse_payloads,
    _sorted_messages,
    cmd_checkin,
    cmd_checkout,
    cmd_prune,
    cmd_recent,
    cmd_scan,
    cmd_scan_unfinished,
)
from overlord.spool import SpoolWriter


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Redirect both MaildirStore and SpoolWriter to a temp directory."""
    import overlord.maildir as md_mod
    import overlord.spool as sp_mod

    monkeypatch.setattr(md_mod, "DEFAULT_DATA_DIR", tmp_path)
    monkeypatch.setattr(sp_mod, "DEFAULT_DATA_DIR", tmp_path)
    return tmp_path


@pytest.fixture
def store(data_dir):
    return MaildirStore(data_dir=data_dir)


def _deliver_presence(store, job_name, description, timestamp, task_id=None):
    """Deliver a presence message directly to the maildir (bypassing spool)."""
    payload = json.dumps({
        "task_id": task_id or f"exec-{job_name}",
        "job_name": job_name,
        "description": description,
        "timestamp": timestamp,
    })
    msg = MaildirStore.build_message(
        payload=payload,
        consumer=PRESENCE_CONSUMER,
        job_name=job_name,
    )
    store.deliver(msg, consumer=PRESENCE_CONSUMER)


def _deliver_checkout(store, job_name, timestamp, task_id=None):
    """Deliver a checkout (finished) message directly to the maildir."""
    payload = json.dumps({
        "task_id": task_id or f"exec-{job_name}",
        "job_name": job_name,
        "status": "finished",
        "timestamp": timestamp,
    })
    msg = MaildirStore.build_message(
        payload=payload,
        consumer=PRESENCE_CONSUMER,
        job_name=job_name,
    )
    store.deliver(msg, consumer=PRESENCE_CONSUMER)


class TestSortedMessages:
    def test_sorts_by_timestamp_descending(self):
        messages = [
            {"payload": json.dumps({"timestamp": "2026-01-01T00:00:00Z"})},
            {"payload": json.dumps({"timestamp": "2026-01-03T00:00:00Z"})},
            {"payload": json.dumps({"timestamp": "2026-01-02T00:00:00Z"})},
        ]
        result = _sorted_messages(messages)
        timestamps = [json.loads(m["payload"])["timestamp"] for m in result]
        assert timestamps == [
            "2026-01-03T00:00:00Z",
            "2026-01-02T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ]

    def test_handles_invalid_json_payload(self):
        messages = [
            {"payload": "not json", "date": "2026-01-01"},
            {"payload": json.dumps({"timestamp": "2026-01-02T00:00:00Z"})},
        ]
        result = _sorted_messages(messages)
        assert len(result) == 2

    def test_empty_list(self):
        assert _sorted_messages([]) == []


class TestParsePayloads:
    def test_parses_json_payloads(self):
        messages = [
            {"payload": json.dumps({"job_name": "a"})},
            {"payload": json.dumps({"job_name": "b"})},
        ]
        result = _parse_payloads(messages)
        assert result == [{"job_name": "a"}, {"job_name": "b"}]

    def test_handles_invalid_json(self):
        messages = [{"payload": "not json"}]
        result = _parse_payloads(messages)
        assert result == ["not json"]

    def test_handles_already_parsed_payload(self):
        messages = [{"payload": {"job_name": "a"}}]
        result = _parse_payloads(messages)
        assert result == [{"job_name": "a"}]


class TestCmdCheckin:
    @staticmethod
    def _spool_eml_files(data_dir):
        """Return .eml files in the spool directory (excludes tmp/ subdir)."""
        return list((data_dir / "spool").glob("*.eml"))

    @staticmethod
    def _parse_spool_payload(eml_path):
        """Parse the payload.json attachment from a spool .eml file."""
        from email import policy
        from email.parser import BytesParser

        msg = BytesParser(policy=policy.default).parsebytes(eml_path.read_bytes())
        for part in msg.walk():
            if part.get_filename() == "payload.json":
                return json.loads(part.get_content())
        raise ValueError("no payload.json found")

    def test_writes_to_spool(self, data_dir, monkeypatch):
        monkeypatch.setenv("OVERLORD_EXECUTION_ID", "exec-42")
        monkeypatch.setenv("OVERLORD_JOB_NAME", "test-job")
        cmd_checkin("doing something")

        assert len(self._spool_eml_files(data_dir)) == 1

    def test_payload_content(self, data_dir, monkeypatch):
        monkeypatch.setenv("OVERLORD_EXECUTION_ID", "exec-99")
        monkeypatch.setenv("OVERLORD_JOB_NAME", "my-job")
        cmd_checkin("test task")

        payload = self._parse_spool_payload(self._spool_eml_files(data_dir)[0])
        assert payload["task_id"] == "exec-99"
        assert payload["job_name"] == "my-job"
        assert payload["description"] == "test task"
        assert "timestamp" in payload

    def test_defaults_when_env_missing(self, data_dir, monkeypatch):
        monkeypatch.delenv("OVERLORD_EXECUTION_ID", raising=False)
        monkeypatch.delenv("OVERLORD_JOB_NAME", raising=False)
        cmd_checkin("orphan task")

        payload = self._parse_spool_payload(self._spool_eml_files(data_dir)[0])
        assert payload["task_id"] == "unknown"
        assert payload["job_name"] == "unknown"


class TestCmdRecent:
    def test_returns_at_most_5(self, data_dir, store, capsys):
        for i in range(7):
            _deliver_presence(store, f"job-{i}", f"task {i}", f"2026-01-0{i+1}T00:00:00Z")

        cmd_recent()
        output = json.loads(capsys.readouterr().out)
        assert len(output) == 5

    def test_returns_newest_first(self, data_dir, store, capsys):
        _deliver_presence(store, "old", "old task", "2026-01-01T00:00:00Z")
        _deliver_presence(store, "new", "new task", "2026-01-05T00:00:00Z")

        cmd_recent()
        output = json.loads(capsys.readouterr().out)
        assert output[0]["job_name"] == "new"
        assert output[1]["job_name"] == "old"

    def test_empty_mailbox(self, data_dir, store, capsys):
        cmd_recent()
        output = json.loads(capsys.readouterr().out)
        assert output == []


class TestCmdScan:
    def test_returns_all_messages(self, data_dir, store, capsys):
        for i in range(7):
            _deliver_presence(store, f"job-{i}", f"task {i}", f"2026-01-0{i+1}T00:00:00Z")

        cmd_scan()
        output = json.loads(capsys.readouterr().out)
        assert len(output) == 7


class TestCmdPrune:
    def test_prunes_old_messages(self, data_dir, store, capsys):
        for i in range(8):
            _deliver_presence(store, f"job-{i}", f"task {i}", f"2026-01-0{i+1}T00:00:00Z")

        cmd_prune()
        stderr = capsys.readouterr().err
        assert "Pruned 3" in stderr

        # Verify only 5 remain
        remaining = store.fetch_messages(PRESENCE_CONSUMER)
        assert len(remaining) == 5

    def test_nothing_to_prune(self, data_dir, store, capsys):
        _deliver_presence(store, "job-1", "task", "2026-01-01T00:00:00Z")

        cmd_prune()
        stderr = capsys.readouterr().err
        assert "Nothing to prune" in stderr

    def test_prune_keeps_newest(self, data_dir, store, capsys):
        for i in range(7):
            _deliver_presence(store, f"job-{i}", f"task {i}", f"2026-01-0{i+1}T00:00:00Z")

        cmd_prune()

        # Scan remaining to verify they are the newest
        cmd_scan()
        output = json.loads(capsys.readouterr().out)
        timestamps = [p["timestamp"] for p in output]
        # The 5 newest should remain (days 3-7)
        for ts in timestamps:
            day = int(ts[8:10])
            assert day >= 3


class TestCmdCheckout:
    @staticmethod
    def _spool_eml_files(data_dir):
        return list((data_dir / "spool").glob("*.eml"))

    @staticmethod
    def _parse_spool_payload(eml_path):
        from email import policy
        from email.parser import BytesParser

        msg = BytesParser(policy=policy.default).parsebytes(eml_path.read_bytes())
        for part in msg.walk():
            if part.get_filename() == "payload.json":
                return json.loads(part.get_content())
        raise ValueError("no payload.json found")

    def test_writes_finished_to_spool(self, data_dir, monkeypatch):
        monkeypatch.setenv("OVERLORD_EXECUTION_ID", "exec-42")
        monkeypatch.setenv("OVERLORD_JOB_NAME", "test-job")
        cmd_checkout()

        files = self._spool_eml_files(data_dir)
        assert len(files) == 1
        payload = self._parse_spool_payload(files[0])
        assert payload["status"] == "finished"
        assert payload["task_id"] == "exec-42"
        assert payload["job_name"] == "test-job"


class TestCmdScanUnfinished:
    def test_returns_only_unfinished(self, data_dir, store, capsys):
        # job-a checks in and checks out
        _deliver_presence(store, "job-a", "task a", "2026-01-01T00:00:00Z", task_id="exec-1")
        _deliver_checkout(store, "job-a", "2026-01-01T01:00:00Z", task_id="exec-1")
        # job-b checks in but never checks out
        _deliver_presence(store, "job-b", "task b", "2026-01-02T00:00:00Z", task_id="exec-2")

        cmd_scan_unfinished()
        output = json.loads(capsys.readouterr().out)
        assert len(output) == 1
        assert output[0]["job_name"] == "job-b"

    def test_empty_when_all_finished(self, data_dir, store, capsys):
        _deliver_presence(store, "job-a", "task a", "2026-01-01T00:00:00Z", task_id="exec-1")
        _deliver_checkout(store, "job-a", "2026-01-01T01:00:00Z", task_id="exec-1")

        cmd_scan_unfinished()
        output = json.loads(capsys.readouterr().out)
        assert output == []

    def test_all_unfinished_when_no_checkouts(self, data_dir, store, capsys):
        _deliver_presence(store, "job-a", "task a", "2026-01-01T00:00:00Z", task_id="exec-1")
        _deliver_presence(store, "job-b", "task b", "2026-01-02T00:00:00Z", task_id="exec-2")

        cmd_scan_unfinished()
        output = json.loads(capsys.readouterr().out)
        assert len(output) == 2

    def test_checkout_does_not_match_different_task_id(self, data_dir, store, capsys):
        _deliver_presence(store, "job-a", "task a", "2026-01-01T00:00:00Z", task_id="exec-1")
        _deliver_checkout(store, "job-a", "2026-01-01T01:00:00Z", task_id="exec-999")

        cmd_scan_unfinished()
        output = json.loads(capsys.readouterr().out)
        assert len(output) == 1
        assert output[0]["task_id"] == "exec-1"
