"""Tests for the Overlord CLI module."""

import asyncio
import json
import time
from io import StringIO
from unittest import mock

import pytest

from overlord.cli import (
    DEFAULT_MCP_URL,
    _print_job_status,
    _print_job_table,
    _print_messages,
    _print_messages_text,
    build_parser,
    cmd_list,
    cmd_messages,
    cmd_register,
    cmd_status,
    cmd_trigger,
    cmd_unregister,
    cmd_update,
)
from overlord.database import Database
from overlord.mcp_server import create_mcp_server
from overlord.models import ExecutionStatus, Job


# -- Parser tests --


class TestBuildParser:
    def test_daemon_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["daemon"])
        assert args.command == "daemon"
        assert args.tick == 60
        assert args.mcp_host == "127.0.0.1"
        assert args.mcp_port == 8000
        assert args.db is None

    def test_daemon_custom(self):
        parser = build_parser()
        args = parser.parse_args([
            "daemon", "--db", "/tmp/test.db", "--tick", "30",
            "--mcp-host", "0.0.0.0", "--mcp-port", "9000",
        ])
        assert args.db == "/tmp/test.db"
        assert args.tick == 30
        assert args.mcp_host == "0.0.0.0"
        assert args.mcp_port == 9000

    def test_list_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["list"])
        assert args.command == "list"
        assert args.status is None
        assert args.mcp_url == DEFAULT_MCP_URL

    def test_list_with_status(self):
        parser = build_parser()
        args = parser.parse_args(["list", "--status", "enabled"])
        assert args.status == "enabled"

    def test_status_command(self):
        parser = build_parser()
        args = parser.parse_args(["status", "my-job"])
        assert args.command == "status"
        assert args.name == "my-job"

    def test_register_command(self):
        parser = build_parser()
        args = parser.parse_args([
            "register", "--name", "backup", "--cron", "0 2 * * *",
            "--command", "backup.sh", "--lock", "bk", "--timeout", "600",
        ])
        assert args.name == "backup"
        assert args.cron == "0 2 * * *"
        assert args.command == "backup.sh"
        assert args.lock == "bk"
        assert args.timeout == 600

    def test_register_with_consumes(self):
        parser = build_parser()
        args = parser.parse_args([
            "register", "--name", "consumer", "--cron", "*/5 * * * *",
            "--command", "process.sh", "--consumes", "job-a,job-b",
        ])
        assert args.consumes == "job-a,job-b"

    def test_unregister_command(self):
        parser = build_parser()
        args = parser.parse_args(["unregister", "old-job"])
        assert args.name == "old-job"

    def test_trigger_command(self):
        parser = build_parser()
        args = parser.parse_args(["trigger", "my-job"])
        assert args.command == "trigger"
        assert args.name == "my-job"

    def test_messages_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["messages"])
        assert args.command == "messages"
        assert args.job is None
        assert args.consumer is None
        assert args.unconsumed is False
        assert args.limit is None
        assert args.text is False

    def test_messages_text_flag(self):
        parser = build_parser()
        args = parser.parse_args(["messages", "--text"])
        assert args.text is True

    def test_messages_jsonl_flag(self):
        parser = build_parser()
        args = parser.parse_args(["messages", "--jsonl"])
        assert args.jsonl is True
        assert args.text is False

    def test_messages_text_jsonl_mutually_exclusive(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["messages", "--text", "--jsonl"])

    def test_messages_consume_flag(self):
        parser = build_parser()
        args = parser.parse_args(["messages", "--consume"])
        assert args.consume is True

    def test_messages_no_consumer_flag(self):
        parser = build_parser()
        args = parser.parse_args(["messages", "--no-consumer"])
        assert args.no_consumer is True

    def test_messages_consumer_no_consumer_mutually_exclusive(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["messages", "--consumer", "x", "--no-consumer"])

    def test_messages_with_filters(self):
        parser = build_parser()
        args = parser.parse_args([
            "messages", "--job", "my-job", "--consumer", "agent",
            "--unconsumed", "--limit", "10",
        ])
        assert args.job == "my-job"
        assert args.consumer == "agent"
        assert args.unconsumed is True
        assert args.limit == 10

    def test_update_command(self):
        parser = build_parser()
        args = parser.parse_args([
            "update", "--name", "my-job", "--cron", "*/10 * * * *",
            "--command", "new-cmd.sh", "--timeout", "120",
        ])
        # Note: args.command is overridden by --command flag; check func instead
        assert args.func == cmd_update
        assert args.name == "my-job"
        assert args.cron == "*/10 * * * *"
        assert args.command == "new-cmd.sh"
        assert args.timeout == 120

    def test_update_minimal(self):
        parser = build_parser()
        args = parser.parse_args(["update", "--name", "my-job", "--cron", "0 * * * *"])
        assert args.name == "my-job"
        assert args.cron == "0 * * * *"
        assert args.command is None

    def test_custom_mcp_url(self):
        parser = build_parser()
        args = parser.parse_args(["list", "--mcp-url", "http://host:9000/mcp/"])
        assert args.mcp_url == "http://host:9000/mcp/"


# -- Output formatting tests --


class TestPrintJobTable:
    def test_empty_list(self, capsys):
        _print_job_table("[]")
        assert "No jobs found" in capsys.readouterr().out

    def test_normal_output(self, capsys):
        jobs = [
            {"id": 1, "name": "backup", "cron_expression": "0 2 * * *",
             "status": "enabled", "consumes": []},
            {"id": 2, "name": "cleanup", "cron_expression": "0 0 * * 0",
             "status": "disabled", "consumes": ["job-a"]},
        ]
        _print_job_table(json.dumps(jobs))
        out = capsys.readouterr().out
        assert "backup" in out
        assert "cleanup" in out
        assert "ID" in out
        assert "CONSUMES" in out

    def test_error_response(self, capsys):
        with pytest.raises(SystemExit):
            _print_job_table(json.dumps({"error": "something broke"}))
        assert "something broke" in capsys.readouterr().err


class TestPrintJobStatus:
    def test_basic_output(self, capsys):
        data = {
            "name": "my-job",
            "id": 1,
            "status": "enabled",
            "cron_expression": "* * * * *",
            "command": "echo hello",
            "consumes": [],
            "exclusive_lock": None,
            "timeout_seconds": None,
            "max_retries": 0,
            "retry_delay_seconds": 0,
            "created_at": "2026-01-01",
            "updated_at": "2026-01-01",
            "recent_executions": [],
        }
        _print_job_status(json.dumps(data))
        out = capsys.readouterr().out
        assert "my-job" in out
        assert "No recent executions" in out

    def test_with_executions(self, capsys):
        data = {
            "name": "my-job",
            "id": 1,
            "status": "enabled",
            "cron_expression": "* * * * *",
            "command": "echo hello",
            "consumes": ["source-a"],
            "exclusive_lock": "lk",
            "timeout_seconds": 60,
            "max_retries": 2,
            "retry_delay_seconds": 5,
            "created_at": "2026-01-01",
            "updated_at": "2026-01-01",
            "recent_executions": [
                {"id": 10, "status": "success", "exit_code": 0,
                 "started_at": "2026-01-01 00:00:00", "finished_at": "2026-01-01 00:01:00"},
            ],
        }
        _print_job_status(json.dumps(data))
        out = capsys.readouterr().out
        assert "Lock:" in out
        assert "Timeout:" in out
        assert "Retries:" in out
        assert "Consumes:" in out
        assert "source-a" in out
        assert "success" in out


class TestPrintMessages:
    def test_empty_list(self, capsys):
        _print_messages("[]")
        assert "No messages found" in capsys.readouterr().out

    def test_normal_output(self, capsys):
        messages = [
            {"id": 1, "source_job_id": 5, "source_job_name": "my-job", "consumed": False,
             "consumer": "agent", "created_at": "2026-01-01 00:00:00"},
            {"id": 2, "source_job_id": 5, "source_job_name": "my-job", "consumed": True,
             "consumer": None, "created_at": "2026-01-01 00:01:00"},
        ]
        _print_messages(json.dumps(messages))
        out = capsys.readouterr().out
        assert "ID" in out
        assert "JOB" in out
        assert "my-job" in out
        assert "agent" in out
        assert "yes" in out
        assert "no" in out

    def test_error_response(self, capsys):
        with pytest.raises(SystemExit):
            _print_messages(json.dumps({"error": "something broke"}))
        assert "something broke" in capsys.readouterr().err

    def test_text_mode_dispatches(self, capsys):
        messages = [
            {"id": 1, "source_job_name": "my-job", "consumed": False,
             "consumer": "agent", "created_at": "2026-01-01 00:00:00",
             "payload": {"message": "hello world"}},
        ]
        _print_messages(json.dumps(messages), text=True)
        out = capsys.readouterr().out
        assert "--- Message 1 ---" in out
        assert "hello world" in out

    def test_text_mode_empty(self, capsys):
        _print_messages("[]", text=True)
        assert "No messages found" in capsys.readouterr().out


class TestPrintMessagesText:
    def test_single_message_with_dict_payload(self, capsys, monkeypatch):
        monkeypatch.setenv("TZ", "UTC")
        time.tzset()
        messages = [
            {"id": 42, "source_job_name": "my-job", "consumed": False,
             "consumer": "worker-1", "created_at": "2026-04-22 11:48:16",
             "payload": {"message": "task completed"}},
        ]
        _print_messages_text(messages)
        out = capsys.readouterr().out
        assert "--- Message 42 ---" in out
        assert "Job: my-job" in out
        assert "Consumed: no" in out
        assert "Consumer: worker-1" in out
        assert "Created: 2026-04-22 11:48:16" in out
        assert '"message": "task completed"' in out

    def test_multiple_messages(self, capsys):
        messages = [
            {"id": 1, "source_job_name": "job-a", "consumed": True,
             "consumer": None, "created_at": "2026-01-01",
             "payload": "plain text payload"},
            {"id": 2, "source_job_name": "job-b", "consumed": False,
             "consumer": "agent", "created_at": "2026-01-02",
             "payload": {"key": "value"}},
        ]
        _print_messages_text(messages)
        out = capsys.readouterr().out
        assert "--- Message 1 ---" in out
        assert "--- Message 2 ---" in out
        assert "plain text payload" in out
        assert "Consumer: -" in out  # None consumer shows as "-"

    def test_null_payload(self, capsys):
        messages = [
            {"id": 5, "source_job_name": "job-c", "consumed": False,
             "consumer": None, "created_at": "2026-01-01",
             "payload": None},
        ]
        _print_messages_text(messages)
        out = capsys.readouterr().out
        assert "(no payload)" in out

    def test_list_payload(self, capsys):
        messages = [
            {"id": 6, "source_job_name": "job-d", "consumed": False,
             "consumer": None, "created_at": "2026-01-01",
             "payload": [1, 2, 3]},
        ]
        _print_messages_text(messages)
        out = capsys.readouterr().out
        assert "[\n  1,\n  2,\n  3\n]" in out


# -- Command handlers with mocked MCP calls --


class TestCommandHandlers:
    """Test command handlers by patching _call_tool."""

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_list(self, mock_call, capsys):
        jobs = [{"id": 1, "name": "j1", "cron_expression": "* * * * *",
                 "status": "enabled", "consumes": []}]
        mock_call.return_value = json.dumps(jobs)
        args = build_parser().parse_args(["list"])
        cmd_list(args)
        mock_call.assert_called_once()
        assert mock_call.call_args[0][1] == "list_jobs"
        assert "j1" in capsys.readouterr().out

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_list_with_status_filter(self, mock_call, capsys):
        mock_call.return_value = "[]"
        args = build_parser().parse_args(["list", "--status", "paused"])
        cmd_list(args)
        assert mock_call.call_args[0][2] == {"status": "paused"}

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_status(self, mock_call, capsys):
        data = {
            "name": "j1", "id": 1, "status": "enabled",
            "cron_expression": "* * * * *", "command": "echo hi",
            "consumes": [],
            "exclusive_lock": None, "timeout_seconds": None,
            "max_retries": 0, "retry_delay_seconds": 0,
            "created_at": "2026-01-01", "updated_at": "2026-01-01",
            "recent_executions": [],
        }
        mock_call.return_value = json.dumps(data)
        args = build_parser().parse_args(["status", "j1"])
        cmd_status(args)
        mock_call.assert_called_once()
        assert mock_call.call_args[0][2] == {"name": "j1"}

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_register(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"name": "new-job", "id": 5})
        args = build_parser().parse_args([
            "register", "--name", "new-job", "--cron", "* * * * *", "--command", "echo hi",
        ])
        cmd_register(args)
        call_args = mock_call.call_args[0][2]
        assert call_args["name"] == "new-job"
        assert call_args["cron_expression"] == "* * * * *"
        assert "Registered" in capsys.readouterr().out

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_register_with_consumes(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"name": "consumer-job", "id": 6})
        args = build_parser().parse_args([
            "register", "--name", "consumer-job", "--cron", "*/5 * * * *",
            "--command", "process.sh", "--consumes", "job-a,job-b",
        ])
        cmd_register(args)
        call_args = mock_call.call_args[0][2]
        assert call_args["consumes"] == "job-a,job-b"

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_register_error(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"error": "Job 'dup' already exists"})
        args = build_parser().parse_args([
            "register", "--name", "dup", "--cron", "* * * * *", "--command", "echo",
        ])
        with pytest.raises(SystemExit):
            cmd_register(args)
        assert "already exists" in capsys.readouterr().err

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_unregister(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"status": "deleted", "name": "old", "id": 3})
        args = build_parser().parse_args(["unregister", "old"])
        cmd_unregister(args)
        assert "Unregistered" in capsys.readouterr().out

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages(self, mock_call, capsys):
        messages = [{"id": 1, "source_job_id": 3, "consumed": False,
                     "consumer": "agent", "created_at": "2026-01-01"}]
        mock_call.return_value = json.dumps(messages)
        args = build_parser().parse_args(["messages", "--job", "my-job", "--unconsumed"])
        cmd_messages(args)
        call_args = mock_call.call_args[0][2]
        assert call_args["source_job_name"] == "my-job"
        assert call_args["unconsumed"] is True
        assert "agent" in capsys.readouterr().out

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages_no_filters(self, mock_call, capsys):
        mock_call.return_value = "[]"
        args = build_parser().parse_args(["messages"])
        cmd_messages(args)
        assert mock_call.call_args[0][2] == {}

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages_text_flag(self, mock_call, capsys):
        messages = [{"id": 1, "source_job_name": "my-job", "consumed": False,
                     "consumer": "agent", "created_at": "2026-01-01",
                     "payload": {"message": "hello"}}]
        mock_call.return_value = json.dumps(messages)
        args = build_parser().parse_args(["messages", "--text"])
        cmd_messages(args)
        out = capsys.readouterr().out
        assert "--- Message 1 ---" in out
        assert "hello" in out

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages_jsonl_flag(self, mock_call, capsys):
        messages = [{"id": 1, "source_job_name": "my-job", "consumed": False,
                     "consumer": "agent", "created_at": "2026-01-01",
                     "payload": {"key": "val"}}]
        mock_call.return_value = json.dumps(messages)
        args = build_parser().parse_args(["messages", "--jsonl"])
        cmd_messages(args)
        out = capsys.readouterr().out
        lines = [line for line in out.strip().split("\n") if line]
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["payload"]["key"] == "val"

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages_jsonl_empty(self, mock_call, capsys):
        mock_call.return_value = "[]"
        args = build_parser().parse_args(["messages", "--jsonl"])
        cmd_messages(args)
        out = capsys.readouterr().out
        assert out.strip() == ""

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages_consume_flag(self, mock_call, capsys):
        messages = [{"id": 1, "source_job_name": "my-job", "consumed": True,
                     "consumer": None, "created_at": "2026-01-01"}]
        mock_call.return_value = json.dumps(messages)
        args = build_parser().parse_args(["messages", "--consume", "--no-consumer"])
        cmd_messages(args)
        assert mock_call.call_args[0][1] == "consume_messages"

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages_consume_strips_unconsumed(self, mock_call, capsys):
        mock_call.return_value = "[]"
        args = build_parser().parse_args(["messages", "--consume", "--unconsumed", "--no-consumer"])
        cmd_messages(args)
        call_args = mock_call.call_args[0][2]
        assert "unconsumed" not in call_args

    def test_cmd_messages_consume_requires_consumer_filter(self, capsys):
        args = build_parser().parse_args(["messages", "--consume"])
        with pytest.raises(SystemExit):
            cmd_messages(args)

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_messages_no_consumer_flag(self, mock_call, capsys):
        mock_call.return_value = "[]"
        args = build_parser().parse_args(["messages", "--no-consumer"])
        cmd_messages(args)
        call_args = mock_call.call_args[0][2]
        assert call_args["no_consumer"] is True

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_update(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"name": "my-job", "id": 3})
        args = build_parser().parse_args([
            "update", "--name", "my-job", "--cron", "*/10 * * * *",
        ])
        cmd_update(args)
        call_args = mock_call.call_args[0][2]
        assert call_args["name"] == "my-job"
        assert call_args["cron_expression"] == "*/10 * * * *"
        assert "Updated" in capsys.readouterr().out

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_update_error(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"error": "Job 'ghost' not found"})
        args = build_parser().parse_args([
            "update", "--name", "ghost", "--cron", "* * * * *",
        ])
        with pytest.raises(SystemExit):
            cmd_update(args)
        assert "not found" in capsys.readouterr().err

    def test_cmd_update_no_fields(self, capsys):
        args = build_parser().parse_args(["update", "--name", "my-job"])
        with pytest.raises(SystemExit):
            cmd_update(args)
        assert "no fields to update" in capsys.readouterr().err

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_trigger(self, mock_call, capsys):
        mock_call.return_value = json.dumps({
            "status": "triggered",
            "job": {"name": "t1", "id": 1},
        })
        args = build_parser().parse_args(["trigger", "t1"])
        cmd_trigger(args)
        out = capsys.readouterr().out
        assert "Triggered" in out
        assert "t1" in out


# -- trigger_job MCP tool test --


class TestTriggerJobTool:
    """Test the trigger_job tool via the MCP server internals."""

    @pytest.fixture
    def setup(self, tmp_path):
        db_path = tmp_path / "trigger_test.db"
        server = create_mcp_server(db_path=db_path)
        db = Database(db_path=db_path)
        tool_map = {}
        for t in server._tool_manager.list_tools():
            tool_map[t.name] = t.fn
        yield tool_map, db
        db.close()

    def test_trigger_job_exists(self, setup):
        tool_map, db = setup
        assert "trigger_job" in tool_map

    def test_trigger_nonexistent_job(self, setup):
        tool_map, db = setup
        result = json.loads(asyncio.run(tool_map["trigger_job"](name="ghost")))
        assert "error" in result

    def test_trigger_existing_job(self, setup):
        tool_map, db = setup
        # Register a quick job
        tool_map["register_job"](
            name="quick",
            cron_expression="* * * * *",
            command="echo triggered",
        )
        result = json.loads(asyncio.run(tool_map["trigger_job"](name="quick")))
        assert result["status"] == "triggered"
        assert result["job"]["name"] == "quick"
