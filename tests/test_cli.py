"""Tests for the Overlord CLI module."""

import asyncio
import json
from io import StringIO
from unittest import mock

import pytest

from overlord.cli import (
    DEFAULT_MCP_URL,
    _print_job_status,
    _print_job_table,
    _print_messages,
    build_parser,
    cmd_list,
    cmd_messages,
    cmd_register,
    cmd_status,
    cmd_trigger,
    cmd_unregister,
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
            {"id": 1, "name": "backup", "cron_expression": "0 2 * * *", "status": "enabled"},
            {"id": 2, "name": "cleanup", "cron_expression": "0 0 * * 0", "status": "disabled"},
        ]
        _print_job_table(json.dumps(jobs))
        out = capsys.readouterr().out
        assert "backup" in out
        assert "cleanup" in out
        assert "ID" in out

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
        assert "success" in out


class TestPrintMessages:
    def test_empty_list(self, capsys):
        _print_messages("[]")
        assert "No messages found" in capsys.readouterr().out

    def test_normal_output(self, capsys):
        messages = [
            {"id": 1, "source_job_id": 5, "source_job_name": "my-job", "consumed": False,
             "consumers": ["agent", "logger"], "created_at": "2026-01-01 00:00:00"},
            {"id": 2, "source_job_id": 5, "source_job_name": "my-job", "consumed": True,
             "consumers": [], "created_at": "2026-01-01 00:01:00"},
        ]
        _print_messages(json.dumps(messages))
        out = capsys.readouterr().out
        assert "ID" in out
        assert "JOB" in out
        assert "my-job" in out
        assert "agent, logger" in out
        assert "yes" in out
        assert "no" in out

    def test_error_response(self, capsys):
        with pytest.raises(SystemExit):
            _print_messages(json.dumps({"error": "something broke"}))
        assert "something broke" in capsys.readouterr().err


# -- Command handlers with mocked MCP calls --


class TestCommandHandlers:
    """Test command handlers by patching _call_tool."""

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_list(self, mock_call, capsys):
        jobs = [{"id": 1, "name": "j1", "cron_expression": "* * * * *", "status": "enabled"}]
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
                     "consumers": ["agent"], "created_at": "2026-01-01"}]
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
