"""Tests for the MCP server — job registry tools."""

import json

import pytest

from overlord.database import Database
from overlord.mcp_server import create_mcp_server, _job_to_dict, _execution_to_dict
from overlord.models import ExecutionRecord, ExecutionStatus, Job, JobStatus


@pytest.fixture
def db(tmp_path):
    d = Database(db_path=tmp_path / "test.db")
    d.init_schema()
    yield d
    d.close()


@pytest.fixture
def mcp_server(tmp_path):
    """Create an MCP server backed by a temporary database."""
    return create_mcp_server(db_path=tmp_path / "mcp_test.db")


class TestCreateMcpServer:
    def test_server_creates_successfully(self, mcp_server):
        assert mcp_server is not None
        assert mcp_server.name == "overlord-job-registry"


class TestRegisterJob:
    def test_register_basic_job(self, mcp_server):
        tools = {t.name: t for t in mcp_server._tool_manager.list_tools()}
        assert "register_job" in tools

    def test_register_and_retrieve(self, tmp_path):
        server = create_mcp_server(db_path=tmp_path / "test.db")
        # Access the tool functions via the internal database
        db = Database(db_path=tmp_path / "test.db")
        db.init_schema()

        # Call register_job through the server's tool
        # Since tools are plain functions captured in closure, we can test via DB
        job = Job(
            name="my-job",
            cron_expression="0 * * * *",
            command="echo test",
        )
        created = db.create_job(job)
        assert created.id is not None

        fetched = db.get_job_by_name("my-job")
        assert fetched is not None
        assert fetched.command == "echo test"
        db.close()


class TestJobToDict:
    def test_serialisation(self):
        job = Job(
            name="test-job",
            cron_expression="*/5 * * * *",
            command="echo hello",
            id=1,
        )
        d = _job_to_dict(job)
        assert d["name"] == "test-job"
        assert d["cron_expression"] == "*/5 * * * *"
        assert d["command"] == "echo hello"
        assert d["id"] == 1
        assert d["status"] == "enabled"
        assert d["exclusive_lock"] is None

    def test_roundtrip_json(self):
        job = Job(
            name="roundtrip",
            cron_expression="0 0 * * *",
            command="date",
            id=42,
            timeout_seconds=300,
            max_retries=2,
            retry_delay_seconds=10,
        )
        text = json.dumps(_job_to_dict(job))
        parsed = json.loads(text)
        assert parsed["name"] == "roundtrip"
        assert parsed["timeout_seconds"] == 300
        assert parsed["max_retries"] == 2


class TestExecutionToDict:
    def test_serialisation(self):
        rec = ExecutionRecord(
            job_id=1,
            status=ExecutionStatus.SUCCESS,
            started_at="2026-01-01 00:00:00",
            finished_at="2026-01-01 00:01:00",
            exit_code=0,
            stdout="ok",
            stderr="",
            id=10,
        )
        d = _execution_to_dict(rec)
        assert d["id"] == 10
        assert d["status"] == "success"
        assert d["exit_code"] == 0


class TestToolsIntegration:
    """Test the MCP tool functions end-to-end via a shared database."""

    @pytest.fixture
    def tools(self, tmp_path):
        """Return a dict mapping tool-name -> callable, plus the backing DB."""
        db_path = tmp_path / "integration.db"
        server = create_mcp_server(db_path=db_path)
        db = Database(db_path=db_path)
        # DB is already initialised by create_mcp_server

        # Extract the raw tool callables from FastMCP internals
        tool_map = {}
        for t in server._tool_manager.list_tools():
            tool_map[t.name] = t.fn
        return tool_map, db

    def test_register_job(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["register_job"](
            name="cron-job",
            cron_expression="*/10 * * * *",
            command="echo cron",
        ))
        assert result["name"] == "cron-job"
        assert result["id"] is not None
        # Verify via DB
        assert db.get_job_by_name("cron-job") is not None
        db.close()

    def test_register_job_with_options(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["register_job"](
            name="fancy-job",
            cron_expression="0 2 * * *",
            command="backup.sh",
            exclusive_lock="backup",
            timeout_seconds=600,
            max_retries=3,
            retry_delay_seconds=30,
        ))
        assert result["exclusive_lock"] == "backup"
        assert result["timeout_seconds"] == 600
        assert result["max_retries"] == 3
        db.close()

    def test_unregister_job(self, tools):
        tool_map, db = tools
        tool_map["register_job"](
            name="doomed",
            cron_expression="* * * * *",
            command="echo bye",
        )
        result = json.loads(tool_map["unregister_job"](name="doomed"))
        assert result["status"] == "deleted"
        assert db.get_job_by_name("doomed") is None
        db.close()

    def test_unregister_nonexistent(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["unregister_job"](name="ghost"))
        assert "error" in result
        db.close()

    def test_list_jobs(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="j1", cron_expression="* * * * *", command="echo 1")
        tool_map["register_job"](name="j2", cron_expression="* * * * *", command="echo 2")
        result = json.loads(tool_map["list_jobs"]())
        assert len(result) == 2
        names = {j["name"] for j in result}
        assert names == {"j1", "j2"}
        db.close()

    def test_list_jobs_filter_status(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="active", cron_expression="* * * * *", command="echo a")
        # Create a disabled job directly via DB
        disabled_job = Job(
            name="inactive",
            cron_expression="* * * * *",
            command="echo b",
            status=JobStatus.DISABLED,
        )
        db.create_job(disabled_job)

        enabled = json.loads(tool_map["list_jobs"](status="enabled"))
        assert len(enabled) == 1
        assert enabled[0]["name"] == "active"

        disabled = json.loads(tool_map["list_jobs"](status="disabled"))
        assert len(disabled) == 1
        assert disabled[0]["name"] == "inactive"
        db.close()

    def test_list_jobs_invalid_status(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["list_jobs"](status="bogus"))
        assert "error" in result
        db.close()

    def test_get_job_status(self, tools):
        tool_map, db = tools
        tool_map["register_job"](
            name="status-check",
            cron_expression="*/5 * * * *",
            command="echo status",
        )
        result = json.loads(tool_map["get_job_status"](name="status-check"))
        assert result["name"] == "status-check"
        assert result["recent_executions"] == []
        db.close()

    def test_get_job_status_with_executions(self, tools):
        tool_map, db = tools
        tool_map["register_job"](
            name="has-history",
            cron_expression="* * * * *",
            command="echo hi",
        )
        job = db.get_job_by_name("has-history")
        # Simulate some execution history
        rec = db.create_execution(job.id)
        db.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0, stdout="hi\n")

        result = json.loads(tool_map["get_job_status"](name="has-history"))
        assert len(result["recent_executions"]) == 1
        assert result["recent_executions"][0]["status"] == "success"
        db.close()

    def test_get_job_status_nonexistent(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["get_job_status"](name="nope"))
        assert "error" in result
        db.close()
