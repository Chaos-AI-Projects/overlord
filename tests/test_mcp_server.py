"""Tests for the MCP server — job registry tools."""

import asyncio
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


@pytest.fixture
def mcp_server_shared_db(db):
    """Create an MCP server that shares an existing Database instance."""
    return create_mcp_server(db=db, host="127.0.0.1", port=9999)


class TestCreateMcpServer:
    def test_server_creates_successfully(self, mcp_server):
        assert mcp_server is not None
        assert mcp_server.name == "overlord-job-registry"

    def test_server_with_shared_db(self, mcp_server_shared_db):
        assert mcp_server_shared_db is not None
        assert mcp_server_shared_db.name == "overlord-job-registry"
        assert mcp_server_shared_db.settings.host == "127.0.0.1"
        assert mcp_server_shared_db.settings.port == 9999

    def test_server_custom_host_port(self, tmp_path):
        server = create_mcp_server(
            db_path=tmp_path / "custom.db", host="0.0.0.0", port=7777,
        )
        assert server.settings.host == "0.0.0.0"
        assert server.settings.port == 7777

    def test_shared_db_tools_work(self, db, mcp_server_shared_db):
        """Tools on a shared-DB server should read/write the same database."""
        tools = {t.name: t for t in mcp_server_shared_db._tool_manager.list_tools()}
        result = json.loads(tools["register_job"].fn(
            name="shared-job",
            cron_expression="* * * * *",
            command="echo shared",
        ))
        assert result["name"] == "shared-job"
        # Verify the shared db sees the job
        assert db.get_job_by_name("shared-job") is not None


class TestRegisterJob:
    def test_register_basic_job(self, mcp_server):
        tools = {t.name: t for t in mcp_server._tool_manager.list_tools()}
        assert "register_job" in tools

    def test_register_and_retrieve(self, tmp_path):
        server = create_mcp_server(db_path=tmp_path / "test.db")
        # Access the tool functions via the internal database
        db = Database(db_path=tmp_path / "test.db")
        db.init_schema()

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
        assert d["consumes"] == []

    def test_roundtrip_json(self):
        job = Job(
            name="roundtrip",
            cron_expression="0 0 * * *",
            command="date",
            id=42,
            timeout_seconds=300,
            max_retries=2,
            retry_delay_seconds=10,
            consumes=["job-a", "job-b"],
        )
        text = json.dumps(_job_to_dict(job))
        parsed = json.loads(text)
        assert parsed["name"] == "roundtrip"
        assert parsed["timeout_seconds"] == 300
        assert parsed["max_retries"] == 2
        assert parsed["consumes"] == ["job-a", "job-b"]


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
        assert result["consumes"] == []
        # Verify via DB
        assert db.get_job_by_name("cron-job") is not None
        db.close()

    def test_register_job_with_consumes(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["register_job"](
            name="consumer-job",
            cron_expression="*/5 * * * *",
            command="process.sh",
            consumes="job-a, job-b",
        ))
        assert result["consumes"] == ["job-a", "job-b"]
        fetched = db.get_job_by_name("consumer-job")
        assert fetched.consumes == ["job-a", "job-b"]
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

    def test_register_job_duplicate_name(self, tools):
        tool_map, db = tools
        tool_map["register_job"](
            name="dup-job",
            cron_expression="* * * * *",
            command="echo first",
        )
        result = json.loads(tool_map["register_job"](
            name="dup-job",
            cron_expression="*/5 * * * *",
            command="echo second",
        ))
        assert "error" in result
        assert "already exists" in result["error"]
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

    def test_update_job(self, tools):
        tool_map, db = tools
        tool_map["register_job"](
            name="updatable",
            cron_expression="* * * * *",
            command="echo old",
        )
        result = json.loads(tool_map["update_job"](
            name="updatable",
            cron_expression="*/10 * * * *",
            command="echo new",
            timeout_seconds=120,
        ))
        assert result["name"] == "updatable"
        assert result["cron_expression"] == "*/10 * * * *"
        assert result["command"] == "echo new"
        assert result["timeout_seconds"] == 120
        # Verify via DB
        fetched = db.get_job_by_name("updatable")
        assert fetched.cron_expression == "*/10 * * * *"
        assert fetched.command == "echo new"
        db.close()

    def test_update_job_partial(self, tools):
        tool_map, db = tools
        tool_map["register_job"](
            name="partial-update",
            cron_expression="* * * * *",
            command="echo original",
            timeout_seconds=60,
        )
        # Only update cron, command should remain unchanged
        result = json.loads(tool_map["update_job"](
            name="partial-update",
            cron_expression="*/5 * * * *",
        ))
        assert result["cron_expression"] == "*/5 * * * *"
        assert result["command"] == "echo original"
        assert result["timeout_seconds"] == 60
        db.close()

    def test_update_job_nonexistent(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["update_job"](name="ghost"))
        assert "error" in result
        db.close()

    def test_update_job_consumes(self, tools):
        tool_map, db = tools
        tool_map["register_job"](
            name="consumer-update",
            cron_expression="* * * * *",
            command="echo hi",
        )
        result = json.loads(tool_map["update_job"](
            name="consumer-update",
            consumes="job-a,job-b",
        ))
        assert result["consumes"] == ["job-a", "job-b"]
        # Clear consumes
        result = json.loads(tool_map["update_job"](
            name="consumer-update",
            consumes="",
        ))
        assert result["consumes"] == []
        db.close()

    def test_query_messages_all(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="msg-job", cron_expression="* * * * *", command="echo hi")
        job = db.get_job_by_name("msg-job")
        db.create_message(job.id, '{"test": true}', consumer="agent")
        db.create_message(job.id, '{"test": false}', consumer="logger")
        result = json.loads(tool_map["query_messages"]())
        assert len(result) == 2
        assert all(r["source_job_name"] == "msg-job" for r in result)
        db.close()

    def test_query_messages_by_job(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="qm-a", cron_expression="* * * * *", command="echo a")
        tool_map["register_job"](name="qm-b", cron_expression="* * * * *", command="echo b")
        ja = db.get_job_by_name("qm-a")
        jb = db.get_job_by_name("qm-b")
        db.create_message(ja.id, "from-a")
        db.create_message(jb.id, "from-b")
        result = json.loads(tool_map["query_messages"](source_job_name="qm-a"))
        assert len(result) == 1
        assert result[0]["source_job_name"] == "qm-a"
        db.close()

    def test_query_messages_by_consumer(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="qm-c", cron_expression="* * * * *", command="echo c")
        job = db.get_job_by_name("qm-c")
        db.create_message(job.id, "for-agent", consumer="agent")
        db.create_message(job.id, "for-logger", consumer="logger")
        result = json.loads(tool_map["query_messages"](consumer="agent"))
        assert len(result) == 1
        assert result[0]["consumer"] == "agent"
        db.close()

    def test_query_messages_unconsumed(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="qm-d", cron_expression="* * * * *", command="echo d")
        job = db.get_job_by_name("qm-d")
        m1 = db.create_message(job.id, "consumed")
        db.create_message(job.id, "unconsumed")
        db.mark_consumed(m1.id)
        result = json.loads(tool_map["query_messages"](unconsumed=True))
        assert len(result) == 1
        assert result[0]["consumed"] is False
        db.close()

    def test_query_messages_parses_payload(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="qm-e", cron_expression="* * * * *", command="echo e")
        job = db.get_job_by_name("qm-e")
        db.create_message(job.id, '{"key": "value"}')
        result = json.loads(tool_map["query_messages"]())
        assert result[0]["payload"] == {"key": "value"}
        db.close()

    def test_send_message_with_consumer(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["send_message"](
            payload='{"action": "run"}',
            consumer="overlord",
        ))
        assert result["id"] is not None
        assert result["source_job_id"] is None
        assert result["consumer"] == "overlord"
        # Verify in DB
        msgs = db.query_messages(consumer="overlord")
        assert len(msgs) == 1
        assert msgs[0].source_job_id is None
        db.close()

    def test_send_message_without_consumer(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["send_message"](payload="hello"))
        assert result["id"] is not None
        assert result["consumer"] is None
        assert result["source_job_id"] is None
        db.close()

    def test_send_message_picked_up_by_consumer_job(self, tools):
        tool_map, db = tools
        # Send a message addressed to "overlord"
        tool_map["send_message"](payload="kick", consumer="overlord")
        # Verify it appears in unconsumed messages for "overlord" consumer
        msgs = db.fetch_unconsumed_for_consumers(["overlord"])
        assert len(msgs) == 1
        assert msgs[0].payload == "kick"
        assert msgs[0].source_job_id is None
        db.close()

    def test_query_messages_shows_cli_messages(self, tools):
        tool_map, db = tools
        tool_map["send_message"](payload="cli-msg", consumer="test")
        result = json.loads(tool_map["query_messages"]())
        assert len(result) == 1
        assert result[0]["source_job_id"] is None
        assert result[0]["source_job_name"] == "(cli)"
        db.close()

    def test_query_messages_no_consumer(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="nc-job", cron_expression="* * * * *", command="echo x")
        job = db.get_job_by_name("nc-job")
        db.create_message(job.id, "unaddressed")
        db.create_message(job.id, "addressed", consumer="agent")
        result = json.loads(tool_map["query_messages"](no_consumer=True))
        assert len(result) == 1
        assert result[0]["consumer"] is None
        db.close()

    def test_consume_messages(self, tools):
        tool_map, db = tools
        tool_map["register_job"](name="cm-job", cron_expression="* * * * *", command="echo y")
        job = db.get_job_by_name("cm-job")
        db.create_message(job.id, '{"data": 1}', consumer="agent")
        db.create_message(job.id, '{"data": 2}', consumer="agent")
        db.create_message(job.id, '{"data": 3}', consumer="logger")
        result = json.loads(tool_map["consume_messages"](consumer="agent"))
        assert len(result) == 2
        assert all(r["consumed"] is True for r in result)
        # Verify they are actually consumed in DB
        remaining = db.fetch_unconsumed_for_consumers(["agent"])
        assert len(remaining) == 0
        # logger message should still be unconsumed
        logger_msgs = db.fetch_unconsumed_for_consumers(["logger"])
        assert len(logger_msgs) == 1
        db.close()

    def test_consume_messages_empty(self, tools):
        tool_map, db = tools
        result = json.loads(tool_map["consume_messages"]())
        assert result == []
        db.close()

    def test_consume_messages_no_consumer(self, tools):
        tool_map, db = tools
        tool_map["send_message"](payload="unaddressed1")
        tool_map["send_message"](payload="addressed1", consumer="x")
        result = json.loads(tool_map["consume_messages"](no_consumer=True))
        assert len(result) == 1
        assert result[0]["consumer"] is None
        db.close()
