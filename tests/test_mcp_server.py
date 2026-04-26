"""Tests for the MCP server — job registry tools."""

import asyncio
import json

import pytest

from overlord.execution_log import ExecutionLog
from overlord.job_store import JobStore
from overlord.maildir import MaildirStore
from overlord.mcp_server import create_mcp_server, _job_to_dict, _execution_to_dict
from overlord.models import ExecutionRecord, ExecutionStatus, Job, JobStatus


@pytest.fixture
def job_store(tmp_path):
    return JobStore(data_dir=tmp_path)


@pytest.fixture
def execution_log(tmp_path):
    return ExecutionLog(data_dir=tmp_path)


@pytest.fixture
def mcp_server(tmp_path):
    """Create an MCP server backed by a temporary data directory."""
    return create_mcp_server(data_dir=tmp_path)


@pytest.fixture
def mcp_server_shared(tmp_path):
    """Create an MCP server that shares existing store instances."""
    js = JobStore(data_dir=tmp_path)
    el = ExecutionLog(data_dir=tmp_path)
    return create_mcp_server(
        job_store=js, execution_log=el,
        host="127.0.0.1", port=9999,
    ), js


class TestCreateMcpServer:
    def test_server_creates_successfully(self, mcp_server):
        assert mcp_server is not None
        assert mcp_server.name == "overlord-job-registry"

    def test_server_with_shared_stores(self, mcp_server_shared):
        server, js = mcp_server_shared
        assert server is not None
        assert server.name == "overlord-job-registry"
        assert server.settings.host == "127.0.0.1"
        assert server.settings.port == 9999

    def test_server_custom_host_port(self, tmp_path):
        server = create_mcp_server(
            data_dir=tmp_path, host="0.0.0.0", port=7777,
        )
        assert server.settings.host == "0.0.0.0"
        assert server.settings.port == 7777

    def test_shared_stores_tools_work(self, mcp_server_shared):
        """Tools on a shared-store server should read/write the same store."""
        server, js = mcp_server_shared
        tools = {t.name: t for t in server._tool_manager.list_tools()}
        result = json.loads(tools["register_job"].fn(
            name="shared-job",
            cron_expression="* * * * *",
            command="echo shared",
        ))
        assert result["name"] == "shared-job"
        # Verify the shared store sees the job
        assert js.get_job_by_name("shared-job") is not None


class TestRegisterJob:
    def test_register_basic_job(self, mcp_server):
        tools = {t.name: t for t in mcp_server._tool_manager.list_tools()}
        assert "register_job" in tools

    def test_register_and_retrieve(self, tmp_path):
        server = create_mcp_server(data_dir=tmp_path)
        js = JobStore(data_dir=tmp_path)

        job = Job(
            name="my-job",
            cron_expression="0 * * * *",
            command="echo test",
        )
        created = js.create_job(job)
        assert created.name is not None

        fetched = js.get_job_by_name("my-job")
        assert fetched is not None
        assert fetched.command == "echo test"


class TestJobToDict:
    def test_serialisation(self):
        job = Job(
            name="test-job",
            cron_expression="*/5 * * * *",
            command="echo hello",
        )
        d = _job_to_dict(job)
        assert d["name"] == "test-job"
        assert d["cron_expression"] == "*/5 * * * *"
        assert d["command"] == "echo hello"
        assert d["status"] == "enabled"
        assert d["exclusive_lock"] is None
        assert d["consumes"] == []

    def test_roundtrip_json(self):
        job = Job(
            name="roundtrip",
            cron_expression="0 0 * * *",
            command="date",
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
            job_id=0,
            job_name="test-job",
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
        assert d["job_name"] == "test-job"


class TestToolsIntegration:
    """Test the MCP tool functions end-to-end via shared stores."""

    @pytest.fixture
    def tools(self, tmp_path):
        """Return a dict mapping tool-name -> callable, plus the backing JobStore and MaildirStore."""
        server = create_mcp_server(data_dir=tmp_path)
        js = JobStore(data_dir=tmp_path)
        el = ExecutionLog(data_dir=tmp_path)
        store = MaildirStore(data_dir=tmp_path)

        # Extract the raw tool callables from FastMCP internals
        tool_map = {}
        for t in server._tool_manager.list_tools():
            tool_map[t.name] = t.fn
        return tool_map, js, el, store

    def test_register_job(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["register_job"](
            name="cron-job",
            cron_expression="*/10 * * * *",
            command="echo cron",
        ))
        assert result["name"] == "cron-job"
        assert result["consumes"] == []
        # Verify via store
        assert js.get_job_by_name("cron-job") is not None

    def test_register_job_with_consumes(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["register_job"](
            name="consumer-job",
            cron_expression="*/5 * * * *",
            command="process.sh",
            consumes="job-a, job-b",
        ))
        assert result["consumes"] == ["job-a", "job-b"]
        fetched = js.get_job_by_name("consumer-job")
        assert fetched.consumes == ["job-a", "job-b"]

    def test_register_job_with_options(self, tools):
        tool_map, js, el, store = tools
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

    def test_register_job_duplicate_name(self, tools):
        tool_map, js, el, store = tools
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

    def test_unregister_job(self, tools):
        tool_map, js, el, store = tools
        tool_map["register_job"](
            name="doomed",
            cron_expression="* * * * *",
            command="echo bye",
        )
        result = json.loads(tool_map["unregister_job"](name="doomed"))
        assert result["status"] == "deleted"
        assert js.get_job_by_name("doomed") is None

    def test_unregister_nonexistent(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["unregister_job"](name="ghost"))
        assert "error" in result

    def test_list_jobs(self, tools):
        tool_map, js, el, store = tools
        tool_map["register_job"](name="j1", cron_expression="* * * * *", command="echo 1")
        tool_map["register_job"](name="j2", cron_expression="* * * * *", command="echo 2")
        result = json.loads(tool_map["list_jobs"]())
        assert len(result) == 2
        names = {j["name"] for j in result}
        assert names == {"j1", "j2"}

    def test_list_jobs_filter_status(self, tools):
        tool_map, js, el, store = tools
        tool_map["register_job"](name="active", cron_expression="* * * * *", command="echo a")
        # Create a disabled job directly via store
        disabled_job = Job(
            name="inactive",
            cron_expression="* * * * *",
            command="echo b",
            status=JobStatus.DISABLED,
        )
        js.create_job(disabled_job)

        enabled = json.loads(tool_map["list_jobs"](status="enabled"))
        assert len(enabled) == 1
        assert enabled[0]["name"] == "active"

        disabled = json.loads(tool_map["list_jobs"](status="disabled"))
        assert len(disabled) == 1
        assert disabled[0]["name"] == "inactive"

    def test_list_jobs_invalid_status(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["list_jobs"](status="bogus"))
        assert "error" in result

    def test_get_job_status(self, tools):
        tool_map, js, el, store = tools
        tool_map["register_job"](
            name="status-check",
            cron_expression="*/5 * * * *",
            command="echo status",
        )
        result = json.loads(tool_map["get_job_status"](name="status-check"))
        assert result["name"] == "status-check"
        assert result["recent_executions"] == []

    def test_get_job_status_with_executions(self, tools):
        tool_map, js, el, store = tools
        tool_map["register_job"](
            name="has-history",
            cron_expression="* * * * *",
            command="echo hi",
        )
        # Simulate some execution history
        rec = el.create_execution("has-history")
        el.finish_execution(rec.id, ExecutionStatus.SUCCESS, exit_code=0, stdout="hi\n")

        result = json.loads(tool_map["get_job_status"](name="has-history"))
        assert len(result["recent_executions"]) == 1
        assert result["recent_executions"][0]["status"] == "success"

    def test_get_job_status_nonexistent(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["get_job_status"](name="nope"))
        assert "error" in result

    def test_update_job(self, tools):
        tool_map, js, el, store = tools
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
        # Verify via store
        fetched = js.get_job_by_name("updatable")
        assert fetched.cron_expression == "*/10 * * * *"
        assert fetched.command == "echo new"

    def test_update_job_partial(self, tools):
        tool_map, js, el, store = tools
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

    def test_update_job_nonexistent(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["update_job"](name="ghost"))
        assert "error" in result

    def test_update_job_consumes(self, tools):
        tool_map, js, el, store = tools
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

    def _deliver(self, store, payload, consumer=None, job_name="test-job"):
        """Helper to deliver a message directly to Maildir."""
        msg = MaildirStore.build_message(
            payload=payload, consumer=consumer, job_name=job_name,
        )
        store.deliver(msg, consumer=consumer)

    def test_query_messages_all(self, tools):
        tool_map, js, el, store = tools
        self._deliver(store, '{"test": true}', consumer="agent", job_name="msg-job")
        self._deliver(store, '{"test": false}', consumer="logger", job_name="msg-job")
        result = json.loads(tool_map["query_messages"]())
        assert len(result) == 2
        assert all(r["source_job_name"] == "msg-job" for r in result)

    def test_query_messages_by_job(self, tools):
        tool_map, js, el, store = tools
        self._deliver(store, "from-a", job_name="qm-a")
        self._deliver(store, "from-b", job_name="qm-b")
        result = json.loads(tool_map["query_messages"](source_job_name="qm-a"))
        assert len(result) == 1
        assert result[0]["source_job_name"] == "qm-a"

    def test_query_messages_by_consumer(self, tools):
        tool_map, js, el, store = tools
        self._deliver(store, "for-agent", consumer="agent", job_name="qm-c")
        self._deliver(store, "for-logger", consumer="logger", job_name="qm-c")
        result = json.loads(tool_map["query_messages"](consumer="agent"))
        assert len(result) == 1
        assert result[0]["consumer"] == "agent"

    def test_query_messages_unconsumed(self, tools):
        tool_map, js, el, store = tools
        # Deliver two messages to "agent" mailbox
        self._deliver(store, "will-consume", consumer="agent", job_name="qm-d")
        self._deliver(store, "unconsumed", consumer="agent", job_name="qm-d")
        # Consume one by moving to processed
        msgs = store.fetch_messages("agent")
        store.consume("agent", msgs[0]["key"])
        result = json.loads(tool_map["query_messages"](unconsumed=True))
        assert len(result) == 1
        assert result[0]["consumed"] is False

    def test_query_messages_parses_payload(self, tools):
        tool_map, js, el, store = tools
        self._deliver(store, '{"key": "value"}', job_name="qm-e")
        result = json.loads(tool_map["query_messages"]())
        assert result[0]["payload"] == {"key": "value"}

    def test_send_message_with_consumer(self, tools, tmp_path):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["send_message"](
            payload='{"action": "run"}',
            consumer="overlord",
        ))
        assert result["id"] is not None
        assert result["consumer"] == "overlord"
        # Verify in spool directory
        spool_dir = tmp_path / "spool"
        spool_files = list(spool_dir.glob("*.eml"))
        assert len(spool_files) == 1

    def test_send_message_without_consumer(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["send_message"](payload="hello"))
        assert result["id"] is not None
        assert result["consumer"] is None

    def test_send_message_picked_up_by_consumer_job(self, tools, tmp_path):
        tool_map, js, el, store = tools
        # Send a message addressed to "overlord" (lands in spool)
        tool_map["send_message"](payload="kick", consumer="overlord")
        # Verify spool file contains the right headers
        spool_dir = tmp_path / "spool"
        spool_files = list(spool_dir.glob("*.eml"))
        assert len(spool_files) == 1
        content = spool_files[0].read_text()
        assert "X-Overlord-Consumer: overlord" in content

    def test_query_messages_shows_cli_messages(self, tools):
        tool_map, js, el, store = tools
        # Deliver a CLI message directly to Maildir
        self._deliver(store, "cli-msg", consumer="test", job_name="cli")
        result = json.loads(tool_map["query_messages"]())
        assert len(result) == 1
        assert result[0]["source_job_name"] == "cli"

    def test_query_messages_no_consumer(self, tools):
        tool_map, js, el, store = tools
        # Deliver one unaddressed (catchall) and one addressed message
        self._deliver(store, "unaddressed", job_name="nc-job")
        self._deliver(store, "addressed", consumer="agent", job_name="nc-job")
        result = json.loads(tool_map["query_messages"](no_consumer=True))
        assert len(result) == 1
        assert result[0]["consumer"] is None

    def test_consume_messages(self, tools):
        tool_map, js, el, store = tools
        self._deliver(store, '{"data": 1}', consumer="agent", job_name="cm-job")
        self._deliver(store, '{"data": 2}', consumer="agent", job_name="cm-job")
        self._deliver(store, '{"data": 3}', consumer="logger", job_name="cm-job")
        result = json.loads(tool_map["consume_messages"](consumer="agent"))
        assert len(result) == 2
        assert all(r["consumed"] is True for r in result)
        # Verify they are actually consumed (moved to processed)
        remaining = store.fetch_messages("agent")
        assert len(remaining) == 0
        # logger message should still be unconsumed
        logger_msgs = store.fetch_messages("logger")
        assert len(logger_msgs) == 1

    def test_consume_messages_empty(self, tools):
        tool_map, js, el, store = tools
        result = json.loads(tool_map["consume_messages"]())
        assert result == []

    def test_consume_messages_no_consumer(self, tools):
        tool_map, js, el, store = tools
        self._deliver(store, "unaddressed1")
        self._deliver(store, "addressed1", consumer="x")
        result = json.loads(tool_map["consume_messages"](no_consumer=True))
        assert len(result) == 1
        assert result[0]["consumer"] is None
