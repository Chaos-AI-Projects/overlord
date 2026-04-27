"""Tests for log rotation: logging_config, MCP tool, and CLI command."""

import json
import logging
from unittest import mock

import pytest

from overlord.cli import build_parser, cmd_rotate_log, main
from overlord.logging_config import JSONFormatter, rotate_log_handler, setup_logging


class TestSetupLogging:
    def setup_method(self):
        # Clear handlers between tests so setup_logging() is not short-circuited.
        root = logging.getLogger("overlord")
        root.handlers.clear()

    def test_stderr_only(self):
        setup_logging()
        root = logging.getLogger("overlord")
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0], logging.StreamHandler)
        assert not isinstance(root.handlers[0], logging.FileHandler)

    def test_with_log_file(self, tmp_path):
        log_file = tmp_path / "overlord.log"
        setup_logging(log_file=str(log_file))
        root = logging.getLogger("overlord")
        assert len(root.handlers) == 2
        file_handlers = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        assert file_handlers[0].baseFilename == str(log_file)
        # Clean up
        for h in root.handlers:
            h.close()

    def test_idempotent(self):
        setup_logging()
        setup_logging()  # should not add duplicate handlers
        root = logging.getLogger("overlord")
        assert len(root.handlers) == 1


class TestRotateLogHandler:
    def setup_method(self):
        root = logging.getLogger("overlord")
        root.handlers.clear()

    def test_no_file_handler(self):
        setup_logging()
        result = rotate_log_handler()
        assert "no file handler" in result

    def test_rotate_reopens_file(self, tmp_path):
        log_file = tmp_path / "overlord.log"
        setup_logging(log_file=str(log_file))
        root = logging.getLogger("overlord")

        # Write a log line
        root.info("before rotation")
        root.handlers[-1].flush()
        assert log_file.read_text().strip()

        # Simulate external rename
        rotated_file = tmp_path / "overlord.log.1"
        log_file.rename(rotated_file)

        # Rotate
        result = rotate_log_handler()
        assert "rotated" in result

        # New log line should go to fresh file
        root.info("after rotation")
        root.handlers[-1].flush()
        assert log_file.exists()
        assert "after rotation" in log_file.read_text()
        assert "before rotation" in rotated_file.read_text()

        for h in root.handlers:
            h.close()


class TestRotateLogMCPTool:
    def test_tool_registered(self, tmp_path):
        from overlord.mcp_server import create_mcp_server

        server = create_mcp_server(data_dir=tmp_path, rotate_log_callback=rotate_log_handler)
        tool_names = [t.name for t in server._tool_manager.list_tools()]
        assert "rotate_log" in tool_names

    def test_tool_no_callback(self, tmp_path):
        from overlord.mcp_server import create_mcp_server

        server = create_mcp_server(data_dir=tmp_path)
        tool_map = {t.name: t.fn for t in server._tool_manager.list_tools()}
        result = json.loads(tool_map["rotate_log"]())
        assert "error" in result

    def test_tool_with_callback(self, tmp_path):
        from overlord.mcp_server import create_mcp_server

        root = logging.getLogger("overlord")
        root.handlers.clear()
        log_file = tmp_path / "overlord.log"
        setup_logging(log_file=str(log_file))

        server = create_mcp_server(data_dir=tmp_path, rotate_log_callback=rotate_log_handler)
        tool_map = {t.name: t.fn for t in server._tool_manager.list_tools()}
        result = json.loads(tool_map["rotate_log"]())
        assert result["status"].startswith("log file rotated: ")

        for h in root.handlers:
            h.close()
        root.handlers.clear()


class TestRotateLogCLI:
    def test_daemon_log_file_arg(self):
        parser = build_parser()
        args = parser.parse_args(["daemon", "--log-file", "/tmp/test.log"])
        assert args.log_file == "/tmp/test.log"

    def test_daemon_log_file_default(self):
        parser = build_parser()
        args = parser.parse_args(["daemon"])
        assert args.log_file == "auto"

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_rotate_log_success(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"status": "log file rotated"})
        from overlord.cli import DEFAULT_MCP_URL
        import argparse
        ns = argparse.Namespace(mcp_url=DEFAULT_MCP_URL)
        cmd_rotate_log(ns)
        mock_call.assert_called_once_with(DEFAULT_MCP_URL, "rotate_log", {})
        assert "log file rotated" in capsys.readouterr().out

    @mock.patch("overlord.cli._call_tool")
    def test_cmd_rotate_log_error(self, mock_call, capsys):
        mock_call.return_value = json.dumps({"error": "not available"})
        from overlord.cli import DEFAULT_MCP_URL
        import argparse
        ns = argparse.Namespace(mcp_url=DEFAULT_MCP_URL)
        with pytest.raises(SystemExit):
            cmd_rotate_log(ns)
        assert "not available" in capsys.readouterr().err

    @mock.patch("overlord.cli.cmd_rotate_log")
    def test_main_hidden_command(self, mock_cmd):
        main(["rotate-log"])
        mock_cmd.assert_called_once()

    @mock.patch("overlord.cli.cmd_rotate_log")
    def test_main_hidden_command_with_mcp_url(self, mock_cmd):
        main(["rotate-log", "--mcp-url", "http://host:9000/mcp/"])
        ns = mock_cmd.call_args[0][0]
        assert ns.mcp_url == "http://host:9000/mcp/"
