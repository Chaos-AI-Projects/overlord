"""Tests for the `overlord init` command."""

import json
from pathlib import Path

import pytest

from overlord.cli import build_parser, cmd_init


class TestInitParser:
    def test_init_default_path(self):
        parser = build_parser()
        args = parser.parse_args(["init"])
        assert args.command == "init"
        assert args.path == "."

    def test_init_custom_path(self):
        parser = build_parser()
        args = parser.parse_args(["init", "/tmp/my-vault"])
        assert args.path == "/tmp/my-vault"


@pytest.fixture()
def default_data_dir(tmp_path, monkeypatch):
    """Redirect DEFAULT_DATA_DIR to a temp directory for test isolation."""
    data_dir = tmp_path / "data" / "overlord"
    import overlord.job_store as js_mod

    monkeypatch.setattr(js_mod, "DEFAULT_DATA_DIR", data_dir)
    return data_dir


class TestCmdInit:
    def test_creates_vault_structure(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        assert (vault / "CLAUDE.md").exists()
        assert (vault / "overlord_job.sh").exists()
        # Job store directory should exist
        assert (default_data_dir / "jobs").exists()

    def test_claude_md_content(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        content = (vault / "CLAUDE.md").read_text()
        assert "overlord" in content.lower()
        assert "Job Output Format" in content
        assert "periodical" in content.lower()

    def test_skills_installed(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        commands_dir = vault / ".claude" / "commands"
        assert commands_dir.exists()
        assert (commands_dir / "register-job.md").exists()
        assert (commands_dir / "unregister-job.md").exists()
        assert (commands_dir / "update-job.md").exists()
        assert (commands_dir / "rotate-log.md").exists()

    def test_skills_content(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        register = (vault / ".claude" / "commands" / "register-job.md").read_text()
        assert "overlord register" in register
        update = (vault / ".claude" / "commands" / "update-job.md").read_text()
        assert "overlord update" in update

    def test_wrapper_script_executable(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        import os
        mode = os.stat(vault / "overlord_job.sh").st_mode
        assert mode & 0o111  # executable bit set

    def test_registers_overlord_job(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        job_file = default_data_dir / "jobs" / "overlord.json"
        assert job_file.exists()

        data = json.loads(job_file.read_text())
        assert data["cron_expression"] == "*/5 * * * *"
        assert data["consumes"] == ["overlord"]
        assert data["status"] == "enabled"

    def test_idempotent(self, tmp_path, default_data_dir, capsys):
        """Running init twice should not duplicate files or jobs."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)
        cmd_init(args)

        out = capsys.readouterr().out
        assert "already exists" in out

        # Still only one job file
        jobs_dir = default_data_dir / "jobs"
        job_files = [f for f in jobs_dir.iterdir() if f.suffix == ".json"]
        assert len(job_files) == 1
