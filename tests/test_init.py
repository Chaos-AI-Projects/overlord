"""Tests for the `overlord init` command."""

import json
import sqlite3
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
def default_db_path(tmp_path, monkeypatch):
    """Redirect DEFAULT_DB_PATH to a temp directory for test isolation."""
    db_path = tmp_path / "data" / "overlord" / "overlord.db"
    import overlord.database as db_mod

    monkeypatch.setattr(db_mod, "DEFAULT_DB_PATH", db_path)
    return db_path


class TestCmdInit:
    def test_creates_vault_structure(self, tmp_path, default_db_path):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        assert (vault / "CLAUDE.md").exists()
        assert (vault / "overlord_job.sh").exists()
        # DB is now at DEFAULT_DB_PATH, not in the vault
        assert default_db_path.exists()

    def test_claude_md_content(self, tmp_path, default_db_path):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        content = (vault / "CLAUDE.md").read_text()
        assert "overlord" in content.lower()
        assert "overlord register" in content

    def test_wrapper_script_executable(self, tmp_path, default_db_path):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        import os
        mode = os.stat(vault / "overlord_job.sh").st_mode
        assert mode & 0o111  # executable bit set

    def test_registers_overlord_job(self, tmp_path, default_db_path):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        conn = sqlite3.connect(str(default_db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM jobs WHERE name = 'overlord'").fetchone()
        conn.close()

        assert row is not None
        assert row["cron_expression"] == "*/5 * * * *"
        assert json.loads(row["consumes"]) == ["overlord"]
        assert row["status"] == "enabled"

    def test_idempotent(self, tmp_path, default_db_path, capsys):
        """Running init twice should not duplicate files or jobs."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)
        cmd_init(args)

        out = capsys.readouterr().out
        assert "already exists" in out

        # Still only one job
        conn = sqlite3.connect(str(default_db_path))
        count = conn.execute("SELECT COUNT(*) FROM jobs WHERE name = 'overlord'").fetchone()[0]
        conn.close()
        assert count == 1

    def test_db_schema_version(self, tmp_path, default_db_path):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        conn = sqlite3.connect(str(default_db_path))
        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        conn.close()

        from overlord.database import SCHEMA_VERSION
        assert version == SCHEMA_VERSION
