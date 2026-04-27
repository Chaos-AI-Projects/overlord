"""Tests for the `overlord init` command."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

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

        # Still only one job file
        jobs_dir = default_data_dir / "jobs"
        job_files = [f for f in jobs_dir.iterdir() if f.suffix == ".json"]
        assert len(job_files) == 1


class TestOriginDirectory:
    """Tests for the origin/ template tracking directory."""

    def test_origin_created(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        origin = vault / "origin"
        assert origin.exists()
        assert (origin / "CLAUDE.md").exists()
        assert (origin / ".claude" / "commands" / "register-job.md").exists()
        assert (origin / "overlord_job.sh").exists()

    def test_origin_matches_working_copy_on_fresh_init(self, tmp_path, default_data_dir):
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        assert (vault / "CLAUDE.md").read_bytes() == (vault / "origin" / "CLAUDE.md").read_bytes()
        assert (vault / ".claude" / "commands" / "register-job.md").read_bytes() == \
            (vault / "origin" / ".claude" / "commands" / "register-job.md").read_bytes()


class TestAutoMerge:
    """Tests for the auto-merge behavior on re-init."""

    def test_unmodified_file_gets_updated(self, tmp_path, default_data_dir, capsys):
        """Re-init with a new template version updates unmodified working copies."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        # Verify initial content
        original = (vault / "CLAUDE.md").read_bytes()
        assert original == (vault / "origin" / "CLAUDE.md").read_bytes()

        # Simulate a template upgrade by modifying the origin/ file
        # (In real use, the new package version would write different content)
        # We'll monkey-patch VAULT_CLAUDE_MD for the second init
        import overlord.vault_template as vt_mod
        old_template = vt_mod.VAULT_CLAUDE_MD
        try:
            vt_mod.VAULT_CLAUDE_MD = old_template + "\n<!-- upgraded -->\n"
            cmd_init(args)
        finally:
            vt_mod.VAULT_CLAUDE_MD = old_template

        out = capsys.readouterr().out
        assert "Updated CLAUDE.md" in out
        # Working copy should have the new content
        assert (vault / "CLAUDE.md").read_text().endswith("<!-- upgraded -->\n")

    def test_modified_file_is_skipped(self, tmp_path, default_data_dir, capsys):
        """Re-init skips files that were locally modified."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        # User modifies a skill file
        skill = vault / ".claude" / "commands" / "register-job.md"
        skill.write_text("# My custom version\n")

        # Re-init (same templates, but the working copy differs from origin/)
        cmd_init(args)

        out = capsys.readouterr().out
        assert "Skipped (locally modified): .claude/commands/register-job.md" in out
        # Working copy should still have user's changes
        assert skill.read_text() == "# My custom version\n"

    def test_modified_file_new_version_in_origin(self, tmp_path, default_data_dir):
        """When a file is locally modified, origin/ still gets the latest template."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        # User modifies CLAUDE.md
        (vault / "CLAUDE.md").write_text("custom content")

        # Simulate template upgrade
        import overlord.vault_template as vt_mod
        old_template = vt_mod.VAULT_CLAUDE_MD
        try:
            vt_mod.VAULT_CLAUDE_MD = "NEW TEMPLATE CONTENT"
            cmd_init(args)
        finally:
            vt_mod.VAULT_CLAUDE_MD = old_template

        # origin/ has the new version, working copy untouched
        assert (vault / "origin" / "CLAUDE.md").read_text() == "NEW TEMPLATE CONTENT"
        assert (vault / "CLAUDE.md").read_text() == "custom content"


class TestGitFallback:
    """Tests for behavior when git is not available."""

    def test_fallback_without_git(self, tmp_path, default_data_dir, capsys):
        """When git is unavailable, init still works (no commits, no git init)."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])

        with patch("overlord.cli._git_available", return_value=False):
            cmd_init(args)

        out = capsys.readouterr().out
        assert "git not found" in out
        # Files should still be created
        assert (vault / "CLAUDE.md").exists()
        assert (vault / ".claude" / "commands" / "register-job.md").exists()
        assert (vault / "origin" / "CLAUDE.md").exists()
        # No .git directory in the vault
        assert not (vault / ".git").exists()

    def test_fallback_still_tracks_origin(self, tmp_path, default_data_dir):
        """Even without git, origin/ is written for future comparison."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])

        with patch("overlord.cli._git_available", return_value=False):
            cmd_init(args)

        assert (vault / "origin" / "CLAUDE.md").read_bytes() == (vault / "CLAUDE.md").read_bytes()


class TestGitIntegration:
    """Tests for git repo initialization and commits."""

    def _git(self, cwd, *git_args):
        return subprocess.run(
            ["git"] + list(git_args),
            cwd=cwd,
            capture_output=True,
            text=True,
        )

    def test_git_init_creates_repo(self, tmp_path, default_data_dir):
        """Init creates a git repo if the vault is not inside one."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        r = self._git(vault, "rev-parse", "--is-inside-work-tree")
        assert r.stdout.strip() == "true"

    def test_git_commits_created(self, tmp_path, default_data_dir):
        """Init creates commits for origin/ and working copies."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        r = self._git(vault, "log", "--oneline")
        assert r.returncode == 0
        lines = r.stdout.strip().splitlines()
        assert len(lines) >= 1

    def test_commits_use_overlord_author(self, tmp_path, default_data_dir):
        """Commits use the overlord author to avoid needing user git config."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        r = self._git(vault, "log", "--format=%an <%ae>")
        assert "overlord <overlord@local>" in r.stdout

    def test_skips_git_init_inside_existing_repo(self, tmp_path, default_data_dir):
        """If vault is inside an existing repo, don't create a nested one."""
        # Create a parent repo
        parent = tmp_path / "parent"
        parent.mkdir()
        self._git(parent, "init")
        self._git(parent, "-c", "user.name=test", "-c", "user.email=test@test",
                   "commit", "--allow-empty", "-m", "initial")

        vault = parent / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        # No nested .git in the vault itself
        assert not (vault / ".git").exists()
        # But commits should be in the parent repo
        r = self._git(parent, "log", "--oneline")
        assert "overlord init" in r.stdout

    def test_reinit_auto_merge_with_git(self, tmp_path, default_data_dir, capsys):
        """Full round-trip: init, then re-init with new template triggers auto-merge."""
        vault = tmp_path / "vault"
        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        # Simulate template upgrade
        import overlord.vault_template as vt_mod
        old_template = vt_mod.VAULT_CLAUDE_MD
        try:
            vt_mod.VAULT_CLAUDE_MD = old_template + "\n<!-- v2 -->\n"
            cmd_init(args)
        finally:
            vt_mod.VAULT_CLAUDE_MD = old_template

        out = capsys.readouterr().out
        assert "Updated CLAUDE.md" in out

        # Check git log has multiple commits
        r = self._git(vault, "log", "--oneline")
        lines = r.stdout.strip().splitlines()
        assert len(lines) >= 2


class TestPreExistingFiles:
    """Tests for vaults that have files predating origin/ tracking."""

    def test_pre_existing_files_skipped(self, tmp_path, default_data_dir, capsys):
        """Files that exist before the first origin/-tracked init are treated as pre-existing."""
        vault = tmp_path / "vault"
        vault.mkdir(parents=True)

        # Create a pre-existing CLAUDE.md (before any init)
        (vault / "CLAUDE.md").write_text("my custom claude md")

        args = build_parser().parse_args(["init", str(vault)])
        cmd_init(args)

        out = capsys.readouterr().out
        assert "Skipped (pre-existing): CLAUDE.md" in out
        # Working copy should be untouched
        assert (vault / "CLAUDE.md").read_text() == "my custom claude md"
        # origin/ should have the template version
        assert (vault / "origin" / "CLAUDE.md").exists()
