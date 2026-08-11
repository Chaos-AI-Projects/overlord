"""Tests for failure reporting in scripts/build.sh.

Drives the real shell script under subprocess in a temp dir, with fake
``gh``/``nix``/``podman`` shims on PATH. The ``gh`` shim is stateful: issues
created by one run are visible to the ``issue list`` of the next, so the dedup
behaviour is exercised end to end rather than mocked.

Regression under test: ``report_failure`` used to call ``gh issue create``
unconditionally on every failed run, which filed 27 identical issues
(#408-#434) for just two commits when the build was re-invoked per tick.
"""

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

BUILD_SH = Path(__file__).resolve().parent.parent / "scripts" / "build.sh"

SHA_A = "2e2556a1111111111111111111111111111111ab"
SHA_B = "5a051f12222222222222222222222222222222cd"

GH_SHIM = r'''#!/usr/bin/env python3
"""Fake `gh` that keeps a JSON issue store so dedup can be tested for real."""
import json
import os
import sys

args = sys.argv[1:]
with open(os.environ["GH_ARGV_LOG"], "a") as fh:
    fh.write(json.dumps(args) + "\n")

STORE = os.environ["GH_ISSUE_STORE"]


def load():
    try:
        with open(STORE) as fh:
            return json.load(fh)
    except FileNotFoundError:
        return []


def opt(name, default=None):
    return args[args.index(name) + 1] if name in args else default


if args[:2] == ["issue", "list"]:
    if os.environ.get("GH_LIST_FAIL"):
        sys.stderr.write("gh: simulated list failure\n")
        sys.exit(1)
    # Deliberately ignores --search and --state and returns every issue: dedup
    # must be decided by build.sh's own exact-title and state checks rather
    # than by us emulating GitHub's server-side filtering.
    for issue in load():
        sys.stdout.write("{}\t{}\t{}\n".format(
            issue["number"], issue["state"].upper(), issue["title"]))
    sys.exit(0)

if args[:2] == ["issue", "create"]:
    if os.environ.get("GH_CREATE_FAIL"):
        sys.stderr.write("gh: simulated create failure\n")
        sys.exit(1)
    issues = load()
    number = max([i["number"] for i in issues], default=400) + 1
    issues.append({
        "number": number,
        "title": opt("--title", ""),
        "body": opt("--body", ""),
        "state": "open",
    })
    with open(STORE, "w") as fh:
        json.dump(issues, fh)
    sys.stdout.write("https://github.com/test/test/issues/%d\n" % number)
    sys.exit(0)

sys.stderr.write("gh: unsupported invocation %r\n" % (args,))
sys.exit(2)
'''

NIX_SHIM = r'''#!/usr/bin/env python3
import os
import sys

args = sys.argv[1:]
if os.environ.get("NIX_BUILD_FAIL"):
    sys.stderr.write("nix: simulated build failure\n")
    sys.exit(1)
if "--out-link" in args:
    with open(args[args.index("--out-link") + 1], "w") as fh:
        fh.write("fake-image-tarball\n")
sys.exit(0)
'''

PODMAN_SHIM = r'''#!/usr/bin/env python3
import os
import sys

args = sys.argv[1:]
if args[:1] == ["load"] and os.environ.get("PODMAN_LOAD_FAIL"):
    sys.stderr.write("podman: simulated load failure\n")
    sys.exit(1)
sys.exit(0)
'''


class BuildHarness:
    """A temp copy of build.sh with fake gh/nix/podman on PATH."""

    def __init__(self, tmp):
        self.tmp = Path(tmp)
        scripts = self.tmp / "overlord" / "scripts"
        scripts.mkdir(parents=True)
        self.script = scripts / "build.sh"
        shutil.copy(BUILD_SH, self.script)
        self.script.chmod(0o755)

        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        for name, body in (("gh", GH_SHIM), ("nix", NIX_SHIM), ("podman", PODMAN_SHIM)):
            path = self.bin / name
            path.write_text(body)
            path.chmod(0o755)

        self.store = self.tmp / "issues.json"
        self.argv_log = self.tmp / "gh-argv.log"

    def seed_issues(self, issues):
        self.store.write_text(json.dumps(issues))

    def issues(self):
        if not self.store.exists():
            return []
        return json.loads(self.store.read_text())

    def created_titles(self):
        titles = []
        for line in self.argv_log.read_text().splitlines():
            args = json.loads(line)
            if args[:2] == ["issue", "create"]:
                titles.append(args[args.index("--title") + 1])
        return titles

    def run(self, sha, *, fail="nix", triggered_by=None, extra_env=None):
        env = dict(os.environ)
        env["PATH"] = "%s:%s" % (self.bin, env["PATH"])
        env["GH_ARGV_LOG"] = str(self.argv_log)
        env["GH_ISSUE_STORE"] = str(self.store)
        env.pop("NIX_BUILD_FAIL", None)
        env.pop("PODMAN_LOAD_FAIL", None)
        env.pop("GH_LIST_FAIL", None)
        env.pop("GH_CREATE_FAIL", None)
        if fail == "nix":
            env["NIX_BUILD_FAIL"] = "1"
        elif fail == "podman-load":
            env["PODMAN_LOAD_FAIL"] = "1"
        env.update(extra_env or {})

        argv = ["bash", str(self.script), "--repo", "test/test", "--sha", sha]
        if triggered_by is not None:
            argv += ["--triggered-by", triggered_by]
        return subprocess.run(argv, env=env, capture_output=True, text=True)


class BuildReportTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.h = BuildHarness(self.tmpdir.name)

    # --- basic reporting -------------------------------------------------

    def test_first_failure_creates_exactly_one_issue(self):
        result = self.h.run(SHA_A)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(self.h.created_titles()), 1)

    def test_title_carries_short_sha(self):
        self.h.run(SHA_A)
        title = self.h.created_titles()[0]
        self.assertIn(SHA_A[:7], title, "short SHA must be in the issue title")
        self.assertIn("nix build failed", title)

    # --- dedup -----------------------------------------------------------

    def test_second_failure_same_sha_same_error_creates_no_new_issue(self):
        """The exact regression that produced #408-#434."""
        first = self.h.run(SHA_A)
        second = self.h.run(SHA_A)
        self.assertEqual(first.returncode, 1)
        self.assertEqual(second.returncode, 1, "build failure must still exit 1")
        self.assertEqual(len(self.h.created_titles()), 1,
                         "second failure for the same SHA+error must not file a duplicate")

    def test_skip_log_references_existing_issue_number(self):
        self.h.run(SHA_A)
        existing = self.h.issues()[0]["number"]
        second = self.h.run(SHA_A)
        self.assertIn("#%d" % existing, second.stdout,
                      "skip log must point the operator at the existing issue")

    def test_different_sha_creates_new_issue(self):
        self.h.run(SHA_A)
        self.h.run(SHA_B)
        self.assertEqual(len(self.h.created_titles()), 2)

    def test_different_error_same_sha_creates_new_issue(self):
        self.h.run(SHA_A, fail="nix")
        self.h.run(SHA_A, fail="podman-load")
        titles = self.h.created_titles()
        self.assertEqual(len(titles), 2)
        self.assertTrue(any("nix build failed" in t for t in titles))
        self.assertTrue(any("podman load failed" in t for t in titles))

    def test_closed_issue_does_not_suppress_new_report(self):
        # Derive the pre-existing issue from a real run and then close it, so
        # the seeded title can never drift from the format build.sh emits.
        self.h.run(SHA_A)
        issues = self.h.issues()
        self.assertEqual(len(issues), 1)
        issues[0]["state"] = "closed"
        self.h.seed_issues(issues)

        result = self.h.run(SHA_A)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.h.created_titles()), 2,
                         "a closed issue must not suppress a fresh failure report")

    # --- fail-soft -------------------------------------------------------

    def test_lookup_failure_still_reports_and_exits_1(self):
        result = self.h.run(SHA_A, extra_env={"GH_LIST_FAIL": "1"})
        self.assertEqual(result.returncode, 1, "build failure must still exit 1")
        self.assertEqual(len(self.h.created_titles()), 1,
                         "a broken dedup lookup must not swallow the report")

    def test_create_failure_still_exits_1(self):
        result = self.h.run(SHA_A, extra_env={"GH_CREATE_FAIL": "1"})
        self.assertEqual(result.returncode, 1)
        self.assertIn("Warning: failed to create GitHub issue", result.stdout)

    # --- triggered-by ----------------------------------------------------

    def test_triggered_by_defaults_to_ci_pull_and_build(self):
        self.h.run(SHA_A)
        self.assertIn("ci-pull-and-build.sh", self.h.issues()[0]["body"])

    def test_triggered_by_flag_is_reported(self):
        self.h.run(SHA_A, triggered_by="monoci-watcher.sh")
        body = self.h.issues()[0]["body"]
        self.assertIn("monoci-watcher.sh", body)
        self.assertNotIn("ci-pull-and-build.sh", body,
                         "body must not hardcode the old trigger when a label is given")

    def test_triggered_by_does_not_affect_dedup(self):
        self.h.run(SHA_A, triggered_by="ci-pull-and-build.sh")
        self.h.run(SHA_A, triggered_by="monoci-watcher.sh")
        self.assertEqual(len(self.h.created_titles()), 1,
                         "same SHA+error from a different trigger is still the same failure")


if __name__ == "__main__":
    unittest.main()
