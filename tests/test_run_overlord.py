"""Tests for the ``podman run`` line in scripts/run-overlord.sh.

Drives the real script under subprocess with a fake ``podman`` on PATH. The
shim records its argv and, on ``run``, sends SIGTERM to the script so the
infinite restart loop exits through its own ``cleanup`` trap.

Regression under test (MS-688): the container's resolver sent the A and AAAA
queries in parallel over one socket, and a dropped answer stalled each lookup
for 5s. ``single-request-reopen`` sends them on separate sockets. Podman's
``--dns-option`` replaces the host's ``options`` line rather than adding to it,
so ``edns0`` from the host's resolv.conf must be passed explicitly too.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

RUN_SH = Path(__file__).resolve().parent.parent / "scripts" / "run-overlord.sh"

PODMAN_SHIM = r'''#!/usr/bin/env python3
import json
import os
import signal
import sys

with open(os.environ["PODMAN_ARGV_LOG"], "a") as fh:
    fh.write(json.dumps(sys.argv[1:]) + "\n")
if sys.argv[1:2] == ["run"]:
    os.kill(os.getppid(), signal.SIGTERM)
'''


def run_args():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        shim = tmp / "bin" / "podman"
        shim.parent.mkdir()
        shim.write_text(PODMAN_SHIM)
        shim.chmod(0o755)
        log = tmp / "argv.log"
        env = dict(
            os.environ,
            PATH=f"{shim.parent}:{os.environ['PATH']}",
            PODMAN_ARGV_LOG=str(log),
            IMAGE_STATE_FILE=str(tmp / "image-state"),
            DATA_DIR=str(tmp / "data"),
            PODMAN_SOCKET=str(tmp / "podman.sock"),
            RESTART_DELAY="0",
            LATEST_IMAGE="overlord:latest",
        )
        subprocess.run(["bash", str(RUN_SH)], env=env, timeout=30, check=False,
                       capture_output=True)
        calls = [json.loads(line) for line in log.read_text().splitlines()]
    runs = [c for c in calls if c[:1] == ["run"]]
    assert len(runs) == 1, calls
    return runs[0]


def dns_options(argv):
    # Flags after the image would be the container's command, not podman's.
    image = argv.index("overlord:latest")
    return [argv[i + 1] for i, a in enumerate(argv[:image]) if a == "--dns-option"]


class PodmanRunDnsOptions(unittest.TestCase):
    def test_single_request_reopen_is_passed(self):
        self.assertIn("single-request-reopen", dns_options(run_args()))

    def test_host_edns0_is_kept(self):
        self.assertIn("edns0", dns_options(run_args()))

    def test_still_uses_host_network(self):
        self.assertIn("--network=host", run_args())


if __name__ == "__main__":
    unittest.main()
