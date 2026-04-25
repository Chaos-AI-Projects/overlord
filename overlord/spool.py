"""File-based spool for asynchronous message delivery to Maildir.

Producers write JSON envelope files atomically to ``<data_dir>/spool/``.
A dedicated async task polls the spool directory and delivers each envelope
to the appropriate Maildir via :class:`~overlord.maildir.MaildirStore`.

Atomic writes: files are first written to a ``tmp/`` subdirectory inside the
spool, then renamed into the spool root.  This prevents the delivery task
from reading partially-written files.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .maildir import DEFAULT_DATA_DIR, MaildirStore

logger = logging.getLogger("overlord.spool")


class SpoolWriter:
    """Write message envelopes atomically to the spool directory.

    Parameters
    ----------
    data_dir : Path, optional
        Root data directory.  The spool lives at ``<data_dir>/spool/``.
    """

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or DEFAULT_DATA_DIR
        self.spool_dir = self.data_dir / "spool"
        self.tmp_dir = self.spool_dir / "tmp"
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        payload: str,
        consumer: Optional[str] = None,
        job_name: str = "unknown",
        execution_time: Optional[datetime] = None,
    ) -> Path:
        """Write a message envelope to the spool atomically.

        Parameters
        ----------
        payload : str
            JSON payload string.
        consumer : str, optional
            Target consumer name.  ``None`` routes to the catchall mailbox.
        job_name : str
            Originating job name.
        execution_time : datetime, optional
            Execution timestamp.  Defaults to now (UTC).

        Returns
        -------
        Path
            The path of the delivered spool file.
        """
        if execution_time is None:
            execution_time = datetime.now(timezone.utc)

        envelope = {
            "payload": payload,
            "consumer": consumer,
            "job_name": job_name,
            "execution_time": execution_time.isoformat(),
        }

        filename = f"{time.monotonic_ns()}-{uuid.uuid4().hex}.json"
        tmp_path = self.tmp_dir / filename
        final_path = self.spool_dir / filename

        tmp_path.write_text(json.dumps(envelope), encoding="utf-8")
        os.rename(tmp_path, final_path)

        logger.debug(
            "spooled message file=%s consumer=%s job=%s",
            filename, consumer, job_name,
        )
        return final_path


class SpoolProcessor:
    """Async task that polls the spool directory and delivers to Maildir.

    Parameters
    ----------
    data_dir : Path, optional
        Root data directory (shared with :class:`SpoolWriter` and
        :class:`~overlord.maildir.MaildirStore`).
    poll_interval : float
        Seconds between spool directory polls.  Defaults to 1.0.
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        poll_interval: float = 1.0,
    ):
        self.data_dir = data_dir or DEFAULT_DATA_DIR
        self.spool_dir = self.data_dir / "spool"
        self.poll_interval = poll_interval
        self._store = MaildirStore(data_dir=self.data_dir)
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Poll the spool directory and deliver messages until stopped."""
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Spool processor started (poll_interval=%.1fs, spool_dir=%s)",
            self.poll_interval, self.spool_dir,
        )

        while not self._stop_event.is_set():
            try:
                self._process_spool()
            except Exception:
                logger.exception("Error processing spool directory")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.poll_interval,
                )
            except asyncio.TimeoutError:
                pass  # normal: poll interval elapsed

        # Final drain before exit.
        try:
            self._process_spool()
        except Exception:
            logger.exception("Error during final spool drain")

        logger.info("Spool processor stopped")

    def stop(self) -> None:
        """Signal the processor to stop after the current poll cycle."""
        self._stop_event.set()

    def _process_spool(self) -> None:
        """Scan the spool directory and deliver all ready files."""
        if not self.spool_dir.exists():
            return

        # Sort by filename (timestamp-prefixed) for FIFO ordering.
        spool_files = sorted(
            f for f in self.spool_dir.iterdir()
            if f.is_file() and f.suffix == ".json"
        )

        for spool_file in spool_files:
            try:
                self._deliver_file(spool_file)
            except Exception:
                logger.exception("Failed to deliver spool file=%s", spool_file.name)

    def _deliver_file(self, spool_file: Path) -> None:
        """Read a spool file, deliver to Maildir, and remove the file."""
        data = json.loads(spool_file.read_text(encoding="utf-8"))

        payload = data["payload"]
        consumer = data.get("consumer")
        job_name = data.get("job_name", "unknown")
        execution_time_str = data.get("execution_time")

        execution_time = None
        if execution_time_str:
            execution_time = datetime.fromisoformat(execution_time_str)

        msg = MaildirStore.build_message(
            payload=payload,
            consumer=consumer,
            job_name=job_name,
            execution_time=execution_time,
        )

        key = self._store.deliver(msg, consumer=consumer)
        spool_file.unlink()

        logger.debug(
            "delivered spool file=%s -> mailbox=%s key=%s",
            spool_file.name, consumer or "catchall", key,
        )
