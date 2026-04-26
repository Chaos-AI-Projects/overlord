"""File-based spool for asynchronous message delivery to Maildir.

Producers build an :class:`~email.message.EmailMessage` (via
:meth:`~overlord.maildir.MaildirStore.build_message`) and write the RFC 822
bytes atomically to ``<data_dir>/spool/``.  A dedicated async task polls the
spool directory and delivers each message to the appropriate Maildir via
:class:`~overlord.maildir.MaildirStore`.

Atomic writes: files are first written to a ``tmp/`` subdirectory inside the
spool, then renamed into the spool root.  This prevents the delivery task
from reading partially-written files.
"""

import asyncio
import logging
import os
import signal
import time
import uuid
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from pathlib import Path
from typing import Optional

from .maildir import DEFAULT_DATA_DIR, MaildirStore

logger = logging.getLogger("overlord.spool")


class SpoolWriter:
    """Write RFC 822 message files atomically to the spool directory.

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

    def write(self, msg: EmailMessage) -> Path:
        """Write an RFC 822 message to the spool atomically.

        Parameters
        ----------
        msg : EmailMessage
            A fully-built RFC 822 message (e.g. from
            :meth:`MaildirStore.build_message`).

        Returns
        -------
        Path
            The path of the delivered spool file.
        """
        consumer = msg.get("X-Overlord-Consumer")
        job_name = msg.get("X-Overlord-Job", "unknown")

        filename = f"{time.monotonic_ns()}-{uuid.uuid4().hex}.eml"
        tmp_path = self.tmp_dir / filename
        final_path = self.spool_dir / filename

        tmp_path.write_bytes(msg.as_bytes())
        os.rename(tmp_path, final_path)

        logger.debug(
            "spooled message file=%s consumer=%s job=%s",
            filename, consumer, job_name,
        )

        # Wake the spool processor if a SIGUSR1 handler has been registered.
        # The default disposition (SIG_DFL) would terminate the process, so
        # only send when something is actively listening.
        current = signal.getsignal(signal.SIGUSR1)
        if current not in (signal.SIG_DFL, signal.SIG_IGN, None):
            try:
                os.kill(os.getpid(), signal.SIGUSR1)
            except OSError:
                pass

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
        poll_interval: float = 30.0,
    ):
        self.data_dir = data_dir or DEFAULT_DATA_DIR
        self.spool_dir = self.data_dir / "spool"
        self.poll_interval = poll_interval
        self._store = MaildirStore(data_dir=self.data_dir)
        self._stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()

    async def run(self) -> None:
        """Poll the spool directory and deliver messages until stopped.

        A SIGUSR1 handler is registered so that writers in the same process
        can interrupt the poll sleep and trigger immediate processing.
        """
        self.spool_dir.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGUSR1, self._wake_event.set)
        except (ValueError, OSError):
            # Signal handlers can't be set from non-main threads or on
            # platforms that don't support it (Windows).
            logger.debug("Could not register SIGUSR1 handler; polling only")

        logger.info(
            "Spool processor started (poll_interval=%.1fs, spool_dir=%s)",
            self.poll_interval, self.spool_dir,
        )

        try:
            while not self._stop_event.is_set():
                try:
                    self._process_spool()
                except Exception:
                    logger.exception("Error processing spool directory")

                self._wake_event.clear()
                # Wait until either stop, wake (SIGUSR1), or timeout.
                done, _ = await asyncio.wait(
                    [
                        asyncio.ensure_future(self._stop_event.wait()),
                        asyncio.ensure_future(self._wake_event.wait()),
                    ],
                    timeout=self.poll_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                # Cancel leftover futures.
                for fut in _:
                    fut.cancel()

            # Final drain before exit.
            try:
                self._process_spool()
            except Exception:
                logger.exception("Error during final spool drain")

            logger.info("Spool processor stopped")
        finally:
            try:
                loop.remove_signal_handler(signal.SIGUSR1)
            except (ValueError, OSError):
                pass

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
            if f.is_file() and f.suffix == ".eml"
        )

        for spool_file in spool_files:
            try:
                self._deliver_file(spool_file)
            except Exception:
                logger.exception("Failed to deliver spool file=%s", spool_file.name)

    def _deliver_file(self, spool_file: Path) -> None:
        """Read an RFC 822 spool file, deliver to Maildir, and remove it."""
        parser = BytesParser(policy=policy.default)
        msg = parser.parsebytes(spool_file.read_bytes())

        consumer = msg.get("X-Overlord-Consumer")

        key = self._store.deliver(msg, consumer=consumer)
        spool_file.unlink()

        logger.debug(
            "delivered spool file=%s -> mailbox=%s key=%s",
            spool_file.name, consumer or "catchall", key,
        )
