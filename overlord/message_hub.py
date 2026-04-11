"""Message hub — poll-based message consumption with agent invocation.

Jobs produce messages (via the executor) that are persisted in SQLite.
The MessageHub polls for unconsumed messages and dispatches them to a
configurable handler (typically a shell command that invokes an agent).
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from .database import Database
from .models import Message

logger = logging.getLogger("overlord.message_hub")


class MessageHub:
    """Poll-based message consumer that invokes agents on new messages.

    Usage::

        hub = MessageHub(
            db_path=Path("/path/to/overlord.db"),
            handler_command="claude-agent handle-message",
            poll_seconds=10,
        )
        await hub.run()
    """

    def __init__(
        self,
        db: Optional[Database] = None,
        db_path: Optional[Path] = None,
        handler_command: Optional[str] = None,
        poll_seconds: int = 10,
        batch_size: int = 10,
    ):
        """Initialise the message hub.

        Args:
            db: Existing Database instance (takes precedence over db_path).
            db_path: Path to the SQLite database.
            handler_command: Shell command to invoke for each message.
                The message payload is passed on stdin as a JSON object
                containing ``message_id``, ``source_job_id``, ``payload``,
                and ``created_at``.  If None, messages are logged but no
                handler is invoked.
            poll_seconds: Interval between poll cycles.
            batch_size: Max messages to fetch per poll cycle.
        """
        self._db = db if db is not None else Database(db_path)
        self._handler_command = handler_command
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Start the poll loop.  Blocks until ``stop()`` is called."""
        logger.info(
            "MessageHub started (poll=%ds, handler=%s)",
            self._poll_seconds,
            self._handler_command or "<none>",
        )
        try:
            while not self._stop_event.is_set():
                await self._poll_cycle()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._poll_seconds,
                    )
                except asyncio.TimeoutError:
                    pass  # normal: poll interval elapsed
        finally:
            logger.info("MessageHub stopped")

    async def stop(self) -> None:
        """Request a graceful stop."""
        logger.info("MessageHub stop requested")
        self._stop_event.set()

    async def _poll_cycle(self) -> None:
        """Fetch unconsumed messages, invoke the handler, mark consumed."""
        messages = self._db.poll_messages(limit=self._batch_size)
        if not messages:
            return

        logger.info("Polled %d new message(s)", len(messages))
        for msg in messages:
            try:
                await self._handle_message(msg)
                self._db.mark_consumed(msg.id)
            except Exception:
                logger.exception(
                    "Failed to handle message id=%d from job=%d",
                    msg.id, msg.source_job_id,
                )

    async def _handle_message(self, msg: Message) -> None:
        """Invoke the handler command (agent) with the message payload.

        The message is serialised as a JSON object and piped to the
        handler's stdin.  If no handler is configured, the message is
        logged at INFO level.
        """
        envelope = _message_to_dict(msg)

        if self._handler_command is None:
            logger.info(
                "message id=%d from job=%d (no handler): %s",
                msg.id, msg.source_job_id, msg.payload,
            )
            return

        logger.info(
            "message id=%d from job=%d -> %s",
            msg.id, msg.source_job_id, self._handler_command,
        )
        stdin_data = json.dumps(envelope).encode()

        proc = await asyncio.create_subprocess_shell(
            self._handler_command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await proc.communicate(input=stdin_data)

        if proc.returncode != 0:
            stderr_text = stderr_bytes.decode(errors="replace") if stderr_bytes else ""
            logger.error(
                "Handler exited %d for message id=%d: %s",
                proc.returncode, msg.id, stderr_text,
            )
            raise RuntimeError(
                f"Handler failed (exit {proc.returncode}) for message {msg.id}"
            )

        if stdout_bytes:
            logger.debug(
                "Handler stdout for message id=%d: %s",
                msg.id, stdout_bytes.decode(errors="replace"),
            )


def _message_to_dict(msg: Message) -> dict:
    """Convert a Message to a JSON-serialisable dict."""
    return {
        "message_id": msg.id,
        "source_job_id": msg.source_job_id,
        "payload": msg.payload,
        "created_at": str(msg.created_at) if msg.created_at else None,
    }
