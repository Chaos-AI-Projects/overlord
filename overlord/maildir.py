"""Maildir-backed message delivery for Overlord.

Uses Python stdlib ``mailbox.Maildir`` for all maildir operations.

Directory layout::

    <data_dir>/mailboxes/<consumer_name>/   -- per-consumer Maildir
    <data_dir>/mailboxes/catchall/          -- messages with no consumer target

Messages are RFC 822-enveloped:

- **Subject**: ``<job_name> <execution_time>``
- **Body**: placeholder text (not used for payload)
- **Attachment**: ``payload.json`` containing the actual JSON payload

After processing, consumed messages are moved to a ``processed`` subfolder.
"""

import json
import logging
import mailbox
import os
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Optional

logger = logging.getLogger("overlord.maildir")

CATCHALL = "catchall"

DEFAULT_DATA_DIR = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord"


class MaildirStore:
    """Maildir-backed message store.

    Parameters
    ----------
    data_dir : Path, optional
        Root data directory.  Maildirs live under ``<data_dir>/mailboxes/``.
    """

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or DEFAULT_DATA_DIR
        self.mailboxes_dir = self.data_dir / "mailboxes"
        self.mailboxes_dir.mkdir(parents=True, exist_ok=True)

    def _get_maildir(self, consumer: str) -> mailbox.Maildir:
        """Return (and lazily create) the Maildir for *consumer*."""
        path = self.mailboxes_dir / consumer
        md = mailbox.Maildir(str(path), create=True)
        return md

    def _get_processed_maildir(self, consumer: str) -> mailbox.Maildir:
        """Return (and lazily create) the ``processed`` sub-Maildir."""
        parent = self._get_maildir(consumer)
        # mailbox.Maildir.add_folder creates a subfolder Maildir
        try:
            sub = parent.get_folder("processed")
        except mailbox.NoSuchMailboxError:
            sub = parent.add_folder("processed")
        return sub

    # ------------------------------------------------------------------
    # Delivery
    # ------------------------------------------------------------------

    @staticmethod
    def build_message(
        payload: str,
        consumer: Optional[str] = None,
        job_name: str = "unknown",
        execution_time: Optional[datetime] = None,
    ) -> EmailMessage:
        """Build an RFC 822 message for delivery.

        Parameters
        ----------
        payload : str
            JSON payload string.
        consumer : str, optional
            Target consumer name (stored in X-Overlord-Consumer header).
        job_name : str
            Originating job name (used in the Subject header).
        execution_time : datetime, optional
            Execution timestamp (used in the Subject header).  Defaults to now.

        Returns
        -------
        EmailMessage
            The constructed message ready for delivery.
        """
        if execution_time is None:
            execution_time = datetime.now(timezone.utc)

        msg = EmailMessage()
        msg["Subject"] = f"{job_name} {execution_time.isoformat()}"
        msg["Date"] = execution_time.isoformat()
        msg["X-Overlord-Job"] = job_name
        if consumer:
            msg["X-Overlord-Consumer"] = consumer

        msg.set_content("Overlord message delivery. See payload.json attachment.")

        msg.add_attachment(
            payload.encode("utf-8"),
            maintype="application",
            subtype="json",
            filename="payload.json",
        )

        return msg

    def deliver(
        self,
        msg: EmailMessage,
        consumer: Optional[str] = None,
    ) -> str:
        """Deliver an RFC 822 message to the appropriate Maildir.

        Parameters
        ----------
        msg : EmailMessage
            The RFC 822 message to deliver.
        consumer : str, optional
            Target consumer name.  ``None`` routes to the catchall mailbox.

        Returns
        -------
        str
            The Maildir message key of the delivered message.
        """
        target = consumer or CATCHALL

        md = self._get_maildir(target)
        key = md.add(msg)
        md.flush()
        logger.debug(
            "delivered message key=%s to mailbox=%s",
            key, target,
        )
        return key

    # ------------------------------------------------------------------
    # Consumption
    # ------------------------------------------------------------------

    def fetch_messages(self, consumer: str) -> list[dict]:
        """Fetch all messages from a consumer's Maildir (new + cur).

        Returns a list of dicts with keys: ``key``, ``consumer``,
        ``payload``, ``subject``, ``date``.
        """
        md = self._get_maildir(consumer)
        results = []
        for key, msg in md.iteritems():
            payload = self._extract_payload(msg)
            results.append({
                "key": key,
                "consumer": consumer if consumer != CATCHALL else None,
                "payload": payload,
                "subject": msg.get("Subject", ""),
                "date": msg.get("Date", ""),
            })
        return results

    def consume(self, consumer: str, key: str) -> None:
        """Mark a message as consumed by moving it to the ``processed`` subfolder.

        Parameters
        ----------
        consumer : str
            The consumer mailbox name.
        key : str
            The Maildir message key to consume.
        """
        md = self._get_maildir(consumer)
        processed = self._get_processed_maildir(consumer)

        try:
            msg = md.get(key)
        except KeyError:
            logger.warning(
                "message key=%s not found in mailbox=%s", key, consumer,
            )
            return

        if msg is None:
            logger.warning(
                "message key=%s not found in mailbox=%s", key, consumer,
            )
            return

        md.discard(key)
        md.flush()
        processed.add(msg)
        processed.flush()
        logger.debug("consumed message key=%s from mailbox=%s", key, consumer)

    def consume_bulk(self, consumer: str, keys: list[str]) -> None:
        """Move multiple messages to the ``processed`` subfolder."""
        for key in keys:
            self.consume(consumer, key)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def list_mailboxes(self) -> list[str]:
        """Return names of all consumer mailboxes that exist on disk."""
        if not self.mailboxes_dir.exists():
            return []
        return sorted(
            d.name
            for d in self.mailboxes_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        )

    def count_messages(self, consumer: str) -> int:
        """Return the number of unconsumed messages in a consumer's mailbox."""
        md = self._get_maildir(consumer)
        return len(md)

    def fetch_processed_messages(self, consumer: str) -> list[dict]:
        """Fetch consumed (processed) messages from a consumer's Maildir.

        Returns a list of dicts with keys: ``key``, ``consumer``,
        ``payload``, ``subject``, ``date``.
        """
        processed = self._get_processed_maildir(consumer)
        results = []
        for key, msg in processed.iteritems():
            payload = self._extract_payload(msg)
            results.append({
                "key": key,
                "consumer": consumer if consumer != CATCHALL else None,
                "payload": payload,
                "subject": msg.get("Subject", ""),
                "date": msg.get("Date", ""),
            })
        return results

    def fetch_unconsumed_for_consumers(
        self, consumer_names: list[str],
    ) -> list[dict]:
        """Fetch unconsumed messages for the given consumer names.

        If ``"*"`` is in *consumer_names*, messages from the catchall
        mailbox are returned.
        """
        results = []
        for name in consumer_names:
            target = CATCHALL if name == "*" else name
            results.extend(self.fetch_messages(target))
        return results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_payload(msg: EmailMessage) -> str:
        """Extract the payload.json attachment content from an email message."""
        for part in msg.walk():
            fn = part.get_filename()
            if fn == "payload.json":
                data = part.get_payload(decode=True)
                if data is not None:
                    return data.decode("utf-8")
        # Fallback: return the text body if no attachment found
        body = msg.get_body(preferencelist=("plain",))
        if body:
            return body.get_content()
        return ""
