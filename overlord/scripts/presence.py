#!/usr/bin/env python3
"""Presence system helper — manages job presence via the overlord message hub.

Sub-commands:
    checkin <description>  Record current job's presence (reads identity from env)
    checkout [--what-next <text>]
                           Clear presence for the current job (mark task finished),
                           optionally recording a follow-up note
    recent                 Return the 5 most recent presence messages
    prune                  Consume all but the 5 most recent presence messages
    scan                   Return all presence messages (for self-monitor)
    scan-unfinished        Return only presence messages without a matching checkout
    dispatch-whatnext [--max <n>]
                           Dispatch finished records' what_next follow-ups to the
                           overlord consumer, guarded by bound/dedup/idempotency

All presence messages use consumer="presence" in the message hub.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow importing overlord package when run from the scripts/ directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from overlord.maildir import MaildirStore
from overlord.spool import SpoolWriter

PRESENCE_CONSUMER = "presence"
OVERLORD_CONSUMER = "overlord"

# Maximum number of distinct follow-ups dispatched per invocation (bound guard).
DEFAULT_MAX_DISPATCH = 5


def _get_store() -> MaildirStore:
    return MaildirStore()


def _get_spool() -> SpoolWriter:
    return SpoolWriter()


def _sorted_messages(messages: list[dict]) -> list[dict]:
    """Sort messages by timestamp in the payload (newest first)."""
    def sort_key(m: dict) -> str:
        try:
            payload = json.loads(m["payload"]) if isinstance(m["payload"], str) else m["payload"]
            return payload.get("timestamp", "")
        except (json.JSONDecodeError, TypeError):
            return m.get("date", "")
    return sorted(messages, key=sort_key, reverse=True)


def cmd_checkin(description: str) -> None:
    """Record a presence check-in for the current job."""
    execution_id = os.environ.get("OVERLORD_EXECUTION_ID", "unknown")
    job_name = os.environ.get("OVERLORD_JOB_NAME", "unknown")

    payload = json.dumps({
        "task_id": execution_id,
        "job_name": job_name,
        "description": description,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    spool = _get_spool()
    msg = MaildirStore.build_message(
        payload=payload,
        consumer=PRESENCE_CONSUMER,
        job_name=job_name,
    )
    spool.write(msg)
    # Note: the message is written to the spool and delivered asynchronously.
    # It will NOT appear in an immediately subsequent `recent` call.
    print(f"Checked in: execution={execution_id} job={job_name}", file=sys.stderr)


def cmd_checkout(what_next: str = "") -> None:
    """Record that the current job's task has finished.

    Idempotent: if a checkout (finished) record already exists for this
    task_id, this is a no-op so repeated invocations never write duplicate
    records. An optional ``what_next`` note describing the follow-up is
    included only when it is non-empty (after stripping whitespace).
    """
    execution_id = os.environ.get("OVERLORD_EXECUTION_ID", "unknown")
    job_name = os.environ.get("OVERLORD_JOB_NAME", "unknown")

    # Idempotency guard: skip if this task_id has already been checked out.
    store = _get_store()
    existing = _parse_payloads(store.fetch_messages(PRESENCE_CONSUMER))
    if any(
        isinstance(p, dict)
        and p.get("status") == "finished"
        and p.get("task_id") == execution_id
        for p in existing
    ):
        print(f"Already checked out: execution={execution_id} job={job_name}", file=sys.stderr)
        return

    record = {
        "task_id": execution_id,
        "job_name": job_name,
        "status": "finished",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if what_next.strip():
        record["what_next"] = what_next.strip()

    spool = _get_spool()
    msg = MaildirStore.build_message(
        payload=json.dumps(record),
        consumer=PRESENCE_CONSUMER,
        job_name=job_name,
    )
    spool.write(msg)
    print(f"Checked out: execution={execution_id} job={job_name}", file=sys.stderr)


def _parse_payloads(messages: list[dict]) -> list:
    """Extract and parse JSON payloads from a list of messages."""
    result = []
    for m in messages:
        try:
            payload = json.loads(m["payload"]) if isinstance(m["payload"], str) else m["payload"]
        except (json.JSONDecodeError, TypeError):
            payload = m["payload"]
        result.append(payload)
    return result


def cmd_recent() -> None:
    """Print the 5 most recent presence messages as JSON."""
    store = _get_store()
    messages = store.fetch_messages(PRESENCE_CONSUMER)
    sorted_msgs = _sorted_messages(messages)[:5]
    print(json.dumps(_parse_payloads(sorted_msgs), indent=2))


def cmd_prune() -> None:
    """Consume all but the 5 most recent presence messages."""
    store = _get_store()
    messages = store.fetch_messages(PRESENCE_CONSUMER)
    sorted_msgs = _sorted_messages(messages)

    # Keep the 5 most recent, consume the rest
    to_consume = sorted_msgs[5:]
    if not to_consume:
        print(f"Nothing to prune ({len(sorted_msgs)} messages, cap is 5)", file=sys.stderr)
        return

    keys = [m["key"] for m in to_consume]
    store.consume_bulk(PRESENCE_CONSUMER, keys)
    print(f"Pruned {len(keys)} old presence messages (kept {min(len(sorted_msgs), 5)})", file=sys.stderr)


def cmd_scan() -> None:
    """Print all presence messages as JSON (for self-monitor evaluation)."""
    store = _get_store()
    messages = store.fetch_messages(PRESENCE_CONSUMER)
    sorted_msgs = _sorted_messages(messages)
    print(json.dumps(_parse_payloads(sorted_msgs), indent=2))


def cmd_scan_unfinished() -> None:
    """Print only presence messages that don't have a matching checkout.

    A checkin is considered finished if there is a checkout message with the
    same task_id. Messages with status="finished" are checkout records and
    are excluded from the output.
    """
    store = _get_store()
    messages = store.fetch_messages(PRESENCE_CONSUMER)
    sorted_msgs = _sorted_messages(messages)
    payloads = _parse_payloads(sorted_msgs)

    # Collect task_ids that have been checked out
    finished_ids = {
        p.get("task_id")
        for p in payloads
        if isinstance(p, dict) and p.get("status") == "finished"
    }

    # Filter to checkins without a matching checkout. Records carrying a
    # status ("finished" checkouts, "dispatched" markers) are bookkeeping
    # entries, not open checkins, so they are never reported as unfinished --
    # this also covers a dispatched marker whose finished record has since been
    # pruned away.
    unfinished = [
        p for p in payloads
        if isinstance(p, dict)
        and p.get("status") not in ("finished", "dispatched")
        and p.get("task_id") not in finished_ids
    ]

    print(json.dumps(unfinished, indent=2))


def cmd_dispatch_whatnext(max_dispatch: int = DEFAULT_MAX_DISPATCH) -> None:
    """Dispatch finished records' follow-up notes to the overlord consumer.

    A finished (checkout) record carrying a non-empty ``what_next`` note is a
    follow-up the originating job wanted the next run to pick up. This command
    forwards those notes to the overlord consumer behind three guards:

    * **idempotency** -- a ``dispatched`` marker (keyed by ``task_id``) is
      written for every record forwarded, and records whose ``task_id`` already
      has such a marker are skipped, so each follow-up is dispatched at most
      once across repeated invocations.
    * **dedup** -- identical ``what_next`` strings collapse to a single overlord
      message; every contributing ``task_id`` is still stamped so none resurface.
    * **bound** -- at most ``max_dispatch`` distinct follow-ups are forwarded per
      invocation; the remainder are left for a later run.
    """
    store = _get_store()
    payloads = _parse_payloads(store.fetch_messages(PRESENCE_CONSUMER))

    # Idempotency: task_ids that have already been dispatched.
    dispatched_ids = {
        p.get("task_id")
        for p in payloads
        if isinstance(p, dict) and p.get("status") == "dispatched"
    }

    # Candidate follow-ups: finished records with a non-empty what_next note
    # that have not yet been dispatched.
    candidates = [
        p for p in payloads
        if isinstance(p, dict)
        and p.get("status") == "finished"
        and str(p.get("what_next", "")).strip()
        and p.get("task_id") not in dispatched_ids
    ]

    # Dedup: group by the follow-up text. The first record for each distinct
    # string drives the dispatch; later ones only contribute their task_id.
    grouped: dict[str, list[dict]] = {}
    for record in candidates:
        grouped.setdefault(str(record["what_next"]).strip(), []).append(record)

    spool = _get_spool()
    dispatched_count = 0
    for what_next, records in grouped.items():
        # Bound: stop once the per-invocation cap is reached. Untouched groups
        # keep their records unstamped so a later run picks them up.
        if dispatched_count >= max_dispatch:
            break

        origin = records[0]
        dispatch_payload = json.dumps({
            "source": "presence-whatnext",
            "what_next": what_next,
            "origin_task_id": origin.get("task_id"),
            "origin_job": origin.get("job_name"),
        })
        spool.write(MaildirStore.build_message(
            payload=dispatch_payload,
            consumer=OVERLORD_CONSUMER,
            job_name="presence",
        ))

        # Idempotency: stamp every contributing task_id so the follow-up (and
        # its duplicates) is never dispatched again.
        for record in records:
            marker = json.dumps({
                "task_id": record.get("task_id"),
                "job_name": record.get("job_name"),
                "status": "dispatched",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            spool.write(MaildirStore.build_message(
                payload=marker,
                consumer=PRESENCE_CONSUMER,
                job_name="presence",
            ))

        dispatched_count += 1

    print(
        f"Dispatched {dispatched_count} follow-up(s) to {OVERLORD_CONSUMER}",
        file=sys.stderr,
    )


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <checkin|checkout|recent|prune|scan|scan-unfinished|dispatch-whatnext> [args...]", file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]

    if command == "checkin":
        if len(sys.argv) < 3:
            print("Usage: presence.py checkin <description>", file=sys.stderr)
            sys.exit(1)
        cmd_checkin(" ".join(sys.argv[2:]))
    elif command == "checkout":
        what_next = ""
        args = sys.argv[2:]
        if "--what-next" in args:
            idx = args.index("--what-next")
            if idx + 1 >= len(args):
                print("Usage: presence.py checkout [--what-next <text>]", file=sys.stderr)
                sys.exit(1)
            what_next = args[idx + 1]
        cmd_checkout(what_next=what_next)
    elif command == "recent":
        cmd_recent()
    elif command == "prune":
        cmd_prune()
    elif command == "scan":
        cmd_scan()
    elif command == "scan-unfinished":
        cmd_scan_unfinished()
    elif command == "dispatch-whatnext":
        max_dispatch = DEFAULT_MAX_DISPATCH
        args = sys.argv[2:]
        if "--max" in args:
            idx = args.index("--max")
            if idx + 1 >= len(args):
                print("Usage: presence.py dispatch-whatnext [--max <n>]", file=sys.stderr)
                sys.exit(1)
            try:
                max_dispatch = int(args[idx + 1])
            except ValueError:
                print("dispatch-whatnext: --max must be a positive integer", file=sys.stderr)
                sys.exit(1)
            if max_dispatch < 1:
                print("dispatch-whatnext: --max must be a positive integer", file=sys.stderr)
                sys.exit(1)
        cmd_dispatch_whatnext(max_dispatch=max_dispatch)
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
