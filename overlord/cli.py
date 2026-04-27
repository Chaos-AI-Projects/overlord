"""CLI for managing Overlord jobs via the MCP interface.

All management commands communicate with the running scheduler daemon through
its MCP streamable-HTTP endpoint, avoiding direct database access.

Usage::

    overlord init [PATH]
    overlord daemon [--data-dir PATH] [--tick N] [--mcp-host HOST] [--mcp-port PORT] [--log-file PATH]
    overlord list [--status STATUS] [--mcp-url URL]
    overlord status JOB_NAME [--mcp-url URL]
    overlord register --name NAME --cron EXPR --command CMD [options] [--mcp-url URL]
    overlord update --name NAME [--cron EXPR] [--command CMD] [options] [--mcp-url URL]
    overlord unregister JOB_NAME [--mcp-url URL]
    overlord trigger JOB_NAME [--mcp-url URL]
    overlord send [--consumer NAME] [--payload TEXT] [--mcp-url URL]
"""

import argparse
import asyncio
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DEFAULT_MCP_URL = "http://127.0.0.1:8000/mcp/"


def _utc_to_local(ts: str) -> str:
    """Convert a UTC timestamp string to local time for display."""
    if not ts:
        return ""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return ts

# Path to the bundled wrapper script (inside the installed package).
_SCRIPTS_DIR = Path(__file__).resolve().parent / "scripts"
_OVERLORD_JOB_SCRIPT = _SCRIPTS_DIR / "overlord_job.sh"


async def _call_tool(mcp_url: str, tool_name: str, arguments: dict) -> str:
    """Connect to the MCP server and call a single tool, returning the text result."""
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async with streamablehttp_client(mcp_url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)
            parts = []
            for block in result.content:
                if hasattr(block, "text"):
                    parts.append(block.text)
            return "\n".join(parts)


def _print_json(raw: str) -> None:
    """Pretty-print a JSON string to stdout."""
    try:
        data = json.loads(raw)
        print(json.dumps(data, indent=2))
    except json.JSONDecodeError:
        print(raw)


def _print_messages(raw: str, text: bool = False, as_jsonl: bool = False) -> None:
    """Print a list of messages as a human-readable table, full text, or JSONL."""
    try:
        messages = json.loads(raw)
    except json.JSONDecodeError:
        print(raw)
        return

    if isinstance(messages, dict) and "error" in messages:
        print(f"Error: {messages['error']}", file=sys.stderr)
        sys.exit(1)

    if not messages:
        if not as_jsonl:
            print("No messages found.")
        return

    if as_jsonl:
        for msg in messages:
            print(json.dumps(msg))
        return

    if text:
        _print_messages_text(messages)
        return

    fmt = "{:<36} {:<20} {:<10} {:<25} {:<30}"
    print(fmt.format("ID", "JOB", "CONSUMED", "CONSUMER", "CREATED"))
    print("-" * 123)
    for m in messages:
        consumer = m.get("consumer") or ""
        job_label = m.get("source_job_name") or ""
        print(fmt.format(
            str(m.get("id", ""))[:36],
            job_label[:20],
            "yes" if m.get("consumed") else "no",
            consumer[:25],
            _utc_to_local(str(m.get("created_at", "")))[:30],
        ))


def _print_messages_text(messages: list[dict]) -> None:
    """Print messages in plain-text record format with full payload."""
    for i, m in enumerate(messages):
        if i > 0:
            print()
        msg_id = m.get("id", "?")
        job_label = m.get("source_job_name") or ""
        consumer = m.get("consumer") or "-"
        consumed = "yes" if m.get("consumed") else "no"
        created = _utc_to_local(m.get("created_at", ""))

        print(f"--- Message {msg_id} ---")
        print(f"Job: {job_label}")
        print(f"Consumed: {consumed}")
        print(f"Consumer: {consumer}")
        print(f"Created: {created}")
        print()

        payload = m.get("payload")
        if payload is None:
            print("(no payload)")
        elif isinstance(payload, dict) or isinstance(payload, list):
            print(json.dumps(payload, indent=2))
        else:
            print(str(payload))


def _print_job_table(raw: str) -> None:
    """Print a list of jobs as a human-readable table."""
    try:
        jobs = json.loads(raw)
    except json.JSONDecodeError:
        print(raw)
        return

    if isinstance(jobs, dict) and "error" in jobs:
        print(f"Error: {jobs['error']}", file=sys.stderr)
        sys.exit(1)

    if not jobs:
        print("No jobs found.")
        return

    # Table header
    fmt = "{:<25} {:<20} {:<10} {:<12} {:<20}"
    print(fmt.format("NAME", "CRON", "STATUS", "QUEUE", "CONSUMES"))
    print("-" * 89)
    for j in jobs:
        consumes = ", ".join(j.get("consumes", []))
        print(fmt.format(
            j.get("name", "")[:25],
            j.get("cron_expression", "")[:20],
            j.get("status", ""),
            j.get("queue_name", "default")[:12],
            consumes[:20] if consumes else "-",
        ))


def _print_job_status(raw: str) -> None:
    """Print job status with recent executions."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        print(raw)
        return

    if isinstance(data, dict) and "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)

    print(f"Job:      {data.get('name')}")
    print(f"Status:   {data.get('status')}")
    print(f"Cron:     {data.get('cron_expression')}")
    print(f"Command:  {data.get('command')}")
    queue = data.get("queue_name", "default")
    if queue != "default":
        print(f"Queue:    {queue}")
    consumes = data.get("consumes", [])
    if consumes:
        print(f"Consumes: {', '.join(consumes)}")
    if data.get("exclusive_lock"):
        print(f"Lock:     {data['exclusive_lock']}")
    if data.get("timeout_seconds"):
        print(f"Timeout:  {data['timeout_seconds']}s")
    if data.get("max_retries"):
        print(f"Retries:  {data['max_retries']} (delay {data.get('retry_delay_seconds', 0)}s)")
    print(f"Created:  {_utc_to_local(data.get('created_at', ''))}")
    print(f"Updated:  {_utc_to_local(data.get('updated_at', ''))}")

    execs = data.get("recent_executions", [])
    if execs:
        print(f"\nRecent executions ({len(execs)}):")
        efmt = "  {:<6} {:<10} {:<6} {:<20} {:<20}"
        print(efmt.format("ID", "STATUS", "EXIT", "STARTED", "FINISHED"))
        print("  " + "-" * 64)
        for e in execs:
            print(efmt.format(
                e.get("id", ""),
                e.get("status", ""),
                str(e.get("exit_code", "")),
                _utc_to_local(str(e.get("started_at", "")))[:20],
                _utc_to_local(str(e.get("finished_at", "")))[:20],
            ))
    else:
        print("\nNo recent executions.")


# -- Subcommand handlers --


def cmd_init(args: argparse.Namespace) -> None:
    """Scaffold a vault directory, initialize stores, and register the overlord job."""
    from .job_store import DEFAULT_DATA_DIR, JobStore
    from .models import Job
    from .vault_template import VAULT_CLAUDE_MD

    vault = Path(args.path).resolve()
    vault.mkdir(parents=True, exist_ok=True)

    # Write CLAUDE.md
    claude_md = vault / "CLAUDE.md"
    if claude_md.exists():
        print(f"CLAUDE.md already exists at {claude_md}, skipping")
    else:
        claude_md.write_text(VAULT_CLAUDE_MD)
        print(f"Created {claude_md}")

    # Install skill command files into .claude/commands/
    _TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
    commands_src = _TEMPLATES_DIR / "skills"
    commands_dest = vault / ".claude" / "commands"
    commands_dest.mkdir(parents=True, exist_ok=True)
    for src_file in sorted(commands_src.glob("*.md")):
        dest_file = commands_dest / src_file.name
        if dest_file.exists():
            print(f"{dest_file.name} already exists in .claude/commands/, skipping")
        else:
            shutil.copy2(src_file, dest_file)
            print(f"Installed skill: /{ src_file.stem}")

    # Copy wrapper script into the vault
    dest_script = vault / "overlord_job.sh"
    if dest_script.exists():
        print(f"overlord_job.sh already exists at {dest_script}, skipping")
    else:
        shutil.copy2(_OVERLORD_JOB_SCRIPT, dest_script)
        dest_script.chmod(0o755)
        print(f"Copied wrapper script to {dest_script}")

    # Initialize the job store at the shared default location so that
    # `overlord init && overlord daemon` works without any --data-dir flags.
    data_dir = DEFAULT_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)
    job_store = JobStore(data_dir=data_dir)
    print(f"Initialized job store at {data_dir}")

    # Register the overlord job (direct store write, bypassing MCP)
    existing = job_store.get_job_by_name("overlord")
    if existing:
        print(f"Job 'overlord' already registered, skipping")
    else:
        job = Job(
            name="overlord",
            cron_expression="*/5 * * * *",
            command=str(dest_script),
            consumes=["overlord"],
            timeout_seconds=300,
        )
        created = job_store.create_job(job)
        print(f"Registered job 'overlord'")

    print(f"\nVault initialized at {vault}")
    print(f"Start the daemon with: overlord daemon")


def cmd_daemon(args: argparse.Namespace) -> None:
    """Start the scheduler daemon."""
    from .logging_config import setup_logging
    from .scheduler import Scheduler

    setup_logging(log_file=args.log_file)

    scheduler = Scheduler(
        data_dir=Path(args.data_dir) if args.data_dir else None,
        tick_seconds=args.tick,
        mcp_host=args.mcp_host,
        mcp_port=args.mcp_port,
    )
    asyncio.run(scheduler.run())


def cmd_list(args: argparse.Namespace) -> None:
    """List registered jobs."""
    arguments: dict = {}
    if args.status:
        arguments["status"] = args.status
    raw = asyncio.run(_call_tool(args.mcp_url, "list_jobs", arguments))
    _print_job_table(raw)


def cmd_status(args: argparse.Namespace) -> None:
    """Show a job's details and recent executions."""
    raw = asyncio.run(_call_tool(args.mcp_url, "get_job_status", {"name": args.name}))
    _print_job_status(raw)


def cmd_register(args: argparse.Namespace) -> None:
    """Register a new job."""
    arguments: dict = {
        "name": args.name,
        "cron_expression": args.cron,
        "command": args.command,
    }
    if args.lock:
        arguments["exclusive_lock"] = args.lock
    if args.timeout is not None:
        arguments["timeout_seconds"] = args.timeout
    if args.retries is not None:
        arguments["max_retries"] = args.retries
    if args.retry_delay is not None:
        arguments["retry_delay_seconds"] = args.retry_delay
    if args.consumes:
        arguments["consumes"] = args.consumes
    if args.queue:
        arguments["queue_name"] = args.queue

    raw = asyncio.run(_call_tool(args.mcp_url, "register_job", arguments))
    data = json.loads(raw)
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)
    print(f"Registered job '{data['name']}'")


def cmd_update(args: argparse.Namespace) -> None:
    """Update an existing job's parameters."""
    arguments: dict = {"name": args.name}
    if args.cron is not None:
        arguments["cron_expression"] = args.cron
    if args.command is not None:
        arguments["command"] = args.command
    if args.lock is not None:
        arguments["exclusive_lock"] = args.lock
    if args.timeout is not None:
        arguments["timeout_seconds"] = args.timeout
    if args.retries is not None:
        arguments["max_retries"] = args.retries
    if args.retry_delay is not None:
        arguments["retry_delay_seconds"] = args.retry_delay
    if args.consumes is not None:
        arguments["consumes"] = args.consumes
    if args.queue is not None:
        arguments["queue_name"] = args.queue

    if len(arguments) == 1:
        print("Error: no fields to update. Provide at least one option.", file=sys.stderr)
        sys.exit(1)

    raw = asyncio.run(_call_tool(args.mcp_url, "update_job", arguments))
    data = json.loads(raw)
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)
    print(f"Updated job '{data['name']}'")


def cmd_unregister(args: argparse.Namespace) -> None:
    """Remove a job by name."""
    raw = asyncio.run(_call_tool(args.mcp_url, "unregister_job", {"name": args.name}))
    data = json.loads(raw)
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)
    print(f"Unregistered job '{data['name']}'")


def cmd_trigger(args: argparse.Namespace) -> None:
    """Manually trigger a job."""
    raw = asyncio.run(_call_tool(args.mcp_url, "trigger_job", {"name": args.name}))
    data = json.loads(raw)
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)
    print(f"Triggered job '{data['job']['name']}' — use 'overlord status {args.name}' to check progress")


def cmd_stop(args: argparse.Namespace) -> None:
    """Ask the daemon to shut down gracefully."""
    raw = asyncio.run(_call_tool(args.mcp_url, "shutdown_daemon", {}))
    data = json.loads(raw)
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)
    print("Shutdown initiated")


def cmd_rotate_log(args: argparse.Namespace) -> None:
    """Ask the daemon to rotate its log file."""
    raw = asyncio.run(_call_tool(args.mcp_url, "rotate_log", {}))
    data = json.loads(raw)
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        sys.exit(1)
    print(data.get("status", "done"))


def cmd_send(args: argparse.Namespace) -> None:
    """Send a message by writing directly to the file-based spool."""
    from . import maildir as _maildir
    from . import spool as _spool

    payload = args.payload
    if payload is None:
        if sys.stdin.isatty():
            print("Error: --payload not provided and stdin is a terminal. "
                  "Provide --payload or pipe data via stdin.", file=sys.stderr)
            sys.exit(1)
        payload = sys.stdin.read()

    spool_writer = _spool.SpoolWriter()
    email_msg = _maildir.MaildirStore.build_message(
        payload=payload,
        consumer=args.consumer,
        job_name="cli",
    )
    spool_path = spool_writer.write(email_msg)
    consumer_info = f" → {args.consumer}" if args.consumer else ""
    print(f"Message spooled ({spool_path.name}{consumer_info})")


def cmd_messages(args: argparse.Namespace) -> None:
    """Query messages by reading directly from Maildir directories."""
    from . import maildir as _maildir

    store = _maildir.MaildirStore()
    CATCHALL = _maildir.CATCHALL

    if args.consume:
        if not args.consumer and not args.no_consumer:
            print("Error: --consume requires --consumer or --no-consumer", file=sys.stderr)
            sys.exit(1)

    # Determine which mailboxes to read
    if args.no_consumer:
        targets = [CATCHALL]
    elif args.consumer:
        targets = [args.consumer]
    else:
        targets = store.list_mailboxes()

    # Determine whether to show unconsumed, consumed, or both
    # --unconsumed: show only unconsumed; default (no flag): show both
    show_unconsumed = True
    show_consumed = not args.unconsumed

    raw_messages: list[tuple[dict, bool, str]] = []  # (msg, consumed_flag, target)
    for target in targets:
        if args.consume or show_unconsumed:
            for m in store.fetch_messages(target):
                raw_messages.append((m, False, target))
        if show_consumed:
            for m in store.fetch_processed_messages(target):
                raw_messages.append((m, True, target))

    # Filter by source job name
    if args.job:
        raw_messages = [
            (m, c, t) for m, c, t in raw_messages
            if m.get("subject", "").split(" ", 1)[0] == args.job
        ]

    # Apply limit
    limit = args.limit if args.limit is not None else 20
    raw_messages = raw_messages[:limit]

    # If consuming, move messages to processed
    if args.consume:
        for m, _, target in raw_messages:
            store.consume(target, m["key"])

    # Build result in the format expected by _print_messages
    result = []
    for m, consumed_flag, _ in raw_messages:
        payload = m["payload"]
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            pass
        subject = m.get("subject", "")
        job_name = subject.split(" ", 1)[0] if subject else "(unknown)"
        result.append({
            "id": m["key"],
            "source_job_name": job_name,
            "payload": payload,
            "consumer": m.get("consumer"),
            "consumed": True if args.consume else consumed_flag,
            "created_at": m.get("date"),
            "consumed_at": None,
        })

    _print_messages(json.dumps(result), text=args.text, as_jsonl=args.jsonl)


# -- Argument parser --


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="overlord",
        description="Overlord — repeatable tasks manager CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # init
    p_init = sub.add_parser("init", help="Scaffold a vault directory with stores and overlord job")
    p_init.add_argument("path", nargs="?", default=".", help="Vault directory path (default: current directory)")
    p_init.set_defaults(func=cmd_init)

    # daemon
    p_daemon = sub.add_parser("daemon", help="Start the scheduler daemon")
    p_daemon.add_argument("--data-dir", metavar="PATH", help="Path to data directory")
    p_daemon.add_argument("--tick", type=int, default=60, help="Tick interval in seconds (default: 60)")
    p_daemon.add_argument("--mcp-host", default="127.0.0.1", help="MCP bind address (default: 127.0.0.1)")
    p_daemon.add_argument("--mcp-port", type=int, default=8000, help="MCP port (default: 8000)")
    p_daemon.add_argument("--log-file", metavar="PATH", help="Write logs to FILE in addition to stderr")
    p_daemon.set_defaults(func=cmd_daemon)

    # list
    p_list = sub.add_parser("list", help="List registered jobs")
    p_list.add_argument("--status", choices=["enabled", "disabled", "paused"], help="Filter by status")
    p_list.add_argument("--mcp-url", default=DEFAULT_MCP_URL, help=f"MCP server URL (default: {DEFAULT_MCP_URL})")
    p_list.set_defaults(func=cmd_list)

    # status
    p_status = sub.add_parser("status", help="Show job details and recent executions")
    p_status.add_argument("name", help="Job name")
    p_status.add_argument("--mcp-url", default=DEFAULT_MCP_URL, help=f"MCP server URL (default: {DEFAULT_MCP_URL})")
    p_status.set_defaults(func=cmd_status)

    # register
    p_reg = sub.add_parser("register", help="Register a new job")
    p_reg.add_argument("--name", required=True, help="Unique job name")
    p_reg.add_argument("--cron", required=True, help="Cron expression (5-field)")
    p_reg.add_argument("--command", required=True, help="Shell command to execute")
    p_reg.add_argument("--lock", metavar="NAME", help="Exclusive lock name")
    p_reg.add_argument("--timeout", type=int, metavar="SEC", help="Timeout in seconds")
    p_reg.add_argument("--retries", type=int, metavar="N", help="Max retries")
    p_reg.add_argument("--retry-delay", type=int, metavar="SEC", help="Delay between retries in seconds")
    p_reg.add_argument("--consumes", metavar="NAMES", help="Comma-separated consumer names or '*' for catch-all")
    p_reg.add_argument("--queue", metavar="NAME", help="Execution queue name (default: 'default')")
    p_reg.add_argument("--mcp-url", default=DEFAULT_MCP_URL, help=f"MCP server URL (default: {DEFAULT_MCP_URL})")
    p_reg.set_defaults(func=cmd_register)

    # update
    p_upd = sub.add_parser("update", help="Update an existing job's parameters")
    p_upd.add_argument("--name", required=True, help="Job name to update")
    p_upd.add_argument("--cron", metavar="EXPR", help="New cron expression (5-field)")
    p_upd.add_argument("--command", metavar="CMD", help="New shell command")
    p_upd.add_argument("--lock", metavar="NAME", help="New exclusive lock name (empty to clear)")
    p_upd.add_argument("--timeout", type=int, metavar="SEC", help="New timeout in seconds")
    p_upd.add_argument("--retries", type=int, metavar="N", help="New max retries")
    p_upd.add_argument("--retry-delay", type=int, metavar="SEC", help="New retry delay in seconds")
    p_upd.add_argument("--consumes", metavar="NAMES", help="New consumer names (comma-separated, empty to clear)")
    p_upd.add_argument("--queue", metavar="NAME", help="New execution queue name")
    p_upd.add_argument("--mcp-url", default=DEFAULT_MCP_URL, help=f"MCP server URL (default: {DEFAULT_MCP_URL})")
    p_upd.set_defaults(func=cmd_update)

    # unregister
    p_unreg = sub.add_parser("unregister", help="Remove a job by name")
    p_unreg.add_argument("name", help="Job name")
    p_unreg.add_argument("--mcp-url", default=DEFAULT_MCP_URL, help=f"MCP server URL (default: {DEFAULT_MCP_URL})")
    p_unreg.set_defaults(func=cmd_unregister)

    # trigger
    p_trig = sub.add_parser("trigger", help="Manually trigger a job")
    p_trig.add_argument("name", help="Job name")
    p_trig.add_argument("--mcp-url", default=DEFAULT_MCP_URL, help=f"MCP server URL (default: {DEFAULT_MCP_URL})")
    p_trig.set_defaults(func=cmd_trigger)

    # send
    p_send = sub.add_parser("send", help="Send a message via file-based spool")
    p_send.add_argument("--consumer", metavar="NAME", help="Consumer name the message is addressed to")
    p_send.add_argument("--payload", metavar="TEXT", help="Message payload (reads stdin if omitted)")
    p_send.set_defaults(func=cmd_send)

    # messages
    p_msg = sub.add_parser("messages", help="Query messages")
    p_msg.add_argument("--job", metavar="NAME", help="Filter by source job name")
    consumer_group = p_msg.add_mutually_exclusive_group()
    consumer_group.add_argument("--consumer", metavar="NAME", help="Filter by consumer tag")
    consumer_group.add_argument("--no-consumer", action="store_true", help="Show only messages without a consumer")
    p_msg.add_argument("--unconsumed", action="store_true", help="Show only unconsumed messages")
    p_msg.add_argument("--consume", action="store_true", help="Consume all matching unconsumed messages")
    p_msg.add_argument("--limit", type=int, metavar="N", help="Maximum number of results")
    output_group = p_msg.add_mutually_exclusive_group()
    output_group.add_argument("--text", action="store_true", help="Print full message contents in plain-text format")
    output_group.add_argument("--jsonl", action="store_true", help="Print messages as JSON Lines (one object per line)")
    p_msg.set_defaults(func=cmd_messages)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    # Hidden commands: not shown in --help but still usable.
    if argv and argv[0] in ("stop", "rotate-log"):
        ns = argparse.Namespace(mcp_url=DEFAULT_MCP_URL)
        rest = argv[1:]
        for i, arg in enumerate(rest):
            if arg == "--mcp-url" and i + 1 < len(rest):
                ns.mcp_url = rest[i + 1]
        if argv[0] == "stop":
            cmd_stop(ns)
        else:
            cmd_rotate_log(ns)
        return
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
