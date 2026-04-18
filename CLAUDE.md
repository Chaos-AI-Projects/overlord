# Overlord

Repeatable tasks manager for AI agents. Provides cron-based job scheduling, message passing between jobs, exclusive locking, and an MCP interface for agent-driven management.

Included in the mono-repo via `git subtree` from `git@github.com:Chaos-AI-Projects/overlord.git` (branch: `main`, squashed).

## Quick Reference

```bash
# Install (editable)
pip install -e overlord

# Run scheduler daemon
overlord daemon [--db PATH] [--tick N] [--mcp-host HOST] [--mcp-port PORT]

# Manage jobs via CLI (talks to daemon over MCP)
overlord list [--status STATUS] [--mcp-url URL]
overlord status JOB_NAME [--mcp-url URL]
overlord register --name NAME --cron EXPR --command CMD [options] [--mcp-url URL]
overlord unregister JOB_NAME [--mcp-url URL]
overlord trigger JOB_NAME [--mcp-url URL]
overlord send [--consumer NAME] [--payload TEXT] [--mcp-url URL]

# Run tests
pytest overlord/tests -v
```

## Project Structure

```
overlord/
├── overlord/                # Main package
│   ├── cli.py               # CLI entry point (argparse)
│   ├── scheduler.py         # Cron-based async scheduler with graceful shutdown
│   ├── executor.py          # Job execution with locking & retries
│   ├── database.py          # SQLite database layer (WAL mode)
│   ├── models.py            # Data models (Job, ExecutionRecord, Message, Lock, JobOutput)
│   ├── cron.py              # Cron expression parser
│   ├── mcp_server.py        # MCP server for agent-driven job management
│   └── logging_config.py    # Logging setup
├── tests/                   # Test files
├── pyproject.toml           # Package metadata & dependencies
└── LICENSE
```

## Architecture

### Scheduler

- Asyncio-based loop that ticks once per minute
- Evaluates enabled jobs against their cron expressions
- Dispatches due jobs via the executor as concurrent asyncio tasks
- Handles SIGTERM/SIGINT for graceful shutdown
- Cleans up stale locks and validates schema version on startup

### Job Execution

- Jobs run as subprocesses with optional exclusive locking
- Configurable timeout, max retries, and retry delay
- Structured output: successful jobs emit JSON `{"consumer": ..., "message": ...}` on stdout
- Output is parsed via `JobOutput.from_stdout()` and routed to the message hub

### Message Passing

- Jobs can produce messages addressed to a named consumer (or unaddressed)
- Consumer jobs declare what they consume via `Job.consumes` list
- Consumer jobs only run when matching unconsumed messages exist
- Messages are passed to consumer jobs via stdin and auto-marked consumed on success

### MCP Interface

- StreamableHTTP MCP server runs alongside the scheduler
- Exposes tools: register/unregister jobs, list jobs, trigger execution, query messages
- CLI commands communicate with the daemon through MCP (avoids direct DB access)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `XDG_DATA_HOME` | `~/.local/share` | Data directory (DB stored at `$XDG_DATA_HOME/overlord/overlord.db`) |

## Key Dependencies

Python 3.10+: mcp
