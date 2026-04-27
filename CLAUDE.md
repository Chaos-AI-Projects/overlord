# Overlord

Repeatable tasks manager for AI agents. Provides cron-based job scheduling, message passing between jobs, exclusive locking, and an MCP interface for agent-driven management.

Included in the mono-repo via `git subtree` from `git@github.com:Chaos-AI-Projects/overlord.git` (branch: `main`, squashed).

## Quick Reference

```bash
# Install (editable)
pip install -e overlord

# Run scheduler daemon
overlord daemon [--data-dir PATH] [--tick N] [--mcp-host HOST] [--mcp-port PORT] [--log-file PATH]

# Manage jobs via CLI (talks to daemon over MCP)
overlord list [--status STATUS] [--mcp-url URL]
overlord status JOB_NAME [--mcp-url URL]
overlord register --name NAME --cron EXPR --command CMD [options] [--mcp-url URL]
overlord update --name NAME [--cron EXPR] [--command CMD] [options] [--mcp-url URL]
overlord unregister JOB_NAME [--mcp-url URL]
overlord trigger JOB_NAME [--mcp-url URL]
overlord messages [--job NAME] [--consumer NAME] [--unconsumed] [--limit N] [--text] [--mcp-url URL]
overlord send [--consumer NAME] [--payload TEXT] [--mcp-url URL]
overlord stop [--mcp-url URL]       # hidden from --help; asks daemon to exit gracefully
overlord log-path [--data-dir PATH] [--log-file PATH]  # hidden; prints resolved log file path
overlord rotate-log [--mcp-url URL] # hidden from --help; rotates daemon log file

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
│   ├── job_store.py         # JSON file-based job storage (one file per job)
│   ├── execution_log.py     # JSON-lines append-only execution history
│   ├── lock_store.py        # File-based named locks (O_CREAT|O_EXCL)
│   ├── maildir.py           # Maildir-backed message delivery
│   ├── spool.py             # Async spool for message delivery to Maildir
│   ├── models.py            # Data models (Job, ExecutionRecord, Lock, JobOutput)
│   ├── cron.py              # Cron expression parser
│   ├── mcp_server.py        # MCP server for agent-driven job management
│   ├── vault_template.py    # Template for `overlord init` vault scaffolding
│   └── logging_config.py    # Logging setup
├── tests/                   # Test files
├── scripts/                 # Utility scripts (e.g., migration)
├── pyproject.toml           # Package metadata & dependencies
└── LICENSE
```

## Architecture

### Storage Backends

All state is stored as plain files under `$XDG_DATA_HOME/overlord/` (default: `~/.local/share/overlord/`):

| Component | Backend | Path |
|-----------|---------|------|
| Job definitions | JSON files (one per job) | `jobs/<name>.json` |
| Execution history | Append-only JSON-lines | `execution.log` |
| Execution ID counter | Plain text integer | `execution_id` |
| Named locks | Empty files (O_CREAT\|O_EXCL) | `locks/<lock_name>` |
| Messages | Maildir (RFC 822 envelopes) | `mailboxes/<consumer>/` |
| Delivery queue | File-based spool | `spool/` |

Concurrency is handled via `flock(2)` for job files and the ID counter, `O_CREAT|O_EXCL` for locks, and POSIX `O_APPEND` atomicity for the execution log.

### Scheduler

- Asyncio-based loop that ticks once per minute (configurable)
- Evaluates enabled jobs against their cron expressions
- Dispatches due jobs via the executor as concurrent asyncio tasks
- Queue-based execution ordering: jobs on the same queue run serially
- Handles SIGTERM/SIGINT for graceful shutdown
- Cleans up stale locks and marks orphaned executions as failed on startup

### Job Execution

- Jobs run as subprocesses with optional exclusive locking
- Configurable timeout, max retries, and retry delay
- Structured output: successful jobs emit JSON `{"consumer": ..., "message": ...}` on stdout
- Output is parsed via `JobOutput.from_stdout()` and delivered to the spool

### Message Passing

- Jobs produce messages delivered via the spool to Maildir
- Messages are RFC 822 envelopes with a `payload.json` attachment
- Consumer jobs declare what they consume via `Job.consumes` list
- Consumer jobs only run when matching unconsumed messages exist in their Maildir
- Messages are passed to consumer jobs via stdin and auto-marked consumed on success
- Spool processor runs as an async task alongside the scheduler, woken by SIGUSR1

### MCP Interface

- StreamableHTTP MCP server runs alongside the scheduler
- Exposes tools: register/unregister jobs, list jobs, trigger execution, query/send messages, shutdown daemon
- CLI commands communicate with the daemon through MCP (avoids direct file access)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `XDG_DATA_HOME` | `~/.local/share` | Data directory (state stored under `$XDG_DATA_HOME/overlord/`) |
| `TZ` | `UTC` | Timezone for cron schedule evaluation and log/CLI timestamp display (e.g., `Australia/Sydney`) |

## Container

### Prerequisites

- [Nix](https://nixos.org/download/) with flakes enabled

### Building the image

From the current directory:

```bash
nix build .#container
```

This produces a `result` symlink pointing to a layered OCI image tarball (~274 MB).

### Loading the image

```bash
# Podman
podman load < result

# Docker
docker load < result
```

The image is tagged `overlord:latest`.

### Running the container

```bash
# Create a persistent volume directory
mkdir -p ~/overlord-data

# Run with podman (--userns=keep-id maps host UID into the container)
podman run -d \
  --name overlord \
  --userns=keep-id \
  -p 8000:8000 \
  -v ~/overlord-data:/home/overlord:Z \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  overlord:latest
```

Replace `podman` with `docker` if using Docker. The container:

- Exposes the MCP server on port **8000**
- Persists all state (jobs, execution log, mailboxes, claude-code install, brain/) in the mounted volume at `/home/overlord`
- Auto-installs `claude-code` on first start into the volume
- UID mapping is handled by podman (`--userns=keep-id`) rather than inside the container
- Passes any extra arguments to `overlord daemon` (e.g., `--tick 30`)

### Building just the Python package

From the current directory:

```bash
nix build .#overlord
# or: nix build  (overlord is the default package)
```

## Key Dependencies

Python 3.10+: mcp
