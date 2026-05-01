# Overlord

Repeatable tasks manager for AI agents. Provides cron-based job scheduling, message passing between jobs, exclusive locking, and an MCP interface for agent-driven management.

## Architecture Overview

```
                         +---------------------+
                         |  Scheduler Daemon   |
                         |                     |
  CLI Client ---MCP/HTTP--->  MCP Server       |
  AI Agent   ---MCP/HTTP--->    |              |
                         |     +---> Scheduler Loop --evaluate cron--> jobs/*.json
                         |     |        |                              (Job Definitions)
                         |     |     dispatch
                         |     |        |
                         |     |        v
                         |     |    Executor ---subprocess---> Job Process
                         |     |        |   <--stdout JSON---     |
                         |     |        |---acquire/release--> locks/*
                         |     |        |---write------------> execution.log
                         |     |        |---deliver output---> spool/
                         |     |                               (Delivery Queue)
                         |     |    Spool Processor
                         |     |        |---SIGUSR1 wake
                         |     |        |---move to Maildir--> mailboxes/consumer/
                         +-----+-------------------------------+
                                                               (Maildir Messages)

                   File-Based Storage: ~/.local/share/overlord/
```

## Job Lifecycle

```
  overlord register
        |
        v
   Registered --> Enabled
                    |
               cron matches
                    |
                    v
                   Due
                    |
          executor starts subprocess
                    |
                    v
                 Running
                /   |   \
               /    |    \
              v     v     v
         Success  Failed  Timeout
           |        |       |
           |    retries?    end
           |     /    \
           |    v      v
           |  Retrying end
           |    |
           |  (after retry_delay --> Running)
           |
      output has consumer?
         /          \
        v            v
  MessageDelivery   end
        |
        v
      Spool
        |
  spool processor
        |
        v
     Maildir
```

## Message Flow

```
  Producer Job        Executor          Spool           Maildir          Consumer Job
      |                  |                |                |                  |
      |--exit 0 + JSON-->|                |                |                  |
      |                  |--deliver msg-->|                |                  |
      |                  |--SIGUSR1 wake->|                |                  |
      |                  |                |--move to       |                  |
      |                  |                |  consumer/new->|                  |
      |                  |                |                |(RFC 822 envelope |
      |                  |                |                | + payload.json)  |
      |                  |--check unconsumed msgs--------->|                  |
      |                  |                |                |--msgs via stdin->|
      |                  |                |                |<--auto-consumed--|
      |                  |                |                |   on success     |
```

## Installation

```bash
# Install (editable, from mono-repo root)
pip install -e overlord

# Or build with Nix
cd overlord && nix build .#overlord
```

Requires Python 3.10+ and [mcp](https://pypi.org/project/mcp/) >= 1.0.0.

## CLI Usage

Start the daemon, then manage jobs through CLI commands that talk to the daemon over MCP.

### Daemon

```bash
overlord daemon [--data-dir PATH] [--tick N] [--mcp-host HOST] [--mcp-port PORT] [--log-file PATH]
```

Starts the scheduler loop, spool processor, and MCP server. Default MCP endpoint: `http://127.0.0.1:8000/mcp/`. Logs are written to `<data-dir>/overlord.log` by default (override with `--log-file`).

### Job Management

```bash
# List all jobs (optionally filter by status)
overlord list [--status enabled|disabled|paused] [--mcp-url URL]

# Show job details and recent executions
overlord status JOB_NAME [--mcp-url URL]

# Register a new job
overlord register --name NAME --cron EXPR --command CMD \
  [--lock LOCK_NAME] [--timeout SECONDS] \
  [--max-retries N] [--retry-delay SECONDS] \
  [--consumes NAME ...] [--queue QUEUE_NAME] \
  [--mcp-url URL]

# Update an existing job
overlord update --name NAME [--cron EXPR] [--command CMD] [options] [--mcp-url URL]

# Remove a job
overlord unregister JOB_NAME [--mcp-url URL]

# Manually trigger a job
overlord trigger JOB_NAME [--mcp-url URL]
```

### Messages

```bash
# Query messages
overlord messages [--job NAME] [--consumer NAME] [--unconsumed] [--limit N] [--text] [--mcp-url URL]

# Send a message via spool
overlord send [--consumer NAME] [--payload TEXT] [--mcp-url URL]
```

### Daemon Control

```bash
# Graceful daemon shutdown (drains queues first)
overlord stop [--mcp-url URL]

# Show daemon state, jobs, queues, log path, mailboxes
overlord daemon-status [--mcp-url URL]

# Print resolved log file path
overlord log-path [--data-dir PATH] [--log-file PATH]

# Rotate daemon log file
overlord rotate-log [--mcp-url URL]
```

### Vault Scaffolding

```bash
# Scaffold a new vault directory with stores and an overlord job
overlord init [PATH]
```

`overlord init` uses a **git-based template management** strategy to safely handle skill upgrades without overwriting user modifications:

- Templates (skills, `CLAUDE.md`, `overlord_job.sh`) are always written to an `origin/` subdirectory inside the vault, serving as an upstream reference copy.
- Working copies in their normal locations (`.claude/commands/`, `./CLAUDE.md`, etc.) are only updated if they haven't been locally modified — detected by comparing the working copy against the previous `origin/` version.
- Locally modified files are skipped with a warning; the new version is available in `origin/` for manual comparison.
- If git is available, `overlord init` will initialize the vault as a git repo (or use the existing repo if the vault is already inside one). Template updates and working copy merges are committed automatically.
- **Note:** When the vault is inside an existing git repository, init commits will appear in that repository's history.
- If git is not available, `overlord init` falls back to simple copy-and-skip behavior (files are copied only if they don't already exist).
- After initialization, an `init_complete` message is sent to confirm the vault is ready.

## Job Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | *(required)* | Unique job identifier |
| `cron_expression` | string | *(required)* | Cron schedule (minute, hour, day, month, weekday) |
| `command` | string | *(required)* | Shell command to execute |
| `status` | enum | `enabled` | `enabled`, `disabled`, or `paused` |
| `exclusive_lock` | string | `null` | Named lock — prevents concurrent runs |
| `timeout_seconds` | int | `null` | Kill the subprocess after this many seconds |
| `max_retries` | int | `0` | Number of retry attempts on failure |
| `retry_delay_seconds` | int | `0` | Seconds to wait between retries |
| `consumes` | list | `[]` | Consumer mailbox names (job runs only when unconsumed messages exist) |
| `queue_name` | string | `"default"` | Execution queue — jobs on the same queue run serially |

## Structured Job Output

Successful jobs (exit 0) must emit JSON on stdout:

```json
{"consumer": "job-name", "message": "payload string or object"}
```

- `consumer` — target mailbox name (or `null` for unaddressed messages)
- `message` — string or JSON object delivered as the message payload

If stdout is empty or not valid JSON, the execution is marked as failed.

## Storage Layout

All state lives under `$XDG_DATA_HOME/overlord/` (default: `~/.local/share/overlord/`):

```
overlord/
├── jobs/               # One JSON file per job definition
│   ├── fetch-data.json
│   └── process-data.json
├── execution.log       # Append-only JSON-lines execution history
├── execution_id        # Monotonic ID counter (plain text integer)
├── locks/              # Empty files created with O_CREAT|O_EXCL
├── mailboxes/          # Maildir-backed message storage
│   └── <consumer>/
│       ├── new/        # Newly delivered messages
│       ├── cur/        # Consumed messages
│       └── tmp/        # In-flight deliveries
└── spool/              # File-based delivery queue
```

Concurrency safety:
- Job files and ID counter: `flock(2)` advisory locks
- Named locks: atomic `O_CREAT|O_EXCL` file creation
- Execution log: POSIX `O_APPEND` atomic writes

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `XDG_DATA_HOME` | `~/.local/share` | Data directory (state stored under `$XDG_DATA_HOME/overlord/`) |
| `TZ` | `UTC` | Timezone for cron evaluation and timestamp display |

## Container

### Prerequisites

- [Nix](https://nixos.org/download/) with flakes enabled

### Build

```bash
cd overlord
nix build .#container
```

Produces a `result` symlink pointing to a layered OCI image tarball.

### Load

```bash
# Podman
podman load < result

# Docker
docker load < result
```

The image is tagged `overlord:latest`.

### Run

```bash
mkdir -p ~/overlord-data

podman run -d \
  --name overlord \
  --userns=keep-id \
  -p 8000:8000 \
  -v ~/overlord-data:/home/overlord:Z \
  -v /run/user/$(id -u)/podman/podman.sock:/run/podman/podman.sock:Z \
  -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  -e CONTAINER_HOST=unix:///run/podman/podman.sock \
  overlord:latest
```

Replace `podman` with `docker` if using Docker. The container:

- Exposes the MCP server on port **8000**
- Persists all state (jobs, execution log, mailboxes, claude-code install, brain/) in the mounted volume at `/home/overlord`
- Auto-installs `claude-code` on first start into the volume
- UID mapping is handled by podman (`--userns=keep-id`) rather than inside the container
- Passes any extra arguments to `overlord daemon` (e.g., `--tick 30`)
- Mounts the host's podman socket so `podman-remote` can manage containers from inside
- Includes `podman`, `python3` (with pip/venv/pandas), `matrix-commander`, and other tools
- Timezone symlinks are baked into the image at build time (no runtime setup needed)

### Build Just the Python Package

```bash
cd overlord
nix build .#overlord   # or: nix build (default package)
```

## Testing

```bash
pytest overlord/tests -v
```

## Migrating from SQLite

If upgrading from an older SQLite-backed Overlord installation, use the bundled migration script:

```bash
python -m overlord.scripts.migrate_sqlite_to_json [--db PATH] [--data-dir PATH]
```

This reads jobs and execution history from the legacy `overlord.db` SQLite database and writes them as JSON files into the file-based storage layout. The original database is not modified.

## License

See [LICENSE](LICENSE).
