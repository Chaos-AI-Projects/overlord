Register a new scheduled job in the overlord system.

Gather the following information, then run the `overlord register` command:

1. **Job name** — a short, descriptive, unique identifier (e.g., `github-fetcher`, `daily-report`)
2. **Cron expression** — a 5-field cron schedule: `minute hour day-of-month month day-of-week`
   - Examples: `*/5 * * * *` (every 5 min), `0 */2 * * *` (every 2 hours), `0 9 * * 1-5` (weekdays at 9am)
3. **Command** — the shell command to execute
4. **Optional parameters**:
   - `--consumes <names>` — comma-separated consumer names (makes this a consumer job that only runs when matching messages exist)
   - `--timeout <seconds>` — maximum execution time
   - `--lock <name>` — exclusive lock to prevent concurrent runs
   - `--retries <N>` — max retries on failure
   - `--retry-delay <seconds>` — delay between retries

## Command

```bash
overlord register \
  --name <name> \
  --cron "<cron-expression>" \
  --command "<command>" \
  [--consumes <consumer-names>] \
  [--timeout <seconds>] \
  [--lock <lock-name>] \
  [--retries <N>] \
  [--retry-delay <seconds>]
```

## Important

- Jobs must emit JSON on stdout: `{"consumer": "<name-or-null>", "message": "<payload>"}`
- When creating jobs that invoke `claude`, use `claude -p --output-format text --dangerously-skip-permissions` for non-interactive mode.
- Wrap complex job logic in shell scripts placed in this vault directory.
- Use `overlord list` to verify the job was registered successfully.
