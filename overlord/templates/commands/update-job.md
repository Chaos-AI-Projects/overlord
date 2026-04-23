---
description: Update an existing job's parameters in the overlord system
---

Update an existing job's parameters in the overlord system.

1. Run `overlord status <job-name>` to see the current configuration
2. Identify which parameters need to change
3. Run the update command with only the fields that should change

## Command

```bash
overlord update \
  --name <job-name> \
  [--cron "<new-cron-expression>"] \
  [--command "<new-command>"] \
  [--consumes <new-consumer-names>] \
  [--timeout <seconds>] \
  [--lock <lock-name>] \
  [--retries <N>] \
  [--retry-delay <seconds>]
```

Only the provided options are updated; omitted fields remain unchanged.

## Examples

```bash
# Change a job's schedule to run every 10 minutes
overlord update --name my-job --cron "*/10 * * * *"

# Update the command and timeout
overlord update --name my-job --command "./new-script.sh" --timeout 120

# Clear the exclusive lock (pass empty string)
overlord update --name my-job --lock ""
```

## Important

- The `--name` flag identifies which job to update (required).
- Use `overlord status <job-name>` to verify the changes took effect.
