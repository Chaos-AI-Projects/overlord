---
description: Check job statuses and recover failed tasks
---

Health check of jobs that have checked in via the presence system. Focuses on **unfinished tasks** — jobs that checked in but never checked out.

## Steps

1. **Scan unfinished tasks** — get only presence records without a matching checkout:

```bash
python -m overlord.scripts.presence scan-unfinished
```

   This returns checkin records where the task never called `checkout`. Extract the unique `job_name` values — these are the jobs that may need attention.

   If the list is empty, all recent tasks completed successfully. Skip to step 4.

2. **Check each unfinished job** — for every unique job name from the scan, inspect its recent execution history:

```bash
overlord status <job-name>
```

   Look at the `status` and `exit_code` of recent executions. A job needs attention if:
   - Its most recent execution has `status: failed` or a non-zero `exit_code`
   - It has `status: running` for an unusually long time (possible hang)
   - It has never executed despite being enabled

3. **Recover failed tasks** — for each job that has failed recently:
   - Read the execution log to find the failure details:

   ```bash
   overlord status <job-name>
   ```

     Look at the `stderr`, `stdout`, and `exit_code` of the failed execution to understand **why** it failed.

   - Investigate the root cause — check for common failure patterns:
     - Missing dependencies or configuration
     - Network / API errors (transient vs. permanent)
     - Permission issues
     - Bugs in the job's skill or command

   - Attempt to **finish the failed task** yourself:
     - Read the job's skill/command to understand what it was trying to accomplish
     - Carry out the remaining work directly (e.g., run the commands the job would have run, fix the underlying issue)
     - If the failure was caused by a bug in a skill template or job configuration, fix it and commit the change

   - If you cannot resolve the failure (e.g., requires credentials you don't have, hardware issue, or unclear root cause), send a message to the overlord consumer with your findings:

   ```bash
   overlord send --consumer overlord --payload "Job '<job-name>' failed — <summary of root cause and what was attempted>. Needs human intervention."
   ```

4. **Dispatch what's-next follow-ups** — forward any `--what-next` notes left on finished records to the `overlord` consumer so they are picked up by the next run:

```bash
python -m overlord.scripts.presence dispatch-whatnext
```

   This must run **before** the prune step (step 5): prune deletes finished records, and the `what_next` notes live on those records — pruning first would lose the follow-ups. Dispatch is bounded, deduplicated, and idempotent (each task_id is marked `dispatched` so a note is forwarded at most once), so it is safe to run on every cycle.

5. **Prune old presence records** — clean up finished records:

```bash
python -m overlord.scripts.presence prune
```

6. **Report** — summarize your findings:
   - Total tasks scanned vs. unfinished count
   - List jobs whose failed tasks you investigated and completed
   - List jobs where you fixed the underlying issue (with details of the fix)
   - List jobs that need human attention (with root cause analysis)
   - Number of what's-next follow-ups dispatched (if any)
   - If no unfinished tasks: report "All recent tasks completed successfully"

## Important

- This skill shares a lock with `/mindful` — they cannot run concurrently.
- Do **not** invoke `/mindful` when running `/self-monitor` (to avoid circular check-ins).
- This is typically triggered every 2 hours by the `self-monitor-trigger` job.
