---
description: Check job statuses and recover failed tasks
---

Health check of jobs that have checked in via the presence system. Only inspects jobs with presence records — not all registered jobs.

## Steps

1. **Scan presence** — get all presence records to find jobs that have checked in:

```bash
python -m overlord.scripts.presence scan
```

   Extract the unique `job_name` values from the returned JSON. These are the jobs to check.

2. **Check each present job** — for every unique job name from the scan, inspect its recent execution history:

```bash
overlord status <job-name>
```

   Look at the `status` and `exit_code` of recent executions. A job needs attention if:
   - Its most recent execution has `status: failed` or a non-zero `exit_code`
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

4. **Report** — summarize your findings:
   - List jobs that are healthy (recent execution succeeded)
   - List jobs whose failed tasks you investigated and completed
   - List jobs where you fixed the underlying issue (with details of the fix)
   - List jobs that need human attention (with root cause analysis)

## Important

- This skill shares a lock with `/mindful` — they cannot run concurrently.
- Do **not** invoke `/mindful` when running `/self-monitor` (to avoid circular check-ins).
- This is typically triggered every 2 hours by the `self-monitor-trigger` job.
