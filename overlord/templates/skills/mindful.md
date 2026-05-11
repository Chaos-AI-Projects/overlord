---
description: Record your current task and see what other jobs have been doing recently
---

Check in with the presence system to record what you are working on and see recent activity from other jobs.

## Steps

1. **Check in** — record your current task:

```bash
python -m overlord.scripts.presence checkin "<brief description of current task>"
```

2. **View recent activity** — see what the last 5 jobs were doing:

```bash
python -m overlord.scripts.presence recent
```

> **Note:** Your own check-in from step 1 will NOT appear in the recent output.
> The checkin writes to the delivery spool, which is processed asynchronously,
> so it has not reached the maildir yet. This is expected — the recent list
> shows *other* jobs' activity, which is the useful context.

3. Return the recent activity to your caller so they have context on what other jobs have been doing.

## Important

- The `checkin` command reads `OVERLORD_EXECUTION_ID` and `OVERLORD_JOB_NAME` from environment variables automatically — do not pass them as arguments.
- The description should be a brief summary of the task (e.g., "processing GitHub activity for memory-solution", "sending daily email digest").
- This skill shares a lock with `/self-monitor` — they cannot run concurrently.
- Every job should invoke `/mindful` at the start of its task, **except** when the task is `/self-monitor` itself.
