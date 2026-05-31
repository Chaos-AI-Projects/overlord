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

4. **Check out when done** — after completing the task (just before emitting final output), clear your presence. If you discovered a follow-up the *next* run should pick up (work you found but did not finish, or a logical next step), pass it via `--what-next` so the continuity loop can hand it forward:

```bash
python -m overlord.scripts.presence checkout [--what-next "<follow-up for the next run>"]
```

> **Note:** The checkout writes a "finished" record to the presence consumer. The `/self-monitor` skill uses this to distinguish active work from completed work, and forwards any `--what-next` note to the `overlord` consumer so the follow-up is not lost between runs. Always check out, even if the task failed — this prevents false alerts about unfinished work. Omit `--what-next` when there is no follow-up.

## Important

- The `checkin` and `checkout` commands read `OVERLORD_EXECUTION_ID` and `OVERLORD_JOB_NAME` from environment variables automatically — do not pass them as arguments.
- The description should be a brief summary of the task (e.g., "processing GitHub activity for memory-solution", "sending daily email digest").
- Pass `--what-next` only when there is a concrete handoff for the next run; make it a specific, actionable note rather than a generic status line.
- This skill shares a lock with `/self-monitor` — they cannot run concurrently.
- Every job should invoke `/mindful` after understanding the task (not at the very start), **except** when the task is `/self-monitor` itself. For example, after reading an email and knowing what to fix or answer, or after fetching data and knowing what to process.
