Remove a scheduled job from the overlord system.

Before unregistering, confirm which job to remove:

1. Run `overlord list` to see all registered jobs
2. Run `overlord status <job-name>` to verify the job details and check recent executions
3. Confirm with the user before proceeding

## Command

```bash
overlord unregister <job-name>
```

## Important

- Unregistering a job also deletes its execution history and messages.
- This action cannot be undone.
- If the job is currently running, the active execution will continue but no new runs will be scheduled.
