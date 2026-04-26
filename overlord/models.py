"""Data models for the Overlord repeatable tasks manager."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Union


class JobStatus(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    PAUSED = "paused"  # Phase 2: used when scheduler suspends a job temporarily


class ExecutionStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"  # Phase 2: set by scheduler when execution exceeds time limit


@dataclass
class Job:
    """A repeatable job definition stored in the database."""

    name: str
    cron_expression: str
    command: str
    status: JobStatus = JobStatus.ENABLED
    exclusive_lock: Optional[str] = None
    timeout_seconds: Optional[int] = None
    max_retries: int = 0
    retry_delay_seconds: int = 0
    consumes: list[str] = field(default_factory=list)
    queue_name: str = "default"
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class ExecutionRecord:
    """A record of a single job execution."""

    job_id: int
    status: ExecutionStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    id: Optional[int] = None
    job_name: Optional[str] = None


@dataclass
class Message:
    """A message produced by a job, optionally addressed to a single consumer.

    When ``consumer`` is None the message is unaddressed and can be picked up
    by a catch-all job (one whose ``consumes`` list contains ``"*"``).
    """

    source_job_id: Optional[int]
    payload: str
    consumer: Optional[str] = None
    created_at: Optional[datetime] = None
    consumed: bool = False
    consumed_at: Optional[datetime] = None
    id: Optional[int] = None


class JobOutputError(Exception):
    """Raised when job stdout does not match the expected output schema."""


@dataclass
class JobOutput:
    """Structured output schema for job stdout.

    Jobs that succeed (exit 0) must emit JSON on stdout matching this schema.
    If stdout does not match, the execution is marked as failed.
    """

    consumer: Optional[str]
    message: Union[str, dict]

    @classmethod
    def from_stdout(cls, stdout: str) -> "JobOutput":
        """Parse and validate job stdout as a JobOutput.

        Raises JobOutputError if the stdout is not valid JSON or does not
        conform to the expected schema.
        """
        if not stdout or not stdout.strip():
            raise JobOutputError("Job produced no output on stdout")

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise JobOutputError(f"Job stdout is not valid JSON: {exc}") from exc

        if not isinstance(data, dict):
            raise JobOutputError(
                f"Job stdout must be a JSON object, got {type(data).__name__}"
            )

        if "message" not in data:
            raise JobOutputError("Missing required field: 'message'")

        consumer = data.get("consumer")
        if consumer is not None and not isinstance(consumer, str):
            raise JobOutputError(
                f"'consumer' must be a string or null, got {type(consumer).__name__}"
            )

        message = data["message"]
        if not isinstance(message, (str, dict)):
            raise JobOutputError(
                f"'message' must be a string or object, got {type(message).__name__}"
            )

        return cls(consumer=consumer, message=message)


@dataclass
class Lock:
    """A named lock held by a running job execution."""

    lock_name: str
    holder_execution_id: int
    acquired_at: Optional[datetime] = None
    id: Optional[int] = None
