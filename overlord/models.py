"""Data models for the Overlord repeatable tasks manager."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class JobStatus(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    PAUSED = "paused"


class ExecutionStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class Job:
    """A repeatable job definition stored in the database."""

    name: str
    cron_expression: str
    command: str
    status: JobStatus = JobStatus.ENABLED
    exclusive_lock: Optional[str] = None
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


@dataclass
class Message:
    """A message produced by a job, consumed via polling."""

    source_job_id: int
    payload: str
    created_at: Optional[datetime] = None
    consumed: bool = False
    consumed_at: Optional[datetime] = None
    id: Optional[int] = None


@dataclass
class Lock:
    """A named lock held by a running job execution."""

    lock_name: str
    holder_execution_id: int
    acquired_at: Optional[datetime] = None
    id: Optional[int] = None
