

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class JobState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    id: str
    command: str
    state: JobState
    attempts: int
    max_retries: int
    created_at: str
    updated_at: str
    next_run_at: Optional[str] = None
    last_error: Optional[str] = None
    output_log_path: Optional[str] = None
