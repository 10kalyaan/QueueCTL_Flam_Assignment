# # queuectl/engine.py

# import json
# from datetime import timedelta
# from typing import Dict, List, Optional

# from .models import Job, JobState
# from .storage import JobStorage
# from .config import Config
# from .backoff import compute_backoff
# from . import utils


# class QueueEngine:
#     """
#     High-level queue logic: enqueue, status, DLQ, transitions.
#     """

#     def __init__(self, storage: JobStorage, config: Config) -> None:
#         self.storage = storage
#         self.config = config

#     def enqueue_job_from_json(self, job_json: str) -> Job:
#         try:
#             data = json.loads(job_json)
#         except json.JSONDecodeError as e:
#             raise ValueError(f"Invalid job JSON: {e}") from e

#         if "command" not in data or not data["command"]:
#             raise ValueError("Job JSON must contain a non-empty 'command' field")

#         job_id = data.get("id") or utils.generate_job_id()
#         max_retries = int(data.get("max_retries", self.config.max_retries))

#         now = utils.utc_now()
#         now_iso = now.isoformat(timespec="seconds")

#         job = Job(
#             id=job_id,
#             command=data["command"],
#             state=JobState.PENDING,
#             attempts=0,
#             max_retries=max_retries,
#             created_at=now_iso,
#             updated_at=now_iso,
#             next_run_at=now_iso,
#             last_error=None,
#             output_log_path=None,
#         )

#         self.storage.enqueue(job)
#         return job

#     def acquire_job_for_worker(self) -> Optional[Job]:
#         now_iso = utils.utc_now_iso()
#         return self.storage.acquire_due_job(now_iso)

#     def complete_job(self, job: Job, output_log_path: Optional[str]) -> None:
#         job.state = JobState.COMPLETED
#         job.updated_at = utils.utc_now_iso()
#         job.next_run_at = None
#         job.last_error = None
#         job.output_log_path = output_log_path
#         self.storage.update_job(job)

#     def fail_job(self, job: Job, error_message: str) -> None:
#         job.attempts += 1
#         job.updated_at = utils.utc_now_iso()
#         job.last_error = error_message

#         if job.attempts >= job.max_retries:
#             job.state = JobState.DEAD
#             job.next_run_at = None
#         else:
#             job.state = JobState.FAILED
#             base = self.config.backoff_base
#             delay_seconds = compute_backoff(base, job.attempts)
#             next_time = utils.utc_now() + timedelta(seconds=delay_seconds)
#             job.next_run_at = next_time.isoformat(timespec="seconds")

#         self.storage.update_job(job)

#     def list_jobs(self, state: Optional[JobState] = None) -> List[Job]:
#         return self.storage.list_jobs(state)

#     def status(self) -> Dict[str, int]:
#         counts = self.storage.counts_by_state()
#         return {state.value: counts.get(state, 0) for state in JobState}

#     def dlq_list(self) -> List[Job]:
#         return self.storage.list_jobs(JobState.DEAD)

#     def dlq_retry(self, job_id: str) -> Job:
#         job = self.storage.get_job(job_id)
#         if not job:
#             raise ValueError(f"No job found with id '{job_id}'")
#         if job.state != JobState.DEAD:
#             raise ValueError(f"Job '{job_id}' is not in DLQ (state={job.state.value})")

#         job.state = JobState.PENDING
#         job.attempts = 0
#         job.updated_at = utils.utc_now_iso()
#         job.next_run_at = job.updated_at
#         job.last_error = None
#         self.storage.update_job(job)
#         return job
# queuectl/engine.py

import json
from datetime import timedelta, datetime
from typing import Dict, List, Optional, Any

from .models import Job, JobState
from .storage import JobStorage
from .config import Config
from .backoff import compute_backoff
from . import utils


class QueueEngine:
    """
    High-level queue logic: enqueue, status, DLQ, transitions, metrics.
    """

    def __init__(self, storage: JobStorage, config: Config) -> None:
        self.storage = storage
        self.config = config

    def enqueue_job_from_json(self, job_json: str) -> Job:
        """
        Enqueue a job described as JSON.

        Supports optional fields:
        - id: job id (else auto-generated)
        - command: required shell command
        - max_retries: overrides default config
        - run_at: ISO-8601 datetime string when job should first run
        - delay: seconds to wait before first run (if run_at not provided)
        """
        try:
            data = json.loads(job_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid job JSON: {e}") from e

        if "command" not in data or not data["command"]:
            raise ValueError("Job JSON must contain a non-empty 'command' field")

        job_id = data.get("id") or utils.generate_job_id()
        max_retries = int(data.get("max_retries", self.config.max_retries))

        now = utils.utc_now()
        now_iso = now.isoformat(timespec="seconds")

        # Scheduling: run_at or delay (in seconds)
        next_run_iso: str
        run_at_raw = data.get("run_at")
        delay_raw = data.get("delay")

        if run_at_raw is not None:
            # User provided a specific timestamp
            try:
                run_at_dt = datetime.fromisoformat(run_at_raw)
            except Exception as e:  # noqa: BLE001
                raise ValueError(
                    "run_at must be a valid ISO-8601 datetime string, "
                    f"e.g. '2025-11-11T10:30:00'. Got: {run_at_raw}"
                ) from e
            next_run_iso = run_at_dt.isoformat(timespec="seconds")
        elif delay_raw is not None:
            # User provided a delay (seconds) from now
            try:
                delay_seconds = int(delay_raw)
            except Exception as e:  # noqa: BLE001
                raise ValueError(
                    f"delay must be an integer number of seconds, got: {delay_raw}"
                ) from e
            run_at_dt = now + timedelta(seconds=delay_seconds)
            next_run_iso = run_at_dt.isoformat(timespec="seconds")
        else:
            # Default: ready to run immediately
            next_run_iso = now_iso

        job = Job(
            id=job_id,
            command=data["command"],
            state=JobState.PENDING,
            attempts=0,
            max_retries=max_retries,
            created_at=now_iso,
            updated_at=now_iso,
            next_run_at=next_run_iso,
            last_error=None,
            output_log_path=None,
        )

        self.storage.enqueue(job)
        return job

    def acquire_job_for_worker(self) -> Optional[Job]:
        now_iso = utils.utc_now_iso()
        return self.storage.acquire_due_job(now_iso)

    def complete_job(self, job: Job, output_log_path: Optional[str]) -> None:
        job.state = JobState.COMPLETED
        job.updated_at = utils.utc_now_iso()
        job.next_run_at = None
        job.last_error = None
        job.output_log_path = output_log_path
        self.storage.update_job(job)

    def fail_job(self, job: Job, error_message: str) -> None:
        job.attempts += 1
        job.updated_at = utils.utc_now_iso()
        job.last_error = error_message

        if job.attempts >= job.max_retries:
            job.state = JobState.DEAD
            job.next_run_at = None
        else:
            job.state = JobState.FAILED
            base = self.config.backoff_base
            delay_seconds = compute_backoff(base, job.attempts)
            next_time = utils.utc_now() + timedelta(seconds=delay_seconds)
            job.next_run_at = next_time.isoformat(timespec="seconds")

        self.storage.update_job(job)

    def list_jobs(self, state: Optional[JobState] = None) -> List[Job]:
        return self.storage.list_jobs(state)

    def status(self) -> Dict[str, int]:
        counts = self.storage.counts_by_state()
        return {state.value: counts.get(state, 0) for state in JobState}

    def dlq_list(self) -> List[Job]:
        return self.storage.list_jobs(JobState.DEAD)

    def dlq_retry(self, job_id: str) -> Job:
        job = self.storage.get_job(job_id)
        if not job:
            raise ValueError(f"No job found with id '{job_id}'")
        if job.state != JobState.DEAD:
            raise ValueError(f"Job '{job_id}' is not in DLQ (state={job.state.value})")

        job.state = JobState.PENDING
        job.attempts = 0
        job.updated_at = utils.utc_now_iso()
        job.next_run_at = job.updated_at
        job.last_error = None
        self.storage.update_job(job)
        return job

    def metrics(self) -> Dict[str, Any]:
        """
        Compute simple execution metrics from existing job data:
        - total_jobs
        - counts by state
        - success_rate
        - avg_attempts_completed
        - avg_attempts_dead
        - avg_duration_completed_seconds (approx: updated_at - created_at)
        """
        jobs = self.storage.list_jobs()
        total = len(jobs)

        by_state = {
            JobState.PENDING: [],
            JobState.PROCESSING: [],
            JobState.COMPLETED: [],
            JobState.FAILED: [],
            JobState.DEAD: [],
        }
        for j in jobs:
            by_state[j.state].append(j)

        completed = by_state[JobState.COMPLETED]
        dead = by_state[JobState.DEAD]

        def avg_attempts(js):
            return (sum(j.attempts for j in js) / len(js)) if js else 0.0

        avg_attempts_completed = avg_attempts(completed)
        avg_attempts_dead = avg_attempts(dead)

        # Approximate duration = updated_at - created_at for completed jobs
        durations = []
        for j in completed:
            try:
                created = datetime.fromisoformat(j.created_at)
                updated = datetime.fromisoformat(j.updated_at)
                durations.append((updated - created).total_seconds())
            except Exception:
                # If parsing fails for any job, just skip it
                continue

        avg_duration_completed = (
            sum(durations) / len(durations) if durations else 0.0
        )

        success_rate = (len(completed) / total) if total else 0.0

        return {
            "total_jobs": total,
            "by_state": {s.value: len(by_state[s]) for s in by_state},
            "success_rate": success_rate,
            "avg_attempts_completed": avg_attempts_completed,
            "avg_attempts_dead": avg_attempts_dead,
            "avg_duration_completed_seconds": avg_duration_completed,
        }
