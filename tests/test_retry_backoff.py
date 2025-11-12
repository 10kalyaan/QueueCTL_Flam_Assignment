# tests/test_retry_backoff.py
"""
Retry & backoff behavior:
- Enqueue a job with max_retries=2
- Simulate two failures
- After first failure: state=failed, next_run_at in the future
- After second failure: state=dead, next_run_at=None
"""

import os
from datetime import datetime

from queuectl.config import Config
from queuectl.storage import JobStorage
from queuectl.engine import QueueEngine
from queuectl.models import JobState
from queuectl import utils


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def main() -> None:
    print("=== test_retry_backoff: failed job -> backoff -> DLQ ===")

    db_path = utils.get_data_file("test_retry_jobs.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    config = Config()
    # Make sure config has small values
    config.set("max_retries", 2)
    config.set("backoff_base", 2)

    storage = JobStorage(db_path=db_path)
    engine = QueueEngine(storage, config)

    # 1) Enqueue job with invalid command
    job = engine.enqueue_job_from_json(
        '{"command": "this_command_should_not_exist_123", "max_retries": 2}'
    )
    job_id = job.id
    print(f"Enqueued job: {job_id}")

    # Simulate worker acquiring it
    job = engine.acquire_job_for_worker()
    assert job is not None, "No job acquired"
    assert job.id == job_id

    # 2) First failure
    engine.fail_job(job, "simulated failure 1")
    job1 = storage.get_job(job_id)
    assert job1 is not None
    assert job1.state == JobState.FAILED, f"Expected state failed, got {job1.state}"
    assert job1.next_run_at is not None, "next_run_at should be set after first failure"

    t_updated = _parse_iso(job1.updated_at)
    t_next = _parse_iso(job1.next_run_at)
    assert t_next > t_updated, "next_run_at must be in the future"

    # 3) Second failure → should move to DEAD (DLQ)
    engine.fail_job(job1, "simulated failure 2")
    job2 = storage.get_job(job_id)
    assert job2 is not None
    assert job2.state == JobState.DEAD, f"Expected state dead, got {job2.state}"
    assert job2.next_run_at is None, "next_run_at should be None for dead jobs"

    print("OK: retries increment and DLQ behavior verified.")
    print("test_retry_backoff PASSED ✅")


if __name__ == "__main__":
    main()
