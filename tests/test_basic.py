# tests/test_basic.py
"""
Basic smoke test:
- Enqueue a simple echo job
- Acquire it as a worker would
- Run the command
- Mark job as completed
- Verify it's in 'completed' state
"""

import os

from queuectl.config import Config
from queuectl.storage import JobStorage
from queuectl.engine import QueueEngine
from queuectl.executor import run_job
from queuectl import utils
from queuectl.models import JobState


def main() -> None:
    print("=== test_basic: enqueue + run + complete ===")

    # Use a separate DB for this test so we don't touch real data
    db_path = utils.get_data_file("test_basic_jobs.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    config = Config()
    storage = JobStorage(db_path=db_path)
    engine = QueueEngine(storage, config)

    # 1) Enqueue job
    job = engine.enqueue_job_from_json(
        '{"command": "echo test_basic", "max_retries": 2}'
    )
    print(f"Enqueued job: {job.id}")

    # 2) Acquire job like a worker
    job_for_worker = engine.acquire_job_for_worker()
    assert job_for_worker is not None, "No job acquired"
    assert job_for_worker.id == job.id

    # 3) Run command
    result = run_job(job_for_worker, timeout=config.job_timeout)
    assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"

    # 4) Mark as completed
    engine.complete_job(job_for_worker, result.output_log_path)

    # 5) Verify in storage
    jobs = engine.list_jobs()
    completed = [j for j in jobs if j.state == JobState.COMPLETED]
    assert len(completed) == 1, f"Expected 1 completed job, found {len(completed)}"

    print("OK: job completed successfully.")
    print("test_basic PASSED ✅")


if __name__ == "__main__":
    main()
