# tests/test_persistence.py
"""
Persistence test:
- Enqueue a job using one engine/storage
- Recreate storage+engine ("restart")
- Verify job is still present and pending
"""

import os

from queuectl.config import Config
from queuectl.storage import JobStorage
from queuectl.engine import QueueEngine
from queuectl.models import JobState
from queuectl import utils


def main() -> None:
    print("=== test_persistence: jobs persist across restart ===")

    db_path = utils.get_data_file("test_persistence_jobs.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    config = Config()

    # First "run"
    storage1 = JobStorage(db_path=db_path)
    engine1 = QueueEngine(storage1, config)

    job = engine1.enqueue_job_from_json(
        '{"command": "echo persistence_test", "max_retries": 1}'
    )
    job_id = job.id
    print(f"Enqueued job: {job_id}")

    # "Restart" – new storage/engine using same DB
    storage2 = JobStorage(db_path=db_path)
    engine2 = QueueEngine(storage2, config)

    jobs = engine2.list_jobs()
    ids = [j.id for j in jobs]
    assert job_id in ids, "Job not found after restart"

    j = [j for j in jobs if j.id == job_id][0]
    assert j.state == JobState.PENDING, f"Expected pending, got {j.state}"

    print("OK: job present with correct state after restart.")
    print("test_persistence PASSED ✅")


if __name__ == "__main__":
    main()
