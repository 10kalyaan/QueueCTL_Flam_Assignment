

import sqlite3
from typing import List, Optional, Dict
from datetime import datetime

from .models import Job, JobState
from . import utils


class JobStorage:
    

    def __init__(self, db_path: Optional[str] = None) -> None:
        utils.ensure_data_dirs()
        self.db_path = db_path or utils.get_data_file("jobs.db")
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._get_connection()
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    next_run_at TEXT,
                    last_error TEXT,
                    output_log_path TEXT
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_state_next_run "
                "ON jobs (state, next_run_at);"
            )
        finally:
            conn.close()

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            command=row["command"],
            state=JobState(row["state"]),
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            next_run_at=row["next_run_at"],
            last_error=row["last_error"],
            output_log_path=row["output_log_path"],
        )

    def enqueue(self, job: Job) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, command, state, attempts, max_retries,
                    created_at, updated_at, next_run_at, last_error, output_log_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id,
                    job.command,
                    job.state.value,
                    job.attempts,
                    job.max_retries,
                    job.created_at,
                    job.updated_at,
                    job.next_run_at,
                    job.last_error,
                    job.output_log_path,
                ),
            )
        finally:
            conn.close()

    def get_job(self, job_id: str) -> Optional[Job]:
        conn = self._get_connection()
        try:
            cur = conn.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
            row = cur.fetchone()
            return self._row_to_job(row) if row else None
        finally:
            conn.close()

    def update_job(self, job: Job) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE jobs
                SET command = ?, state = ?, attempts = ?, max_retries = ?,
                    created_at = ?, updated_at = ?, next_run_at = ?,
                    last_error = ?, output_log_path = ?
                WHERE id = ?
                """,
                (
                    job.command,
                    job.state.value,
                    job.attempts,
                    job.max_retries,
                    job.created_at,
                    job.updated_at,
                    job.next_run_at,
                    job.last_error,
                    job.output_log_path,
                    job.id,
                ),
            )
        finally:
            conn.close()

    def list_jobs(self, state: Optional[JobState] = None) -> List[Job]:
        conn = self._get_connection()
        try:
            if state is None:
                cur = conn.execute("SELECT * FROM jobs ORDER BY created_at;")
            else:
                cur = conn.execute(
                    "SELECT * FROM jobs WHERE state = ? ORDER BY created_at;",
                    (state.value,),
                )
            return [self._row_to_job(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def counts_by_state(self) -> Dict[JobState, int]:
        conn = self._get_connection()
        try:
            cur = conn.execute("SELECT state, COUNT(*) AS cnt FROM jobs GROUP BY state;")
            result: Dict[JobState, int] = {s: 0 for s in JobState}
            for row in cur.fetchall():
                state = JobState(row["state"])
                result[state] = row["cnt"]
            return result
        finally:
            conn.close()

    def acquire_due_job(self, now_iso: str) -> Optional[Job]:
        
        conn = self._get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE;")
            cur = conn.execute(
                """
                SELECT * FROM jobs
                WHERE state IN (?, ?)
                  AND (next_run_at IS NULL OR next_run_at <= ?)
                ORDER BY created_at
                LIMIT 1
                """,
                (JobState.PENDING.value, JobState.FAILED.value, now_iso),
            )
            row = cur.fetchone()
            if not row:
                conn.execute("COMMIT;")
                return None

            job = self._row_to_job(row)
            job.state = JobState.PROCESSING
            job.updated_at = now_iso
            conn.execute(
                """
                UPDATE jobs
                SET state = ?, updated_at = ?
                WHERE id = ?
                """,
                (job.state.value, job.updated_at, job.id),
            )
            conn.execute("COMMIT;")
            return job
        except Exception:
            conn.execute("ROLLBACK;")
            raise
        finally:
            conn.close()
