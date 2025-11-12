# queuectl/executor.py

import subprocess
from typing import NamedTuple, Optional
from pathlib import Path

from .models import Job
from . import utils


class ExecutionResult(NamedTuple):
    exit_code: int
    output_log_path: Optional[str]
    error_message: Optional[str]


def run_job(job: Job, timeout: int) -> ExecutionResult:
    """
    Run the job's shell command with a timeout.
    Write stdout+stderr to a log file.
    """
    logs_dir = Path(utils.get_logs_dir())
    log_path = logs_dir / f"job-{job.id}.log"

    try:
        completed = subprocess.run(
            job.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = ""
        if completed.stdout:
            output += completed.stdout
        if completed.stderr:
            if output:
                output += "\n"
            output += completed.stderr

        log_path.write_text(output, encoding="utf-8")

        exit_code = completed.returncode
        error_message = None if exit_code == 0 else (completed.stderr or "Non-zero exit code")

        return ExecutionResult(
            exit_code=exit_code,
            output_log_path=str(log_path),
            error_message=error_message,
        )
    except subprocess.TimeoutExpired as e:
        # Kill process & treat as failure
        msg = f"Command timed out after {timeout} seconds"
        log_path.write_text(msg, encoding="utf-8")
        return ExecutionResult(
            exit_code=-1,
            output_log_path=str(log_path),
            error_message=msg,
        )
