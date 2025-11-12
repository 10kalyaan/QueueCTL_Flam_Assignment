# queuectl

queuectl is a simple CLI-based background job queue built in Python.
It supports multiple workers, retries with exponential backoff, a Dead Letter Queue (DLQ), persistent storage, timeouts, logging, and basic metrics.

# Features:
CLI tool for managing background jobs:
    enqueue – add jobs
    worker start/stop – run worker processes
    status – job + worker summary
    list – list jobs by state
    dlq – view & retry dead jobs
    config – manage settings
    metrics – basic execution stats

Job persistence using SQLite (data/jobs.db)
Multiple workers using multiprocessing
Automatic retries with exponential backoff
Dead Letter Queue (DLQ) via state=dead

Configurable:
    max_retries
    backoff_base
    job_timeout (seconds)

Bonus Features Implemented
From the optional list, this project includes:
Job timeout handling
    Jobs are executed with subprocess.run(..., timeout=job_timeout). On timeout, the job is marked failed and the error is stored.
Job output logging
    Each job’s stdout/stderr is written to data/logs/job-<id>.log, and the path is stored on the job.
Scheduled / delayed jobs
    --delay <seconds>: run the job after a delay

    --run-at <ISO timestamp>: run at a specific time
    Jobs are only picked up when next_run_at <= now.
Metrics / execution stats:
metrics prints:
    total jobs
    counts by state
    success rate
    average attempts (completed/dead)
    average duration of completed jobs

# Usage:
#config
python -m queuectl.cli config set max_retries 3
python -m queuectl.cli config set backoff_base 2
python -m queuectl.cli config set job_timeout 10
#Simple immediate job
python -m queuectl.cli enqueue --cmd "echo Hello World" --id job1
#Failing job with custom retries
python -m queuectl.cli enqueue --cmd "this_command_does_not_exist_999" --id fail1 --max-retries 2
#Delayed job (run after 30 seconds)
python -m queuectl.cli enqueue --cmd "echo Delayed" --id delayed1 --delay 30
#Scheduled job (run at a specific time)
python -m queuectl.cli enqueue --cmd "echo Scheduled" --id sched1 --run-at "2025-11-11T18:00:00"
#JSON-style
$json = '{"command": "echo From JSON", "max_retries": 3}'
python -m queuectl.cli enqueue $json
#Start workers (Ctrl+C to stop)
python -m queuectl.cli worker start --count 2
#Ask workers to stop gracefully
python -m queuectl.cli worker stop
#Overall status
python -m queuectl.cli status
#List jobs by state
python -m queuectl.cli list --state pending
python -m queuectl.cli list --state completed
python -m queuectl.cli list --state failed
python -m queuectl.cli list --state dead
#View job logs (example)
type .\data\logs\job-job1.log
#List dead jobs
python -m queuectl.cli dlq list
#Retry a dead job
python -m queuectl.cli dlq retry fail1
#Metrics
python -m queuectl.cli metrics

# Testing
All tests are plain Python scripts (no extra test framework needed):
#basic enqueue + run + complete flow
python tests/test_basic.py           
#retries, backoff, DLQ behavior
python tests/test_retry_backoff.py   
#jobs persist across "restart"
python tests/test_persistence.py  

Video Demo Link:
[text](https://drive.google.com/drive/folders/1EkitaHKVa0kJA9h4p9FKgX0iO9XjT37R)






