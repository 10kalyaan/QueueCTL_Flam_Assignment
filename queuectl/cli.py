import argparse
import sys
import json
from typing import Optional

from .config import Config
from .storage import JobStorage
from .engine import QueueEngine
from .models import JobState
from .worker import start_workers, stop_workers_command
from . import utils


def _print_job_row(job) -> None:
    print(
        f"[{job.id}] state={job.state.value} "
        f"cmd='{job.command}' attempts={job.attempts}/{job.max_retries} "
        f"created_at={job.created_at} updated_at={job.updated_at}"
    )
    if job.last_error:
        print(f"  last_error: {job.last_error}")
    if job.output_log_path:
        print(f"  log: {job.output_log_path}")


def main(argv: Optional[list[str]] = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(prog="queuectl", description="Job queue CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_enqueue = subparsers.add_parser("enqueue", help="Enqueue a new job")
    p_enqueue.add_argument(
        "job_json",
        nargs="?",
        help="Job specification as JSON string (optional if using --cmd/--id)",
    )
    p_enqueue.add_argument(
        "--cmd",
        dest="job_command",
        help="Shell command for the job (alternative to JSON)",
    )
    p_enqueue.add_argument(
        "--id",
        dest="job_id",
        help="Job id (alternative to providing it in JSON)",
    )
    p_enqueue.add_argument(
        "--max-retries",
        dest="max_retries",
        type=int,
        help="Override max_retries for this job (alternative to JSON field)",
    )
    p_enqueue.add_argument(
        "--run-at",
        dest="run_at",
        help="ISO-8601 timestamp when job should first run (e.g. 2025-11-11T10:30:00)",
    )
    p_enqueue.add_argument(
        "--delay",
        dest="delay",
        type=int,
        help="Delay in seconds before the job becomes eligible to run "
             "(ignored if --run-at is provided)",
    )

    p_worker = subparsers.add_parser("worker", help="Worker management")
    worker_sub = p_worker.add_subparsers(dest="worker_command", required=True)

    p_worker_start = worker_sub.add_parser("start", help="Start worker processes")
    p_worker_start.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of workers to start (default: 1)",
    )

    worker_sub.add_parser("stop", help="Stop running workers gracefully")

    subparsers.add_parser("status", help="Show job and worker status")

    p_list = subparsers.add_parser("list", help="List jobs")
    p_list.add_argument(
        "--state",
        choices=[s.value for s in JobState],
        default=None,
        help="Filter by job state",
    )

    p_dlq = subparsers.add_parser("dlq", help="Dead Letter Queue operations")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_command", required=True)
    dlq_sub.add_parser("list", help="List DLQ jobs")
    p_dlq_retry = dlq_sub.add_parser("retry", help="Retry a DLQ job")
    p_dlq_retry.add_argument("job_id", help="Job ID to retry")

    p_config = subparsers.add_parser("config", help="Configuration management")
    config_sub = p_config.add_subparsers(dest="config_command", required=True)

    p_config_get = config_sub.add_parser("get", help="Get a config value")
    p_config_get.add_argument("key", help="Config key")

    p_config_set = config_sub.add_parser("set", help="Set a config value")
    p_config_set.add_argument("key", help="Config key")
    p_config_set.add_argument("value", help="Config value")

    subparsers.add_parser("metrics", help="Show aggregated execution metrics")

    args = parser.parse_args(argv)

    if args.command == "worker":
        if args.worker_command == "start":
            utils.ensure_data_dirs()
            count = max(1, int(args.count))
            start_workers(count)
            return
        elif args.worker_command == "stop":
            stop_workers_command()
            return

    utils.ensure_data_dirs()
    config = Config()
    storage = JobStorage()
    engine = QueueEngine(storage, config)

    if args.command == "enqueue":
        try:
            if args.job_json:
                job = engine.enqueue_job_from_json(args.job_json)
            else:
                if not args.job_command:
                    raise ValueError(
                        "You must provide either a JSON payload or --cmd for the job command"
                    )

                if args.run_at and args.delay is not None:
                    raise ValueError("You cannot specify both --run-at and --delay")

                payload = {"command": args.job_command}
                if args.job_id:
                    payload["id"] = args.job_id
                if args.max_retries is not None:
                    payload["max_retries"] = args.max_retries
                if args.run_at:
                    payload["run_at"] = args.run_at
                elif args.delay is not None:
                    payload["delay"] = args.delay

                job_json = json.dumps(payload)
                job = engine.enqueue_job_from_json(job_json)
        except Exception as e:  # noqa: BLE001
            print(f"Error enqueuing job: {e}", file=sys.stderr)
            sys.exit(1)

        print(f"Enqueued job {job.id}")

    elif args.command == "status":
        status = engine.status()
        print("Job counts by state:")
        for state, count in status.items():
            print(f"  {state}: {count}")
        pid_files = utils.list_worker_pid_files()
        print(f"Active workers (approx): {len(pid_files)}")

    elif args.command == "list":
        state = JobState(args.state) if args.state else None
        jobs = engine.list_jobs(state)
        if not jobs:
            print("No jobs found.")
        else:
            for job in jobs:
                _print_job_row(job)

    elif args.command == "dlq":
        if args.dlq_command == "list":
            jobs = engine.dlq_list()
            if not jobs:
                print("DLQ is empty.")
            else:
                for job in jobs:
                    _print_job_row(job)
        elif args.dlq_command == "retry":
            try:
                job = engine.dlq_retry(args.job_id)
            except Exception as e:  # noqa: BLE001
                print(f"Error retrying job: {e}", file=sys.stderr)
                sys.exit(1)
            print(f"Moved job {job.id} from DLQ back to pending.")

    elif args.command == "config":
        if args.config_command == "get":
            value = config.get(args.key)
            print(f"{args.key} = {value}")
        elif args.config_command == "set":
           
            v: str | int
            if args.value.isdigit():
                v = int(args.value)
            else:
                v = args.value
            config.set(args.key, v)
            print(f"Set {args.key} = {v}")

    elif args.command == "metrics":
        m = engine.metrics()
        print("Queue metrics:")
        print(f"  total_jobs: {m['total_jobs']}")
        print("  jobs_by_state:")
        for state, count in m["by_state"].items():
            print(f"    {state}: {count}")
        print(f"  success_rate: {m['success_rate']:.2f}")
        print(f"  avg_attempts_completed: {m['avg_attempts_completed']:.2f}")
        print(f"  avg_attempts_dead: {m['avg_attempts_dead']:.2f}")
        print(
            "  avg_duration_completed_seconds: "
            f"{m['avg_duration_completed_seconds']:.2f}"
        )


if __name__ == "__main__":
    main()
