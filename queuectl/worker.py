# queuectl/worker.py

import time
import signal
import multiprocessing
import traceback

from .config import Config
from .storage import JobStorage
from .engine import QueueEngine
from .executor import run_job
from . import utils


_STOP_REQUESTED = False


def _signal_handler(signum, frame):
    # Just mark a flag; actual loop exit is graceful
    global _STOP_REQUESTED
    _STOP_REQUESTED = True


def worker_main(worker_index: int) -> None:
    """
    Main loop for a single worker process.
    """
    global _STOP_REQUESTED
    _STOP_REQUESTED = False

    pid = multiprocessing.current_process().pid
    utils.register_worker_pid(pid)

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    try:
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        # Not available on some platforms (e.g. Windows for certain signals)
        pass

    config = Config()
    storage = JobStorage()
    engine = QueueEngine(storage, config)

    try:
        while True:
            if _STOP_REQUESTED or utils.has_stop_flag():
                # Finish current iteration and exit
                break

            job = engine.acquire_job_for_worker()
            if not job:
                # No job ready; small sleep
                time.sleep(1.0)
                continue

            try:
                result = run_job(job, timeout=config.job_timeout)
                if result.exit_code == 0:
                    engine.complete_job(job, result.output_log_path)
                else:
                    engine.fail_job(job, result.error_message or "Unknown error")
            except Exception as e:  # noqa: BLE001
                # Any unexpected failure should still mark the job as failed
                tb = traceback.format_exc()
                msg = f"Worker exception: {e}\n{tb}"
                engine.fail_job(job, msg)
    finally:
        utils.unregister_worker_pid(pid)


def start_workers(count: int) -> None:
    """
    Start 'count' worker processes and keep the command running.
    Ctrl+C will signal all workers to stop gracefully.
    """
    utils.ensure_data_dirs()
    procs = []

    print(f"Starting {count} worker(s)...")

    for i in range(count):
        p = multiprocessing.Process(target=worker_main, args=(i + 1,))
        p.start()
        procs.append(p)

    print("Workers are running. Press Ctrl+C to stop them.")
    try:
        while any(p.is_alive() for p in procs):
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Received Ctrl+C, asking workers to stop...")
        utils.create_stop_flag()
        # Wait for workers to finish
        for p in procs:
            p.join(timeout=30.0)
        utils.clear_stop_flag()
    else:
        # Normal exit (e.g. all workers exit because stop flag was set)
        for p in procs:
            p.join(timeout=1.0)


def stop_workers_command() -> None:
    """
    Command-style function: mark stop flag and wait until workers exit.
    """
    utils.ensure_data_dirs()
    pid_files_before = utils.list_worker_pid_files()
    if not pid_files_before:
        print("No active workers found.")
        return

    print("Signaling workers to stop gracefully...")
    utils.create_stop_flag()

    # Wait for workers to remove their PID files
    for _ in range(30):
        time.sleep(1.0)
        if not utils.list_worker_pid_files():
            break

    if utils.list_worker_pid_files():
        print("Workers are still shutting down. They will exit shortly.")
    else:
        print("All workers stopped.")

    utils.clear_stop_flag()
