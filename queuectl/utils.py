# queuectl/utils.py

import os
from pathlib import Path
from datetime import datetime
import uuid
from typing import List

# Project root = parent of this file's directory
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = DATA_DIR / "logs"
STOP_FLAG_PATH = DATA_DIR / "worker.stop"


def ensure_data_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def get_data_file(name: str) -> str:
    ensure_data_dirs()
    return str(DATA_DIR / name)


def get_logs_dir() -> str:
    ensure_data_dirs()
    return str(LOGS_DIR)


def utc_now() -> datetime:
    # Naive UTC; safe for lexicographic comparison as ISO strings
    return datetime.utcnow()


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds")


def generate_job_id() -> str:
    return str(uuid.uuid4())


def has_stop_flag() -> bool:
    return STOP_FLAG_PATH.exists()


def create_stop_flag() -> None:
    ensure_data_dirs()
    STOP_FLAG_PATH.write_text("stop", encoding="utf-8")


def clear_stop_flag() -> None:
    if STOP_FLAG_PATH.exists():
        STOP_FLAG_PATH.unlink()


def worker_pid_file(pid: int) -> Path:
    return DATA_DIR / f"worker_{pid}.pid"


def register_worker_pid(pid: int) -> None:
    ensure_data_dirs()
    pf = worker_pid_file(pid)
    pf.write_text(str(pid), encoding="utf-8")


def unregister_worker_pid(pid: int) -> None:
    pf = worker_pid_file(pid)
    if pf.exists():
        pf.unlink()


def list_worker_pid_files() -> List[Path]:
    ensure_data_dirs()
    return list(DATA_DIR.glob("worker_*.pid"))
