# queuectl/backoff.py

def compute_backoff(base: int, attempts: int) -> int:
    if attempts <= 0:
        return 0
    return base ** attempts
