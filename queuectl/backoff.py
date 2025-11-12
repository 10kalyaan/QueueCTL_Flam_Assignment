# queuectl/backoff.py

def compute_backoff(base: int, attempts: int) -> int:
    """
    Exponential backoff: base ** attempts seconds.
    attempts = number of failed attempts so far.
    """
    if attempts <= 0:
        return 0
    return base ** attempts
