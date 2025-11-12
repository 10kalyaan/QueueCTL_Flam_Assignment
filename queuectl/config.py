
import json
from typing import Any, Dict
from . import utils


class Config:
    

    DEFAULTS: Dict[str, Any] = {
        "max_retries": 3,
        "backoff_base": 2,
        "job_timeout": 60, 
    }

    def __init__(self) -> None:
        utils.ensure_data_dirs()
        self._path = utils.get_data_file("config.json")
        self._data: Dict[str, Any] = dict(self.DEFAULTS)
        self._load()

    def _load(self) -> None:
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                stored = json.load(f)
            self._data.update(stored)
        except FileNotFoundError:
            self._save()

    def _save(self) -> None:
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value
        self._save()

    @property
    def max_retries(self) -> int:
        return int(self.get("max_retries", self.DEFAULTS["max_retries"]))

    @property
    def backoff_base(self) -> int:
        return int(self.get("backoff_base", self.DEFAULTS["backoff_base"]))

    @property
    def job_timeout(self) -> int:
        return int(self.get("job_timeout", self.DEFAULTS["job_timeout"]))
