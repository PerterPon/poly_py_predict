from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Optional


class JsonStore:
    """Tiny atomic JSON store for product runtime state.

    Keeps the repo convention of storing mutable runtime state in ./logs/*.json.
    """

    _locks: dict[str, threading.Lock] = {}
    _global = threading.Lock()

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _lock(self) -> threading.Lock:
        key = str(self.path.resolve())
        with self._global:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    def load(self, default: Optional[Any] = None) -> Any:
        if not self.path.exists():
            return default
        try:
            return json.loads(self.path.read_text(encoding='utf-8'))
        except Exception:
            return default

    def save(self, data: Any) -> None:
        lock = self._lock()
        with lock:
            tmp = self.path.with_suffix(self.path.suffix + '.tmp')
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding='utf-8')
            os.replace(tmp, self.path)
