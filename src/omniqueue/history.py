"""Local JSON store of jobs, so finished/crashed jobs remain visible after they
leave the cluster's accounting window."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from pathlib import Path

from .config import secure_dir
from .models import Job


class HistoryStore:
    def __init__(self, path: Path, retention_days: int = 30):
        self.path = Path(path)
        self.retention_days = retention_days
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self._load()

    # -- persistence -------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        for d in data.get("jobs", []):
            try:
                job = Job.from_dict(d)
            except TypeError:
                continue
            job.source = "history"
            self._jobs[job.key] = job

    def save(self) -> None:
        with self._lock:
            secure_dir(self.path.parent)
            payload = {"version": 1, "saved_at": time.time(), "jobs": [j.to_dict() for j in self._jobs.values()]}
            fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".history-", suffix=".json")
            try:
                with os.fdopen(fd, "w") as fh:
                    json.dump(payload, fh)
                os.chmod(tmp, 0o600)
                os.replace(tmp, self.path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise

    # -- updating ------------------------------------------------------------
    def update_cluster(self, cluster: str, jobs: list[Job], now: float | None = None) -> list[Job]:
        """Merge a fresh poll of one cluster into the store and return the
        combined view for that cluster (fresh jobs plus remembered finished ones).

        A job that was active last time we looked and has now vanished from
        both squeue and sacct is marked ``VANISHED`` so it is not silently lost.
        """
        now = now or time.time()
        fresh = {j.key: j for j in jobs}
        with self._lock:
            for key, old in list(self._jobs.items()):
                if old.cluster != cluster or key in fresh:
                    continue
                if not old.is_terminal and old.state != "VANISHED":
                    old.state = "VANISHED"
                    old.reason = old.reason or "disappeared from squeue/sacct (cancelled or purged?)"
                    old.end_time = old.end_time or time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now))
            self._jobs.update(fresh)
            self._prune(now)
            return [j for j in self._jobs.values() if j.cluster == cluster]

    def _prune(self, now: float) -> None:
        cutoff = now - self.retention_days * 86400
        for key, job in list(self._jobs.items()):
            if job.last_seen and job.last_seen < cutoff:
                del self._jobs[key]

    def jobs_for(self, cluster: str) -> list[Job]:
        with self._lock:
            return [j for j in self._jobs.values() if j.cluster == cluster]

    def all_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def forget(self, key: str) -> bool:
        with self._lock:
            return self._jobs.pop(key, None) is not None
