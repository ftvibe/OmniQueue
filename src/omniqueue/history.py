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
        self.meta: dict = {"gpu_partitions": {}, "covered": {}}  # covered: cluster -> how far back sacct has been asked
        self._stale: set[str] = set()  # job keys written before GPUs were tracked
        self._load()

    # -- persistence -------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        if isinstance(data.get("meta"), dict):
            self.meta.update(data["meta"])
            self.meta.setdefault("gpu_partitions", {})
            self.meta.setdefault("covered", {})
        stale_clusters: set[str] = set()
        for d in data.get("jobs", []):
            try:
                job = Job.from_dict(d)
            except TypeError:
                continue
            job.source = "history"
            self._jobs[job.key] = job
            if "gpus" not in d:  # written before GPUs were tracked
                self._stale.add(job.key)
                stale_clusters.add(job.cluster)
        for cluster in stale_clusters:  # the back-fill runs once more for these and replaces the stale records
            self.meta["covered"].pop(cluster, None)

    def save(self) -> None:
        with self._lock:
            secure_dir(self.path.parent)
            payload = {"version": 1, "saved_at": time.time(), "meta": self.meta, "jobs": [j.to_dict() for j in self._jobs.values()]}
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

    # -- older history, fetched in chunks after the regular poll ------------------------
    def covered_since(self, cluster: str) -> float | None:
        """Unix time back to which your own accounting has been fetched; None before the first poll."""
        with self._lock:
            return self.meta.setdefault("covered", {}).get(cluster)

    def set_covered_since(self, cluster: str, ts: float) -> None:
        with self._lock:
            cur = self.meta.setdefault("covered", {}).get(cluster)
            self.meta["covered"][cluster] = ts if cur is None else min(cur, ts)

    def add_older(self, cluster: str, jobs: list[Job]) -> int:
        """Merge a back-fill chunk: new jobs are added, records written before GPUs were
        tracked are replaced, anything fresher is kept.  Returns how many changed."""
        changed = 0
        with self._lock:
            for j in jobs:
                if j.key not in self._jobs or j.key in self._stale:
                    j.source = "sacct"
                    self._jobs[j.key] = j
                    self._stale.discard(j.key)
                    changed += 1
        return changed

    def set_partition_gres(self, cluster: str, gres: dict[str, int], now: float | None = None) -> None:
        """Remember which partitions of a cluster have GPUs (and how many per node)."""
        with self._lock:
            self.meta.setdefault("gpu_partitions", {})[cluster] = {"checked": now or time.time(), "gpus_per_node": dict(gres)}

    def partition_gres(self, cluster: str) -> dict[str, int]:
        with self._lock:
            return dict(self.meta.get("gpu_partitions", {}).get(cluster, {}).get("gpus_per_node", {}))

    def partition_gres_age(self, cluster: str, now: float | None = None) -> float | None:
        """Seconds since the partition gres was last looked up, None if never."""
        with self._lock:
            checked = self.meta.get("gpu_partitions", {}).get(cluster, {}).get("checked")
        return None if checked is None else (now or time.time()) - checked

    def forget(self, key: str) -> bool:
        with self._lock:
            return self._jobs.pop(key, None) is not None
