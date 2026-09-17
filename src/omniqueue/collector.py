"""Poll every cluster in parallel and keep the latest snapshot in memory."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from .config import ClusterConfig, Config
from .history import HistoryStore
from .models import Job
from .slurm import merge_jobs, parse_sacct, parse_squeue, sacct_command, squeue_command
from .ssh import RemoteError, run_on_cluster

log = logging.getLogger("omniqueue.collector")


@dataclass
class ClusterStatus:
    name: str
    host: str
    ok: bool = False
    error: str | None = None
    warning: str | None = None
    last_attempt: float | None = None
    last_success: float | None = None
    poll_seconds: float | None = None
    color: str | None = None
    counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


class Collector:
    def __init__(self, config: Config, history: HistoryStore):
        self.config = config
        self.history = history
        self._lock = threading.Lock()
        self._jobs: dict[str, list[Job]] = {}
        self._status: dict[str, ClusterStatus] = {
            c.name: ClusterStatus(name=c.name, host=c.host or "local", color=c.color)
            for c in config.enabled_clusters
        }
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_refresh: float | None = None
        self.refreshing = False

    # -- one cluster -----------------------------------------------------------
    def poll_cluster(self, cluster: ClusterConfig) -> tuple[list[Job], ClusterStatus]:
        status = self._status[cluster.name]
        status.last_attempt = time.time()
        t0 = time.monotonic()
        warnings: list[str] = []
        try:
            sq = run_on_cluster(cluster, squeue_command(cluster.user, cluster.squeue_args), self.config.ssh_timeout)
            if sq.returncode != 0:
                raise RemoteError(f"squeue exited {sq.returncode}: {sq.stderr.strip()[:300]}")
            squeue_jobs = parse_squeue(sq.stdout, cluster.name)

            sacct_jobs: list[Job] = []
            if cluster.use_sacct:
                sa = run_on_cluster(
                    cluster,
                    sacct_command(cluster.user, self.config.lookback_hours, cluster.sacct_args),
                    self.config.ssh_timeout,
                )
                if sa.returncode != 0:
                    warnings.append(f"sacct exited {sa.returncode}: {sa.stderr.strip()[:200]}")
                else:
                    sacct_jobs = parse_sacct(sa.stdout, cluster.name)
        except RemoteError as exc:
            status.ok = False
            status.error = str(exc)
            status.poll_seconds = time.monotonic() - t0
            log.warning("%s: %s", cluster.name, exc)
            return self.history.jobs_for(cluster.name), status

        merged = merge_jobs(squeue_jobs, sacct_jobs)
        jobs = self.history.update_cluster(cluster.name, merged)
        status.ok = True
        status.error = None
        status.warning = "; ".join(warnings) or None
        status.last_success = time.time()
        status.poll_seconds = time.monotonic() - t0
        return jobs, status

    # -- all clusters ------------------------------------------------------------
    def refresh(self) -> None:
        clusters = self.config.enabled_clusters
        if not clusters:
            return
        self.refreshing = True
        try:
            with ThreadPoolExecutor(max_workers=min(16, len(clusters))) as pool:
                results = list(pool.map(self.poll_cluster, clusters))
        finally:
            self.refreshing = False
        with self._lock:
            for cluster, (jobs, status) in zip(clusters, results):
                self._jobs[cluster.name] = jobs
                status.counts = _count(jobs)
            self.last_refresh = time.time()
        try:
            self.history.save()
        except OSError as exc:
            log.error("could not save history: %s", exc)

    def request_refresh(self) -> None:
        self._wake.set()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="omniqueue-collector", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh()
            except Exception:  # noqa: BLE001 - keep the poller alive whatever happens
                log.exception("refresh failed")
            self._wake.wait(self.config.refresh_seconds)
            self._wake.clear()

    # -- snapshot for the API -------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            jobs = [j.to_dict() for lst in self._jobs.values() for j in lst]
            clusters = [s.to_dict() for s in self._status.values()]
        for j in jobs:
            j["exit_summary"] = _exit_summary(j)
        return {
            "now": time.time(),
            "last_refresh": self.last_refresh,
            "refreshing": self.refreshing,
            "refresh_seconds": self.config.refresh_seconds,
            "lookback_hours": self.config.lookback_hours,
            "clusters": clusters,
            "jobs": jobs,
        }


def _count(jobs: list[Job]) -> dict[str, int]:
    counts = {"running": 0, "pending": 0, "ok": 0, "problem": 0, "unknown": 0}
    for j in jobs:
        counts[j.category] = counts.get(j.category, 0) + 1
    return counts


def _exit_summary(d: dict[str, Any]) -> str:
    from .slurm import describe_exit

    return describe_exit(Job.from_dict({k: v for k, v in d.items() if k not in ("key", "category", "terminal")}))
