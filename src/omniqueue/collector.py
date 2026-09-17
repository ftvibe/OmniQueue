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
from .slurm import combined_command, merge_jobs, parse_load, parse_sacct, parse_squeue, split_combined_output, summarize_load
from .ssh import RemoteError, close_connection, connection_alive, run_on_cluster

log = logging.getLogger("omniqueue.collector")


@dataclass
class ClusterStatus:
    name: str
    host: str
    ok: bool = False
    error: str | None = None
    error_kind: str | None = None  # network | auth | timeout | other
    failures: int = 0  # consecutive failed polls
    warning: str | None = None
    last_attempt: float | None = None
    last_success: float | None = None
    poll_seconds: float | None = None
    color: str | None = None
    logo: str | None = None  # URL the dashboard can load
    counts: dict[str, int] = field(default_factory=dict)
    partitions: list[dict] = field(default_factory=list)  # cluster load per partition
    load: dict | None = None  # whole-cluster summary

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


class Collector:
    def __init__(self, config: Config, history: HistoryStore):
        self.config = config
        self.history = history
        self._lock = threading.Lock()
        self._jobs: dict[str, list[Job]] = {}
        self._status: dict[str, ClusterStatus] = {
            c.name: ClusterStatus(name=c.name, host=c.host or "local", color=c.color, logo=self._logo_url(c))
            for c in config.enabled_clusters
        }
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_refresh: float | None = None
        self.next_refresh: float | None = None
        self.refreshing = False
        self._conn_cache: dict[str, tuple[float, bool]] = {}
        self.conn_cache_seconds = 10.0

    def _needs_login(self, cluster: ClusterConfig) -> bool:
        if cluster.is_local or not self.config.persist_connections or self.config.connect_on_poll:
            return False
        self._conn_cache.pop(cluster.name, None)  # decide on a fresh check, not a cached one
        return not self.connected(cluster)

    def connected(self, cluster: ClusterConfig) -> bool | None:
        """Whether a persistent ssh master for this cluster is open right now.

        None when the question does not apply (local cluster or persistence off).
        Cached briefly so browser polls do not spawn ssh every few seconds.
        """
        if cluster.is_local or not self.config.persist_connections:
            return None
        now = time.time()
        cached = self._conn_cache.get(cluster.name)
        if cached and now - cached[0] < self.conn_cache_seconds:
            return cached[1]
        alive = connection_alive(cluster, self.config)
        self._conn_cache[cluster.name] = (now, alive)
        return alive

    def _logo_url(self, cluster: ClusterConfig) -> str | None:
        src = self.config.logo_source(cluster)
        if src is None:
            return None
        if src.startswith(("http://", "https://")):
            return src
        return f"/logo/{cluster.name}"

    def logo_path(self, name: str) -> str | None:
        """Local file backing ``/logo/<name>``, or None."""
        for c in self.config.enabled_clusters:
            if c.name == name:
                src = self.config.logo_source(c)
                return None if src is None or src.startswith(("http://", "https://")) else src
        return None

    # -- one cluster -----------------------------------------------------------
    def poll_cluster(self, cluster: ClusterConfig) -> tuple[list[Job], ClusterStatus]:
        status = self._status[cluster.name]
        if self._needs_login(cluster):
            # login -> monitor -> logout: polls only ride on a connection `omniqueue login` opened
            status.ok = False
            status.error = "not logged in"
            status.error_kind = "login"
            status.failures = 0  # nothing changes until the user acts: no retry backoff
            return self.history.jobs_for(cluster.name), status
        status.last_attempt = time.time()
        t0 = time.monotonic()
        warnings: list[str] = []
        try:
            cmd = combined_command(cluster.user, self.config.lookback_hours, cluster.squeue_args,
                                   cluster.sacct_args, cluster.use_sacct, load=cluster.show_load)
            res = run_on_cluster(cluster, cmd, self.config.ssh_timeout, self.config)
            sections = split_combined_output(res.stdout)
            stderr = res.stderr.strip()
            if "squeue" not in sections:
                raise RemoteError(f"no squeue output (exit {res.returncode}): {stderr[:300] or 'empty reply'}")
            sq_out, sq_rc = sections["squeue"]
            if sq_rc != 0:
                raise RemoteError(f"squeue exited {sq_rc}: {stderr[:300]}")
            squeue_jobs = parse_squeue(sq_out, cluster.name)

            sacct_jobs: list[Job] = []
            if cluster.use_sacct:
                sa_out, sa_rc = sections.get("sacct", ("", -1))
                if sa_rc != 0:
                    warnings.append(f"sacct exited {sa_rc}: {stderr[:200]}")
                else:
                    sacct_jobs = parse_sacct(sa_out, cluster.name)
            elif stderr:
                warnings.append(stderr[:200])

            if cluster.show_load:
                si_out, si_rc = sections.get("sinfo", ("", -1))
                sq_all_out, sq_all_rc = sections.get("squeue_all", ("", -1))
                if si_rc != 0:
                    warnings.append(f"sinfo exited {si_rc}: no load view")
                else:
                    status.partitions = parse_load(si_out, sq_all_out if sq_all_rc == 0 else "")
                    status.load = summarize_load(status.partitions)
        except RemoteError as exc:
            status.ok = False
            status.error = str(exc)
            status.error_kind = exc.kind
            status.failures += 1
            status.poll_seconds = time.monotonic() - t0
            log.warning("%s: %s (%s)", cluster.name, exc, exc.kind)
            if exc.kind in ("timeout", "network"):
                # the master connection may be hung on a dead link (laptop changed
                # network or woke from sleep): drop it so the next poll reconnects
                close_connection(cluster, self.config)
            return self.history.jobs_for(cluster.name), status

        merged = merge_jobs(squeue_jobs, sacct_jobs)
        jobs = self.history.update_cluster(cluster.name, merged)
        status.ok = True
        status.error = None
        status.error_kind = None
        status.failures = 0
        if not cluster.is_local and self.config.persist_connections:
            self._conn_cache[cluster.name] = (time.time(), True)
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

    def next_delay(self) -> float:
        """Seconds until the next poll: the normal interval, or a quick retry
        (retry_seconds doubling per consecutive failure) while any cluster is failing."""
        failing = [s.failures for s in self._status.values() if not s.ok and s.failures]
        if not failing:
            return float(self.config.refresh_seconds)
        backoff = self.config.retry_seconds * 2 ** (min(failing) - 1)
        return float(min(self.config.refresh_seconds, backoff))

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh()
            except Exception:  # noqa: BLE001 - keep the poller alive whatever happens
                log.exception("refresh failed")
            delay = self.next_delay()
            self.next_refresh = time.time() + delay
            self._wake.wait(delay)
            self._wake.clear()

    # -- snapshot for the API -------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            jobs = [j.to_dict() for lst in self._jobs.values() for j in lst]
            clusters = [s.to_dict() for s in self._status.values()]
        by_name = {c.name: c for c in self.config.enabled_clusters}
        for c in clusters:
            c["connected"] = self.connected(by_name[c["name"]]) if c["name"] in by_name else None
        for j in jobs:
            j["exit_summary"] = _exit_summary(j)
        reachable = sum(1 for c in clusters if c["ok"])
        active = [c for c in clusters if c.get("error_kind") != "login"]
        return {
            "now": time.time(),
            "last_refresh": self.last_refresh,
            "next_refresh": self.next_refresh,
            "refreshing": self.refreshing,
            "offline": bool(active) and reachable == 0 and self.last_refresh is not None,
            "refresh_seconds": self.config.refresh_seconds,
            "lookback_hours": self.config.lookback_hours,
            "history_days": self.config.history_days,
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
