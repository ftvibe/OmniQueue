"""Slow background poll of whole projects (Slurm accounts): who runs how much, the
project's fairshare and quota, plus a load sample per partition for the predictor.

This runs on its own, much slower, timescale (default every 2 h, per cluster) and
keeps its own JSON store so a rolling overview survives restarts.  Nothing here
ever opens an ssh connection: like the job poll it rides on the connection that
``omniqueue login`` opened.
"""

from __future__ import annotations

import getpass
import json
import logging
import os
import statistics
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from .config import ClusterConfig, Config, secure_dir
from .slurm import (parse_load, parse_project_queue, parse_project_sacct, parse_sshare, project_command,
                    split_combined_output)
from .ssh import RemoteError, close_connection, run_on_cluster, touch_last_use

log = logging.getLogger("omniqueue.projects")

ACTIVE_STATES = {"RUNNING", "PENDING", "COMPLETING", "CONFIGURING", "SUSPENDED", "REQUEUED"}
USAGE_WINDOWS_DAYS = (7, 30)
DAILY_DAYS = 30


def slurm_ts(text: str | None) -> float | None:
    """``2026-09-18T07:47:55`` (cluster local time, taken as local) -> unix seconds."""
    if not text:
        return None
    try:
        return time.mktime(time.strptime(text[:19], "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


class ProjectStore:
    """JSON store: project jobs (from sacct), fairshare samples, load samples, latest queue."""

    def __init__(self, path: Path, retention_days: int = 90):
        self.path = Path(path)
        self.retention_days = retention_days
        self._lock = threading.Lock()
        self.data: dict[str, Any] = {"version": 1, "meta": {"last_poll": {}}, "jobs": {}, "shares": {}, "load": {}, "queue": {}}
        self._load()

    # -- persistence ---------------------------------------------------------------
    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        if isinstance(data, dict) and data.get("version") == 1:
            for key in ("meta", "jobs", "shares", "load", "queue"):
                if isinstance(data.get(key), dict):
                    self.data[key] = data[key]
            self.data["meta"].setdefault("last_poll", {})

    def save(self) -> None:
        with self._lock:
            secure_dir(self.path.parent)
            self.data["saved_at"] = time.time()
            fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".projects-", suffix=".json")
            try:
                with os.fdopen(fd, "w") as fh:
                    json.dump(self.data, fh)
                os.chmod(tmp, 0o600)
                os.replace(tmp, self.path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise

    # -- recording a poll -------------------------------------------------------------
    def last_poll(self, cluster: str) -> float | None:
        return self.data["meta"]["last_poll"].get(cluster)

    def record_poll(self, cluster: str, projects: list[str], now: float, sacct_rows: list[dict],
                    queue_rows: list[dict], sshare_rows: list[dict], load_parts: list[dict]) -> None:
        with self._lock:
            jobs = self.data["jobs"].setdefault(cluster, {})
            for row in sacct_rows:
                proj = row.get("account") or ""
                if proj not in projects:
                    continue
                rec = {k: row.get(k) for k in ("user", "partition", "state", "nodes", "cpus", "cpu_s", "submit",
                                              "start", "end", "time_limit_s", "elapsed_s")}
                rec["seen"] = now
                jobs.setdefault(proj, {})[row["job_id"]] = rec
            queue = self.data["queue"].setdefault(cluster, {})
            for proj in projects:
                queue[proj] = {"ts": now, "rows": [r for r in queue_rows if r.get("account") == proj]}
            shares = self.data["shares"].setdefault(cluster, {})
            for proj in projects:
                mine = [r for r in sshare_rows if r.get("account") == proj]
                if not mine:
                    continue
                account = next((r for r in mine if not r.get("user")), None)
                sample = {
                    "ts": now,
                    "account": {k: account.get(k) for k in ("raw_shares", "norm_shares", "raw_usage", "effective_usage",
                                                             "fairshare", "grp_tres_mins", "grp_tres_raw")} if account else None,
                    "users": {r["user"]: {k: r.get(k) for k in ("raw_shares", "norm_shares", "raw_usage", "effective_usage", "fairshare")}
                              for r in mine if r.get("user")},
                }
                shares.setdefault(proj, []).append(sample)
            self._add_load_samples(cluster, now, load_parts)
            self.data["meta"]["last_poll"][cluster] = now
            self._prune(now)

    def _add_load_samples(self, cluster: str, now: float, load_parts: list[dict]) -> None:
        load = self.data["load"].setdefault(cluster, {})
        for p in load_parts:
            n = p["nodes"]
            load.setdefault(p["partition"], []).append({
                "ts": now, "idle": n["idle"], "mixed": n["mixed"], "allocated": n["allocated"],
                "unavailable": n["unavailable"], "total": n["total"],
                "free_cores": p["cores"]["idle"], "total_cores": p["cores"]["total"],
                "pending_jobs": p["jobs"]["pending"], "pending_nodes": p["pending_nodes"],
                "running_jobs": p["jobs"]["running"], "time_limit_s": p.get("time_limit_s"),
                "tpc": p.get("threads_per_core", 1), "cpus_per_node": p.get("cpus_per_node", 0),
            })

    def add_load_samples(self, cluster: str, now: float, load_parts: list[dict]) -> None:
        """Public entry for pre-recorded samples (the demo back-fills a month this way)."""
        with self._lock:
            self._add_load_samples(cluster, now, load_parts)

    def _prune(self, now: float) -> None:
        cutoff = now - self.retention_days * 86400
        for cluster, projects in self.data["jobs"].items():
            for proj, jobs in projects.items():
                for jid, rec in list(jobs.items()):
                    end = slurm_ts(rec.get("end")) or rec.get("seen") or 0
                    if rec.get("state") in ACTIVE_STATES:
                        end = rec.get("seen") or now
                    if end < cutoff:
                        del jobs[jid]
        for cluster, parts in self.data["load"].items():
            for part, samples in parts.items():
                parts[part] = [s for s in samples if s["ts"] >= cutoff]
        # fairshare: keep every sample for two days, then one per day
        for cluster, projects in self.data["shares"].items():
            for proj, samples in projects.items():
                kept: list[dict] = []
                last_day = None
                for s in sorted(samples, key=lambda x: x["ts"]):
                    if s["ts"] < cutoff:
                        continue
                    day = time.strftime("%Y-%m-%d", time.localtime(s["ts"]))
                    if now - s["ts"] > 2 * 86400 and day == last_day:
                        kept[-1] = s  # keep the last sample of that day
                        continue
                    kept.append(s)
                    last_day = day
                projects[proj] = kept

    def forget_cluster(self, cluster: str) -> None:
        with self._lock:
            for key in ("jobs", "shares", "load", "queue"):
                self.data[key].pop(cluster, None)
            self.data["meta"]["last_poll"].pop(cluster, None)

    # -- reading -------------------------------------------------------------------------
    def jobs(self, cluster: str, project: str) -> dict[str, dict]:
        with self._lock:
            return dict(self.data["jobs"].get(cluster, {}).get(project, {}))

    def queue(self, cluster: str, project: str) -> dict | None:
        with self._lock:
            return self.data["queue"].get(cluster, {}).get(project)

    def shares(self, cluster: str, project: str) -> list[dict]:
        with self._lock:
            return list(self.data["shares"].get(cluster, {}).get(project, []))

    def load_samples(self, cluster: str, partition: str | None = None) -> dict[str, list[dict]]:
        with self._lock:
            parts = self.data["load"].get(cluster, {})
            if partition is not None:
                return {partition: list(parts.get(partition, []))}
            return {k: list(v) for k, v in parts.items()}

    def tpc_map(self, cluster: str) -> dict[str, int]:
        """threads per core per partition, from the latest load sample (1 when unknown)."""
        out: dict[str, int] = {}
        for part, samples in self.load_samples(cluster).items():
            if samples:
                out[part] = max(1, int(samples[-1].get("tpc") or 1))
        return out

    # -- aggregation -----------------------------------------------------------------------
    def summary(self, cluster: str, project: str, now: float, me: str | None = None,
                quota_core_h: float | None = None) -> dict[str, Any]:
        """Everything the project card shows, computed from the store."""
        jobs = self.jobs(cluster, project)
        tpc = self.tpc_map(cluster)
        cores_of = lambda rec: (rec.get("cpus") or 0) / max(1, tpc.get(rec.get("partition") or "", 1))  # noqa: E731

        def interval(rec: dict) -> tuple[float, float] | None:
            start = slurm_ts(rec.get("start"))
            if start is None:
                return None
            end = slurm_ts(rec.get("end")) if rec.get("state") not in ACTIVE_STATES else None
            if end is None:
                end = now if rec.get("state") in ACTIVE_STATES else (start + (rec.get("elapsed_s") or 0))
            return start, max(start, end)

        usage: dict[str, dict] = {}
        for days in USAGE_WINDOWS_DAYS:
            w0 = now - days * 86400
            users: dict[str, dict] = {}
            total = 0.0
            njobs = 0
            for rec in jobs.values():
                iv = interval(rec)
                if not iv:
                    continue
                s, e = iv
                overlap = max(0.0, min(e, now) - max(s, w0))
                if overlap <= 0:
                    continue
                core_h = overlap * cores_of(rec) / 3600
                u = users.setdefault(rec.get("user") or "?", {"core_h": 0.0, "jobs": 0})
                u["core_h"] += core_h
                u["jobs"] += 1
                total += core_h
                njobs += 1
            usage[str(days)] = {"core_h": total, "jobs": njobs, "users": users}

        # daily buckets for the last DAILY_DAYS days, split by user
        day0 = time.localtime(now)
        midnight = time.mktime((day0.tm_year, day0.tm_mon, day0.tm_mday, 0, 0, 0, 0, 0, -1))
        daily: list[dict] = []
        for i in range(DAILY_DAYS - 1, -1, -1):
            d_start = midnight - i * 86400
            d_end = min(now, d_start + 86400)
            users_d: dict[str, float] = {}
            total_d = 0.0
            for rec in jobs.values():
                iv = interval(rec)
                if not iv:
                    continue
                s, e = iv
                overlap = max(0.0, min(e, d_end) - max(s, d_start))
                if overlap <= 0:
                    continue
                core_h = overlap * cores_of(rec) / 3600
                users_d[rec.get("user") or "?"] = users_d.get(rec.get("user") or "?", 0.0) + core_h
                total_d += core_h
            daily.append({"date": time.strftime("%m-%d", time.localtime(d_start)), "core_h": total_d, "users": users_d})

        running = {"jobs": 0, "cores": 0.0, "nodes": 0, "users": {}}
        pending = {"jobs": 0, "cores": 0.0, "nodes": 0, "users": {}}
        q = self.queue(cluster, project)
        for row in (q or {}).get("rows", []):
            bucket = running if row.get("state") == "RUNNING" else pending if row.get("state") == "PENDING" else None
            if bucket is None:
                continue
            count = row.get("tasks") or 1
            cores = (row.get("cpus") or 0) / max(1, tpc.get(row.get("partition") or "", 1)) * count
            bucket["jobs"] += count
            bucket["cores"] += cores
            bucket["nodes"] += (row.get("nodes") or 0) * count
            u = bucket["users"].setdefault(row.get("user") or "?", {"jobs": 0, "cores": 0.0})
            u["jobs"] += count
            u["cores"] += cores

        shares_hist = self.shares(cluster, project)
        latest = shares_hist[-1] if shares_hist else None
        shares = None
        if latest:
            acc = latest.get("account") or {}
            shares = {
                "sampled": latest["ts"], "fairshare": acc.get("fairshare"), "raw_usage": acc.get("raw_usage"),
                "norm_shares": acc.get("norm_shares"), "effective_usage": acc.get("effective_usage"),
                "users": latest.get("users", {}),
                "trend": [{"ts": s["ts"], "fairshare": (s.get("account") or {}).get("fairshare")} for s in shares_hist[-30:]],
            }

        quota = None
        if quota_core_h:
            quota = {"limit_core_h": float(quota_core_h), "used_core_h": usage["30"]["core_h"], "source": "config", "window": "30 d"}
        elif latest and (latest.get("account") or {}).get("grp_tres_mins", {}).get("cpu"):
            acc = latest["account"]
            quota = {"limit_core_h": acc["grp_tres_mins"]["cpu"] / 60, "used_core_h": (acc.get("grp_tres_raw") or {}).get("cpu", 0) / 60,
                     "source": "sshare", "window": "allocation"}
        if quota:
            quota["fraction"] = min(1.0, quota["used_core_h"] / quota["limit_core_h"]) if quota["limit_core_h"] else None

        users = sorted({*usage["30"]["users"], *running["users"], *pending["users"]},
                       key=lambda u: -(usage["30"]["users"].get(u, {}).get("core_h", 0) + running["users"].get(u, {}).get("cores", 0)))
        starts = [slurm_ts(r.get("start")) for r in jobs.values()]
        oldest = min([s for s in starts if s], default=None)
        return {
            "cluster": cluster, "project": project, "updated": (q or {}).get("ts") or self.last_poll(cluster),
            "running": running, "pending": pending, "usage": usage, "daily": daily, "shares": shares, "quota": quota,
            "users": users, "me": me, "jobs_known": len(jobs), "oldest": oldest,
        }

    def typical_hours(self, cluster: str, partition: str) -> float | None:
        """Median run time of finished project jobs on a partition (hours)."""
        lengths = []
        for jobs in self.data["jobs"].get(cluster, {}).values():
            for rec in jobs.values():
                if rec.get("partition") == partition and rec.get("state") not in ACTIVE_STATES and (rec.get("elapsed_s") or 0) > 60:
                    lengths.append(rec["elapsed_s"] / 3600)
        return statistics.median(lengths) if lengths else None


class ProjectPoller:
    """Polls each cluster's projects on its own slow schedule (config.project_interval)."""

    def __init__(self, config: Config, store: ProjectStore, collector):
        self.config = config
        self.store = store
        self.collector = collector  # for the login gate and connection cache
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self.status: dict[str, dict[str, Any]] = {}
        now = time.time()
        for c in config.project_clusters:
            last = store.last_poll(c.name)
            self.status[c.name] = {
                "name": c.name, "projects": list(c.projects), "refresh_seconds": config.project_interval(c),
                "last_poll": last, "next_poll": (last + config.project_interval(c)) if last else now + 5,
                "error": None, "error_kind": None, "warning": None, "fetching": False, "poll_seconds": None,
            }
        self.version = 0  # bumps on every change so the API can answer 304

    @property
    def enabled(self) -> bool:
        return bool(self.status)

    # -- one cluster ------------------------------------------------------------------
    def fetch(self, cluster: ClusterConfig) -> tuple[list[dict], list[dict], list[dict], list[dict], list[str]]:
        """Run the combined command; returns (queue rows, sacct rows, sshare rows, load partitions, warnings)."""
        lookback_h = self.config.project_history_days * 24
        last = self.store.last_poll(cluster.name)
        if last:  # only ask for what changed since the previous poll (plus a day of slack for late accounting)
            lookback_h = min(lookback_h, int((time.time() - last) / 3600) + 24)
        cmd = project_command(list(cluster.projects), lookback_h, cluster.load_partitions or None)
        res = run_on_cluster(cluster, cmd, max(self.config.ssh_timeout, 60), self.config)
        sections = split_combined_output(res.stdout)
        stderr = res.stderr.strip()
        warnings: list[str] = []

        def section(name: str, required: bool = False) -> str:
            out, rc = sections.get(name, ("", -1))
            if rc != 0:
                msg = f"{name} exited {rc}: {stderr[:160]}" if rc >= 0 else f"no {name} output: {stderr[:160] or 'empty reply'}"
                if required:
                    raise RemoteError(msg)
                warnings.append(msg)
                return ""
            return out

        queue_rows = parse_project_queue(section("squeue_proj", required=True))
        sacct_rows = parse_project_sacct(section("sacct_proj"))
        sshare_rows = parse_sshare(section("sshare"))
        si_out = section("sinfo")
        sq_out = section("squeue_all")
        load_parts = parse_load(si_out, sq_out) if si_out else []
        users_seen = {r["user"] for r in queue_rows if r.get("user")}
        users_acct = {r["user"] for r in sacct_rows if r.get("user")}
        if len(users_seen) > 1 and len(users_acct) <= 1 and sacct_rows:
            warnings.append("sacct shows only your own jobs here (site hides other users' accounting); "
                            "usage by user comes from sshare and the queue only")
        return queue_rows, sacct_rows, sshare_rows, load_parts, warnings

    def poll_cluster(self, cluster: ClusterConfig) -> None:
        st = self.status[cluster.name]
        now = time.time()
        interval = self.config.project_interval(cluster)
        if self.collector.needs_login(cluster):
            with self._lock:
                st.update(error="not logged in", error_kind="login", fetching=False, next_poll=now + 120)
                self.version += 1
            return
        with self._lock:
            st["fetching"] = True
            self.version += 1
        t0 = time.monotonic()
        try:
            queue_rows, sacct_rows, sshare_rows, load_parts, warnings = self.fetch(cluster)
        except RemoteError as exc:
            log.warning("%s projects: %s (%s)", cluster.name, exc, exc.kind)
            if exc.kind in ("timeout", "network"):
                close_connection(cluster, self.config)
            with self._lock:
                st.update(error=str(exc), error_kind=exc.kind, fetching=False, poll_seconds=time.monotonic() - t0,
                          next_poll=now + min(interval, 900))
                self.version += 1
            return
        self.store.record_poll(cluster.name, list(cluster.projects), now, sacct_rows, queue_rows, sshare_rows, load_parts)
        if not cluster.is_local and self.config.persist_connections:
            touch_last_use(cluster, self.config)
        with self._lock:
            st.update(error=None, error_kind=None, warning="; ".join(warnings) or None, fetching=False,
                      last_poll=now, next_poll=now + interval, poll_seconds=time.monotonic() - t0)
            self.version += 1

    # -- scheduling ----------------------------------------------------------------------
    def due(self, now: float | None = None) -> list[ClusterConfig]:
        now = now or time.time()
        return [c for c in self.config.project_clusters if self.status[c.name]["next_poll"] <= now and not self.status[c.name]["fetching"]]

    def refresh(self, clusters: list[ClusterConfig] | None = None) -> None:
        clusters = self.due() if clusters is None else clusters
        if not clusters:
            return
        with ThreadPoolExecutor(max_workers=min(8, len(clusters))) as pool:
            list(pool.map(self.poll_cluster, clusters))
        try:
            self.store.save()
        except OSError as exc:
            log.error("could not save project store: %s", exc)

    def request_refresh(self) -> bool:
        """Poll every project cluster now (the card's refresh button)."""
        if not self.enabled:
            return False
        now = time.time()
        with self._lock:
            for st in self.status.values():
                st["next_poll"] = now
        self._wake.set()
        return True

    def start(self) -> None:
        if self._thread is not None or not self.enabled:
            return
        self._thread = threading.Thread(target=self._loop, name="omniqueue-projects", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh()
            except Exception:  # noqa: BLE001
                log.exception("project poll failed")
            now = time.time()
            nxt = min((st["next_poll"] for st in self.status.values()), default=now + 60)
            self._wake.wait(max(1.0, min(300.0, nxt - now)))
            self._wake.clear()

    # -- API ----------------------------------------------------------------------------------
    def me(self, cluster: ClusterConfig) -> str:
        return cluster.user or _local_user()

    def etag(self) -> str:
        return f'"p{self.version}"'

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        clusters = []
        projects = []
        for c in self.config.project_clusters:
            st = dict(self.status[c.name])
            cs = self.collector.status_of(c.name)
            st["color"] = cs.color if cs else c.color
            clusters.append(st)
            for proj in c.projects:
                s = self.store.summary(c.name, proj, now, me=self.me(c), quota_core_h=c.project_quotas.get(proj))
                s["color"] = st["color"]
                s["error"] = st["error"]
                s["error_kind"] = st["error_kind"]
                s["warning"] = st["warning"]
                s["fetching"] = st["fetching"]
                s["refresh_seconds"] = st["refresh_seconds"]
                s["next_poll"] = st["next_poll"]
                projects.append(s)
        return {"now": now, "enabled": self.enabled, "refresh_seconds": self.config.project_refresh_seconds,
                "history_days": self.config.project_history_days, "clusters": clusters, "projects": projects}

    def prediction_data(self, now: float | None = None) -> dict[str, Any]:
        """Everything the (experimental) predictor needs, as plain data: load samples per
        partition, the projects' fairshare and quota, your own past queue waits."""
        now = now or time.time()
        clusters: dict[str, Any] = {}
        for c in self.config.enabled_clusters:
            samples = self.store.load_samples(c.name)
            if not samples and not c.projects:
                continue
            partitions = {}
            for part, ss in samples.items():
                if not ss:
                    continue
                last = ss[-1]
                partitions[part] = {
                    "samples": ss, "time_limit_s": last.get("time_limit_s"), "total_nodes": last.get("total"),
                    "cores_per_node": (last.get("cpus_per_node") or 0) // max(1, last.get("tpc") or 1),
                    "typical_hours": self.store.typical_hours(c.name, part),
                }
            me = self.me(c)
            projs = {}
            for proj in c.projects:
                summ = self.store.summary(c.name, proj, now, me=me, quota_core_h=c.project_quotas.get(proj))
                sh = summ.get("shares") or {}
                projs[proj] = {
                    "fairshare_me": (sh.get("users", {}).get(me) or {}).get("fairshare"),
                    "fairshare_account": sh.get("fairshare"),
                    "quota": summ.get("quota"),
                    "running_cores": summ["running"]["cores"], "pending_cores": summ["pending"]["cores"],
                }
            own_waits = []
            for j in self.collector.history.jobs_for(c.name):
                s, st = slurm_ts(j.submit_time), slurm_ts(j.start_time)
                if s and st and j.state != "PENDING" and st >= s:
                    own_waits.append({"partition": j.partition, "nodes": j.nodes or 1, "wait_s": st - s, "start_ts": st})
            clusters[c.name] = {"nice": c.nice, "partitions": partitions, "projects": projs, "own_waits": own_waits,
                                "color": c.color, "interval": self.config.project_interval(c)}
        return {"now": now, "clusters": clusters}


def _local_user() -> str:
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001
        return os.environ.get("USER", "")
