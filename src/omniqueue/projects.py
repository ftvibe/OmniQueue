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
from .slurm import (parse_load, parse_project_queue, parse_project_sacct, parse_squeue_tres, parse_sshare,
                    project_backfill_command, project_command, project_queue_command, split_combined_output)
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
            self._migrate()

    SCHEMA = 2  # bump when stored records need a one-time re-fetch

    def _migrate(self) -> None:
        """One-time upgrades of an existing store, recorded in ``meta.schema`` so they run
        exactly once; the re-fetch itself proceeds in the usual chunks and its progress
        (``meta.oldest``) is saved after every chunk, so a restart resumes rather than
        starting over.

        schema 2: records written before GPUs were tracked have no ``gpus`` field.  The
        coverage marker is moved to the last poll so the back-fill runs once more and
        fills them in; records sacct no longer returns simply keep counting as CPU jobs."""
        meta = self.data["meta"]
        schema = int(meta.get("schema") or 1)
        if schema < 2:
            oldest = meta.setdefault("oldest", {})
            for cluster, projects in self.data["jobs"].items():
                if any("gpus" not in rec for jobs in projects.values() for rec in jobs.values()):
                    last = meta["last_poll"].get(cluster)
                    if last and oldest.get(cluster, 0) < last:
                        log.info("%s: re-fetching project history once to add GPU counts", cluster)
                        oldest[cluster] = last
        meta["schema"] = self.SCHEMA

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

    def oldest(self, cluster: str) -> float | None:
        """Earliest time the accounting has been fetched back to (None before the first poll)."""
        return self.data["meta"].setdefault("oldest", {}).get(cluster)

    def record_backfill(self, cluster: str, projects: list[str], start_ts: float, sacct_rows: list[dict], now: float) -> None:
        """Merge an older chunk of accounting and move the coverage marker back to `start_ts`."""
        with self._lock:
            self._learn_gpu_partitions(cluster, sacct_rows)
            jobs = self.data["jobs"].setdefault(cluster, {})
            for row in sacct_rows:
                proj = row.get("account") or ""
                if proj not in projects:
                    continue
                rec = {k: row.get(k) for k in ("user", "partition", "state", "nodes", "cpus", "cpu_s", "submit",
                                              "start", "end", "time_limit_s", "elapsed_s", "gpus")}
                rec["seen"] = now
                existing = jobs.setdefault(proj, {}).get(row["job_id"])
                if existing is None or "gpus" not in existing:  # a fresher record wins, unless it predates GPU tracking
                    jobs[proj][row["job_id"]] = rec
            oldest = self.data["meta"].setdefault("oldest", {})
            oldest[cluster] = min(oldest.get(cluster, start_ts), start_ts)

    def _learn_gpu_partitions(self, cluster: str, rows: list[dict]) -> None:
        """A partition where any job was allocated GPUs is a GPU partition (sites whose
        sinfo gres is empty or filtered out still get the right split this way)."""
        learned = self.data["meta"].setdefault("gpu_partitions", {}).setdefault(cluster, [])
        for row in rows:
            part = row.get("partition") or ""
            if (row.get("gpus") or 0) > 0 and part and part not in learned:
                learned.append(part)

    def record_poll(self, cluster: str, projects: list[str], now: float, sacct_rows: list[dict],
                    queue_rows: list[dict], sshare_rows: list[dict], load_parts: list[dict],
                    window_start: float | None = None) -> None:
        with self._lock:
            self._learn_gpu_partitions(cluster, sacct_rows + queue_rows)
            jobs = self.data["jobs"].setdefault(cluster, {})
            for row in sacct_rows:
                proj = row.get("account") or ""
                if proj not in projects:
                    continue
                rec = {k: row.get(k) for k in ("user", "partition", "state", "nodes", "cpus", "cpu_s", "submit",
                                              "start", "end", "time_limit_s", "elapsed_s", "gpus")}
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
            if window_start is not None:
                oldest = self.data["meta"].setdefault("oldest", {})
                oldest[cluster] = min(oldest.get(cluster, window_start), window_start)
            self._prune(now)

    def record_queue(self, cluster: str, projects: list[str], now: float, queue_rows: list[dict]) -> None:
        """Replace the stored queue of a cluster's projects (the quick refresh)."""
        with self._lock:
            self._learn_gpu_partitions(cluster, queue_rows)
            queue = self.data["queue"].setdefault(cluster, {})
            for proj in projects:
                queue[proj] = {"ts": now, "rows": [r for r in queue_rows if r.get("account") == proj]}

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
                "gpus_per_node": p.get("gpus_per_node", 0),
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
            self.data["meta"].setdefault("oldest", {}).pop(cluster, None)

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
    def gpn_map(self, cluster: str) -> dict[str, int]:
        """GPUs per node per partition, from the latest load sample (0 = CPU partition)."""
        out: dict[str, int] = {}
        for part, samples in self.load_samples(cluster).items():
            if samples:
                out[part] = int(samples[-1].get("gpus_per_node") or 0)
        return out

    def gpu_partitions(self, cluster: str, configured: list[str] | None = None) -> set[str]:
        """Partitions whose jobs count as GPU jobs: the configured list, those sinfo reports
        GPUs for, and those where a job was seen with GPUs allocated."""
        parts = set(configured or [])
        parts.update(p for p, n in self.gpn_map(cluster).items() if n > 0)
        with self._lock:
            parts.update(self.data["meta"].get("gpu_partitions", {}).get(cluster, []))
        return parts

    def summary(self, cluster: str, project: str, now: float, me: str | None = None,
                quota_core_h: float | None = None, quota_gpu_h: float | None = None,
                gpu_partitions: list[str] | None = None, gpu_factor: float = 1.0) -> dict[str, Any]:
        """Everything the project card shows, computed from the store.

        ``gpu_factor`` converts Slurm GPU units into billed GPUs: LUMI-G exposes each
        MI250X as two units and bills half a GPU-hour per unit-hour (0.5).

        CPU jobs and GPU jobs (any allocated GPU, or a job on a GPU partition) are kept
        strictly apart: the ``cpu`` buckets hold CPU jobs in core-hours, the ``gpu``
        buckets hold GPU jobs in GPU-hours (``gpu.core_h`` remembers the cores those
        jobs occupied, for reference only)."""
        jobs = self.jobs(cluster, project)
        tpc = self.tpc_map(cluster)
        gpn = self.gpn_map(cluster)
        gpu_parts = self.gpu_partitions(cluster, gpu_partitions)

        def cores_of(rec: dict) -> float:
            return (rec.get("cpus") or 0) / max(1, tpc.get(rec.get("partition") or "", 1))

        def gpu_units(rec: dict) -> float:
            g = rec.get("gpus") or 0
            if not g and (rec.get("partition") or "") in gpu_parts:  # nothing readable: assume whole nodes
                g = (rec.get("nodes") or 1) * gpn.get(rec.get("partition") or "", 0)
            return g

        def gpus_of(rec: dict, count: int = 1) -> float:
            """Billed GPUs of a job (for GPU-hours): Slurm units x the cluster's factor."""
            return gpu_units(rec) * gpu_factor * count

        def with_accounting(row: dict) -> dict:
            """squeue's gres column misses --gpus-per-node requests; sacct's TRES has them."""
            if not row.get("gpus"):
                acc = jobs.get(row.get("job_id") or "")
                if acc and acc.get("gpus"):
                    return {**row, "gpus": acc["gpus"]}
            return row

        def kind_of(rec: dict) -> str:
            return "gpu" if (rec.get("gpus") or 0) > 0 or (rec.get("partition") or "") in gpu_parts else "cpu"

        def interval(rec: dict) -> tuple[float, float] | None:
            start = slurm_ts(rec.get("start"))
            if start is None:
                return None
            end = slurm_ts(rec.get("end")) if rec.get("state") not in ACTIVE_STATES else None
            if end is None:
                end = now if rec.get("state") in ACTIVE_STATES else (start + (rec.get("elapsed_s") or 0))
            return start, max(start, end)

        def empty_usage() -> dict:
            return {"cpu": {"core_h": 0.0, "jobs": 0, "users": {}}, "gpu": {"gpu_h": 0.0, "core_h": 0.0, "jobs": 0, "users": {}}}

        def add_cpu(bucket: dict, user: str, core_h: float) -> None:
            b = bucket["cpu"]
            u = b["users"].setdefault(user, {"core_h": 0.0, "jobs": 0})
            u["core_h"] += core_h
            u["jobs"] += 1
            b["core_h"] += core_h
            b["jobs"] += 1

        def add_usage(bucket: dict, rec: dict, overlap: float) -> None:
            user = rec.get("user") or "?"
            core_h = overlap * cores_of(rec) / 3600
            if kind_of(rec) == "gpu":
                gpu_h = overlap * gpus_of(rec) / 3600
                b = bucket["gpu"]
                u = b["users"].setdefault(user, {"gpu_h": 0.0, "core_h": 0.0, "jobs": 0})
                u["gpu_h"] += gpu_h
                u["core_h"] += core_h
                u["jobs"] += 1
                b["gpu_h"] += gpu_h
                b["core_h"] += core_h
                b["jobs"] += 1
            else:
                add_cpu(bucket, user, core_h)

        intervals = [(rec, iv) for rec in jobs.values() if (iv := interval(rec))]
        usage: dict[str, dict] = {}
        for days in USAGE_WINDOWS_DAYS:
            w0 = now - days * 86400
            bucket = empty_usage()
            for rec, (s0, e0) in intervals:
                overlap = max(0.0, min(e0, now) - max(s0, w0))
                if overlap > 0:
                    add_usage(bucket, rec, overlap)
            usage[str(days)] = bucket

        # daily buckets for the last DAILY_DAYS days
        day0 = time.localtime(now)
        midnight = time.mktime((day0.tm_year, day0.tm_mon, day0.tm_mday, 0, 0, 0, 0, 0, -1))
        daily: list[dict] = []
        for i in range(DAILY_DAYS - 1, -1, -1):
            d_start = midnight - i * 86400
            d_end = min(now, d_start + 86400)
            bucket = empty_usage()
            for rec, (s0, e0) in intervals:
                overlap = max(0.0, min(e0, d_end) - max(s0, d_start))
                if overlap > 0:
                    add_usage(bucket, rec, overlap)
            daily.append({
                "date": time.strftime("%m-%d", time.localtime(d_start)),
                "core_h": bucket["cpu"]["core_h"], "gpu_h": bucket["gpu"]["gpu_h"],
                "users": {u: v["core_h"] for u, v in bucket["cpu"]["users"].items()},
                "gpu_users": {u: v["gpu_h"] for u, v in bucket["gpu"]["users"].items()},
            })

        def empty_now() -> dict:
            return {"cpu": {"jobs": 0, "cores": 0.0, "nodes": 0, "users": {}},
                    "gpu": {"jobs": 0, "gpus": 0.0, "cores": 0.0, "nodes": 0, "users": {}}}

        running, pending = empty_now(), empty_now()
        q = self.queue(cluster, project)
        jobs_now: list[dict] = []
        for row in (q or {}).get("rows", []):
            bucket = running if row.get("state") == "RUNNING" else pending if row.get("state") == "PENDING" else None
            if bucket is None:
                continue
            row = with_accounting(row)
            count = row.get("tasks") or 1
            jobs_now.append({**row, "kind": kind_of(row), "cores": cores_of(row), "gpus": gpu_units(row),
                             "category": "running" if row.get("state") == "RUNNING" else "pending"})
            cores = cores_of(row) * count
            user = row.get("user") or "?"
            targets = []
            if kind_of(row) == "gpu":
                b = bucket["gpu"]
                gpus = gpu_units(row) * count  # what is in use, in Slurm units; the factor only prices GPU-hours
                b["gpus"] += gpus
                u = b["users"].setdefault(user, {"jobs": 0, "gpus": 0.0})
                u["gpus"] += gpus
                targets.append((b, u))
            else:
                b = bucket["cpu"]
                targets.append((b, b["users"].setdefault(user, {"jobs": 0, "cores": 0.0})))
            for b, u in targets:
                b["jobs"] += count
                b["cores"] += cores
                b["nodes"] += (row.get("nodes") or 0) * count
                u["jobs"] += count
                if "cores" in u:
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

        def make_quota(limit: float | None, used: float, source: str, window: str) -> dict | None:
            if not limit:
                return None
            return {"limit_h": float(limit), "used_h": used, "source": source, "window": window,
                    "fraction": min(1.0, used / limit)}

        acc = (latest or {}).get("account") or {}
        grp_mins, grp_raw = acc.get("grp_tres_mins") or {}, acc.get("grp_tres_raw") or {}
        quota = make_quota(quota_core_h, usage["30"]["cpu"]["core_h"], "config", "30 d") if quota_core_h else \
            make_quota((grp_mins.get("cpu") or 0) / 60, (grp_raw.get("cpu") or 0) / 60, "sshare", "allocation")
        gpu_quota = make_quota(quota_gpu_h, usage["30"]["gpu"]["gpu_h"], "config", "30 d") if quota_gpu_h else \
            make_quota((grp_mins.get("gres/gpu") or 0) / 60, (grp_raw.get("gres/gpu") or 0) / 60, "sshare", "allocation")

        u30 = usage["30"]
        weight = {}
        for u, v in u30["cpu"]["users"].items():
            weight[u] = weight.get(u, 0) + v["core_h"]
        for u, v in u30["gpu"]["users"].items():
            weight[u] = weight.get(u, 0) + v["gpu_h"] * 30 + v["core_h"]  # a GPU-hour weighs like ~30 core-hours here
        for bucket in (running["cpu"], running["gpu"], pending["cpu"], pending["gpu"]):
            for u in bucket["users"]:
                weight.setdefault(u, 0)
        users = sorted(weight, key=lambda u: -weight[u])
        has_gpu = bool(gpu_parts) or u30["gpu"]["jobs"] > 0 or running["gpu"]["jobs"] > 0 or pending["gpu"]["jobs"] > 0
        starts = [slurm_ts(r.get("start")) for r in jobs.values()]
        oldest = min([s0 for s0 in starts if s0], default=None)
        return {
            "cluster": cluster, "project": project, "updated": (q or {}).get("ts") or self.last_poll(cluster),
            "running": running, "pending": pending, "usage": usage, "daily": daily, "shares": shares,
            "quota": quota, "gpu_quota": gpu_quota, "has_gpu": has_gpu, "gpu_partitions": sorted(gpu_parts),
            "gpu_factor": gpu_factor,
            "users": users, "me": me, "jobs_known": len(jobs), "oldest": oldest,
            "jobs_now": sorted(jobs_now, key=lambda r: (r["category"] != "running", -(r.get("elapsed_s") or 0), r["job_id"])),
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
                "error": None, "error_kind": None, "warning": None, "fetching": False, "backfilling": False,
                "poll_seconds": None, "coverage_days": None, "queue_fetching": False, "queue_error": None,
            }
            if last is not None:
                self.status[c.name]["coverage_days"] = self.coverage_days(c, now)
                if self.backfill_pending(c, now):  # resume an interrupted back-fill soon after start
                    self.status[c.name]["next_poll"] = min(self.status[c.name]["next_poll"], now + 30)
        self.version = 0  # bumps on every change so the API can answer 304
        self._force_regular = False  # a manual refresh runs a regular poll even mid back-fill

    @property
    def enabled(self) -> bool:
        return bool(self.status)

    # -- one cluster ------------------------------------------------------------------
    FIRST_DAYS = 3  # the first poll asks sacct for this much; older history is back-filled in chunks
    BACKFILL_GAP = 60.0  # seconds between two back-fill chunks

    def _run(self, cluster: ClusterConfig, cmd: str):
        return run_on_cluster(cluster, cmd, max(self.config.ssh_timeout, self.config.project_timeout), self.config)

    def _sections(self, res):
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
        return section, warnings

    def window_start(self, cluster: ClusterConfig, now: float) -> float:
        """Where this poll's sacct window begins: the first poll takes FIRST_DAYS, later
        ones the time since the previous poll plus `project_overlap_hours` of slack, so
        accounting records that arrived late are picked up.  Everything older is already
        in the local store and is never asked for again (sacct lists a job whenever it
        was active at any moment of the window, so a job that was running at the last
        poll and has finished since is included without any overlap)."""
        last = self.store.last_poll(cluster.name)
        if last is None:
            return now - self.FIRST_DAYS * 86400
        return max(now - self.config.project_history_days * 86400, last - self.config.project_overlap_hours * 3600)

    def fetch(self, cluster: ClusterConfig, start_ts: float) -> tuple[list[dict], list[dict], list[dict], list[dict], list[str]]:
        """Run the combined command with an sacct window from `start_ts` to now.
        Returns (queue rows, sacct rows, sshare rows, load partitions, warnings)."""
        res = self._run(cluster, project_command(list(cluster.projects), start_ts, None))  # all partitions: GPU detection needs them
        section, warnings = self._sections(res)
        queue_rows = self._queue_rows(section)
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

    @staticmethod
    def _queue_rows(section) -> list[dict]:
        rows = parse_project_queue(section("squeue_proj", required=True))
        tres = parse_squeue_tres(section("squeue_tres"))  # optional: older squeue may lack the long format
        for r in rows:
            if not r.get("gpus") and tres.get(r["job_id"]):
                r["gpus"] = tres[r["job_id"]]
        return rows

    def fetch_queue(self, cluster: ClusterConfig) -> list[dict]:
        """Only the projects' running and waiting jobs (no accounting, sshare or load)."""
        res = self._run(cluster, project_queue_command(list(cluster.projects)))
        section, _ = self._sections(res)
        return self._queue_rows(section)

    def refresh_queue(self, clusters: list[ClusterConfig] | None = None) -> None:
        """Blocking: re-read the queue of these (default: all) project clusters."""
        clusters = [c for c in (clusters or self.config.project_clusters) if not self.collector.needs_login(c)]
        if not clusters:
            return

        def one(cluster: ClusterConfig) -> None:
            t0 = time.monotonic()
            try:
                rows = self.fetch_queue(cluster)
            except RemoteError as exc:
                log.warning("%s queue: %s (%s)", cluster.name, exc, exc.kind)
                with self._lock:
                    self.status[cluster.name].update(queue_error=str(exc), queue_fetching=False)
                    self.version += 1
                return
            self.store.record_queue(cluster.name, list(cluster.projects), time.time(), rows)
            if not cluster.is_local and self.config.persist_connections:
                touch_last_use(cluster, self.config)
            with self._lock:
                self.status[cluster.name].update(queue_error=None, queue_fetching=False, queue_seconds=time.monotonic() - t0)
                self.version += 1

        with self._lock:
            for c in clusters:
                self.status[c.name]["queue_fetching"] = True
            self.version += 1
        with ThreadPoolExecutor(max_workers=min(8, len(clusters))) as pool:
            list(pool.map(one, clusters))
        try:
            self.store.save()
        except OSError as exc:
            log.error("could not save project store: %s", exc)

    def request_queue_refresh(self, cluster: str | None = None) -> bool:
        """Start a quick queue refresh in the background; False when nothing matches."""
        clusters = [c for c in self.config.project_clusters if cluster is None or c.name == cluster]
        if not clusters or any(self.status[c.name].get("queue_fetching") for c in clusters):
            return False
        threading.Thread(target=self.refresh_queue, args=(clusters,), name="omniqueue-project-queue", daemon=True).start()
        return True

    def fetch_backfill(self, cluster: ClusterConfig, start_ts: float, end_ts: float) -> list[dict]:
        """One older chunk of accounting only (no queue, sshare or load)."""
        res = self._run(cluster, project_backfill_command(list(cluster.projects), start_ts, end_ts))
        section, _ = self._sections(res)
        return parse_project_sacct(section("sacct_proj", required=True))

    def backfill_pending(self, cluster: ClusterConfig, now: float | None = None) -> bool:
        """True while older history is still missing from the store."""
        now = now or time.time()
        oldest = self.store.oldest(cluster.name)
        return oldest is not None and oldest > now - self.config.project_history_days * 86400 + 3600

    def coverage_days(self, cluster: ClusterConfig, now: float | None = None) -> float | None:
        now = now or time.time()
        oldest = self.store.oldest(cluster.name)
        return None if oldest is None else min(self.config.project_history_days, (now - oldest) / 86400)

    def _fail(self, cluster: ClusterConfig, exc: RemoteError, t0: float, retry_in: float) -> None:
        log.warning("%s projects: %s (%s)", cluster.name, exc, exc.kind)
        if exc.kind in ("timeout", "network"):
            close_connection(cluster, self.config)
        with self._lock:
            self.status[cluster.name].update(error=str(exc), error_kind=exc.kind, fetching=False, backfilling=False,
                                             poll_seconds=time.monotonic() - t0, next_poll=time.time() + retry_in)
            self.version += 1

    def poll_cluster(self, cluster: ClusterConfig) -> None:
        """A regular poll, or, when the store is up to date but history is missing, one
        back-fill chunk.  Either way one ssh round trip."""
        st = self.status[cluster.name]
        now = time.time()
        interval = self.config.project_interval(cluster)
        if self.collector.needs_login(cluster):
            with self._lock:
                st.update(error="not logged in", error_kind="login", fetching=False, next_poll=now + 120)
                self.version += 1
            return
        last = self.store.last_poll(cluster.name)
        backfill = (last is not None and now - last < interval and self.backfill_pending(cluster, now)
                    and not getattr(self, "_force_regular", False))
        with self._lock:
            st["fetching"] = True
            st["backfilling"] = backfill
            self.version += 1
        t0 = time.monotonic()
        if backfill:
            end_ts = self.store.oldest(cluster.name)
            start_ts = max(end_ts - self.config.project_backfill_days * 86400, now - self.config.project_history_days * 86400)
            try:
                rows = self.fetch_backfill(cluster, start_ts, end_ts)
            except RemoteError as exc:
                self._fail(cluster, exc, t0, retry_in=min(interval, 900))
                return
            self.store.record_backfill(cluster.name, list(cluster.projects), start_ts, rows, now)
        else:
            start_ts = self.window_start(cluster, now)
            try:
                queue_rows, sacct_rows, sshare_rows, load_parts, warnings = self.fetch(cluster, start_ts)
            except RemoteError as exc:
                self._fail(cluster, exc, t0, retry_in=min(interval, 900))
                return
            self.store.record_poll(cluster.name, list(cluster.projects), now, sacct_rows, queue_rows, sshare_rows,
                                   load_parts, window_start=start_ts)
        if not cluster.is_local and self.config.persist_connections:
            touch_last_use(cluster, self.config)
        more = self.backfill_pending(cluster, now)
        with self._lock:
            st.update(error=None, error_kind=None, fetching=False, backfilling=False, poll_seconds=time.monotonic() - t0,
                      coverage_days=self.coverage_days(cluster, now),
                      # the next regular poll keeps its schedule; back-fill chunks come a minute apart in between
                      next_poll=min(now + self.BACKFILL_GAP, self._next_regular(cluster, now)) if more else self._next_regular(cluster, now))
            if not backfill:
                st.update(last_poll=now, warning="; ".join(warnings) or None)
            self.version += 1

    def _next_regular(self, cluster: ClusterConfig, now: float) -> float:
        last = self.store.last_poll(cluster.name) or now
        return last + self.config.project_interval(cluster)

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
        self._force_regular = False
        try:
            self.store.save()
        except OSError as exc:
            log.error("could not save project store: %s", exc)

    def backfill_all(self, progress=None, max_chunks: int = 200) -> None:
        """Blocking: keep polling until every cluster's history is complete (CLI --poll)."""
        for _ in range(max_chunks):
            todo = [c for c in self.config.project_clusters if self.backfill_pending(c) and not self.status[c.name]["error"]]
            if not todo:
                return
            if progress:
                progress(todo)
            self.refresh(todo)

    def request_refresh(self) -> bool:
        """Poll every project cluster now (the card's refresh button)."""
        if not self.enabled:
            return False
        now = time.time()
        with self._lock:
            for st in self.status.values():
                st["next_poll"] = now
            self._force_regular = True
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
        cached = getattr(self, "_snap_cache", None)
        if cached and cached[0] == self.version and time.time() - cached[1] < 300:
            return cached[2]
        snap = self._snapshot()
        self._snap_cache = (self.version, time.time(), snap)
        return snap

    def _snapshot(self) -> dict[str, Any]:
        now = time.time()
        clusters = []
        projects = []
        for c in self.config.project_clusters:
            st = dict(self.status[c.name])
            cs = self.collector.status_of(c.name)
            st["color"] = cs.color if cs else c.color
            clusters.append(st)
            for proj in c.projects:
                s = self.store.summary(c.name, proj, now, me=self.me(c), quota_core_h=c.project_quotas.get(proj),
                                       quota_gpu_h=c.project_gpu_quotas.get(proj), gpu_partitions=c.gpu_partitions,
                                       gpu_factor=c.gpu_hour_factor)
                s["color"] = st["color"]
                s["pi"] = c.project_pis.get(proj)
                s["error"] = st["error"]
                s["error_kind"] = st["error_kind"]
                s["warning"] = st["warning"]
                s["fetching"] = st["fetching"]
                s["queue_fetching"] = st.get("queue_fetching", False)
                s["queue_error"] = st.get("queue_error")
                s["backfilling"] = st["backfilling"]
                s["coverage_days"] = st["coverage_days"]
                s["backfill_pending"] = self.backfill_pending(c, now)
                s["refresh_seconds"] = st["refresh_seconds"]
                s["next_poll"] = st["next_poll"]
                projects.append(s)
        return {"now": now, "enabled": self.enabled, "refresh_seconds": self.config.project_refresh_seconds,
                "history_days": self.config.project_history_days, "backfill_days": self.config.project_backfill_days,
                "clusters": clusters, "projects": projects}

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
                if not ss or (c.load_partitions and part not in c.load_partitions):
                    continue
                last = ss[-1]
                partitions[part] = {
                    "samples": ss, "time_limit_s": last.get("time_limit_s"), "total_nodes": last.get("total"),
                    "cores_per_node": (last.get("cpus_per_node") or 0) // max(1, last.get("tpc") or 1),
                    "gpus_per_node": last.get("gpus_per_node") or 0,
                    "gpu": part in self.store.gpu_partitions(c.name, c.gpu_partitions),
                    "typical_hours": self.store.typical_hours(c.name, part),
                }
            me = self.me(c)
            projs = {}
            for proj in c.projects:
                summ = self.store.summary(c.name, proj, now, me=me, quota_core_h=c.project_quotas.get(proj),
                                          quota_gpu_h=c.project_gpu_quotas.get(proj), gpu_partitions=c.gpu_partitions,
                                          gpu_factor=c.gpu_hour_factor)
                sh = summ.get("shares") or {}
                projs[proj] = {
                    "fairshare_me": (sh.get("users", {}).get(me) or {}).get("fairshare"),
                    "fairshare_account": sh.get("fairshare"),
                    "quota": summ.get("quota"), "gpu_quota": summ.get("gpu_quota"),
                    "running_cores": summ["running"]["cpu"]["cores"], "pending_cores": summ["pending"]["cpu"]["cores"],
                    "running_gpus": summ["running"]["gpu"]["gpus"], "pending_gpus": summ["pending"]["gpu"]["gpus"],
                }
            own_waits = []
            for j in self.collector.history.jobs_for(c.name):
                s, st = slurm_ts(j.submit_time), slurm_ts(j.start_time)
                if s and st and j.state != "PENDING" and st >= s:
                    own_waits.append({"partition": j.partition, "nodes": j.nodes or 1, "wait_s": st - s, "start_ts": st})
            clusters[c.name] = {"nice": c.nice, "partitions": partitions, "projects": projs, "own_waits": own_waits,
                                "color": c.color, "interval": self.config.project_interval(c), "gpu_factor": c.gpu_hour_factor}
        return {"now": now, "clusters": clusters}


def _local_user() -> str:
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001
        return os.environ.get("USER", "")
