"""Fake clusters and jobs so the dashboard can be tried without any ssh access."""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from .collector import Collector, ClusterStatus
from .config import ClusterConfig, Config
from .history import HistoryStore
from .models import Job
from .projects import ProjectPoller, ProjectStore

_NAMES = ["vasp-relax", "qe-scf", "md-npt", "phonopy-fc", "gw-bandstructure", "neb-path", "aimd-2000K", "elastic-c11"]
_PROBLEMS = ["FAILED", "TIMEOUT", "OUT_OF_MEMORY", "CANCELLED", "NODE_FAIL"]


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def make_demo_jobs(cluster: str, rng: random.Random, n: int = 18) -> list[Job]:
    now = datetime.now()
    jobs: list[Job] = []
    base_id = rng.randint(100000, 900000)
    for i in range(n):
        job_id = str(base_id + i * 7)
        name = f"{rng.choice(_NAMES)}-{rng.randint(1, 40):02d}"
        roll = rng.random()
        limit = rng.choice([3600, 4 * 3600, 12 * 3600, 24 * 3600])
        submit = now - timedelta(hours=rng.uniform(0.2, 60))
        if roll < 0.25:  # running
            elapsed = int(limit * rng.uniform(0.05, 0.95))
            start = now - timedelta(seconds=elapsed)
            jobs.append(Job(cluster, job_id, name, "RUNNING", user="demo", partition="main", nodes=rng.randint(1, 8),
                            cpus=rng.randint(32, 512), node_list=f"n[{rng.randint(1,900)}-{rng.randint(901,999)}]",
                            elapsed_s=elapsed, time_limit_s=limit, submit_time=_ts(submit), start_time=_ts(start),
                            work_dir=f"/proj/demo/{name}", source="squeue", last_seen=time.time()))
        elif roll < 0.45:  # pending
            jobs.append(Job(cluster, job_id, name, "PENDING", user="demo", partition="main", nodes=rng.randint(1, 16),
                            cpus=rng.randint(32, 1024), reason=rng.choice(["Priority", "Resources", "QOSMaxJobsPerUserLimit", "Dependency"]),
                            elapsed_s=0, time_limit_s=limit, submit_time=_ts(submit),
                            work_dir=f"/proj/demo/{name}", source="squeue", last_seen=time.time()))
        else:  # finished
            problem = rng.random() < 0.3
            state = rng.choice(_PROBLEMS) if problem else "COMPLETED"
            elapsed = limit if state == "TIMEOUT" else int(limit * rng.uniform(0.1, 0.9))
            end = now - timedelta(minutes=rng.uniform(5, 2400))
            start = end - timedelta(seconds=elapsed)
            submit = min(submit, start - timedelta(minutes=rng.uniform(1, 300)))
            exit_code = {"FAILED": "1:0", "OUT_OF_MEMORY": "0:125", "CANCELLED": "0:15", "TIMEOUT": "0:0", "NODE_FAIL": "0:0"}.get(state, "0:0")
            jobs.append(Job(cluster, job_id, name, state, user="demo", partition="main", nodes=rng.randint(1, 8),
                            cpus=rng.randint(32, 512), node_list=f"n{rng.randint(1,999)}", exit_code=exit_code,
                            reason="cancelled by uid 1000" if state == "CANCELLED" else "",
                            elapsed_s=elapsed, time_limit_s=limit, submit_time=_ts(submit), start_time=_ts(start),
                            end_time=_ts(end), work_dir=f"/proj/demo/{name}", source="sacct", last_seen=time.time()))
    return jobs


def make_demo_load(cluster: str, rng: random.Random) -> list[dict]:
    specs = {
        "tetralith": [("main", 1800, 32, 3 * 86400, True), ("large", 60, 32, 7 * 86400, False), ("gpu", 40, 128, 86400, False)],
        "dardel": [("main", 1200, 128, 86400, True), ("shared", 200, 128, 86400, False), ("gpu", 56, 64, 86400, False), ("long", 96, 128, 7 * 86400, False)],
        "lumi": [("standard", 1400, 128, 2 * 86400, True), ("standard-g", 2500, 64, 2 * 86400, False), ("small", 300, 128, 3 * 86400, False), ("debug", 8, 128, 1800, False)],
    }.get(cluster, [("batch", 500, 64, 86400, True)])
    out = []
    tpc = 2 if cluster == "lumi" else 1
    for name, nodes, cpn, limit, default in specs:
        cpn *= tpc
        busy = rng.uniform(0.55, 0.98)
        alloc = int(nodes * busy)
        mixed = int(nodes * rng.uniform(0, 0.08))
        down = int(nodes * rng.uniform(0, 0.04))
        idle = max(0, nodes - alloc - mixed - down)
        pend = rng.randint(0, 400)
        out.append({
            "partition": name, "default": default, "avail": "up", "time_limit_s": limit,
            "nodes": {"idle": idle, "mixed": mixed, "allocated": alloc, "unavailable": down, "total": nodes},
            "cpus": {"allocated": alloc * cpn + mixed * cpn // 2, "idle": idle * cpn + mixed * cpn // 2,
                     "other": down * cpn, "total": nodes * cpn},
            "threads_per_core": tpc,
            "cores": {"allocated": (alloc * cpn + mixed * cpn // 2) // tpc, "idle": (idle * cpn + mixed * cpn // 2) // tpc,
                      "other": down * cpn // tpc, "total": nodes * cpn // tpc},
            "jobs": {"running": alloc // rng.randint(1, 4) + 1, "pending": pend},
            "pending_nodes": pend * rng.randint(1, 6), "pending_cpus": pend * cpn, "pending_cores": pend * cpn // tpc,
            "running_nodes": alloc,
        })
    return out


def make_demo_array(cluster: str, rng: random.Random, base: int, name: str, total: int = 40) -> list[Job]:
    """One job array: a few tasks running, some done, a couple failed, the rest waiting."""
    now = datetime.now()
    limit = 4 * 3600
    submit = now - timedelta(hours=3)
    jobs: list[Job] = []
    for t in range(1, total + 1):
        jid = f"{base}_{t}"
        common = dict(cluster=cluster, job_id=jid, name=name, user="demo", partition="main", nodes=1, cpus=32,
                      time_limit_s=limit, submit_time=_ts(submit), work_dir=f"/proj/demo/{name}", last_seen=time.time())
        if t <= 10:  # finished
            elapsed = int(limit * rng.uniform(0.3, 0.8))
            start = submit + timedelta(minutes=5 * t)
            state = "FAILED" if t in (4, 7) else "COMPLETED"
            jobs.append(Job(state=state, exit_code="1:0" if state == "FAILED" else "0:0", elapsed_s=elapsed,
                            start_time=_ts(start), end_time=_ts(start + timedelta(seconds=elapsed)), node_list=f"n{100 + t}",
                            source="sacct", **common))
        elif t <= 15:  # running
            elapsed = int(limit * rng.uniform(0.1, 0.9))
            jobs.append(Job(state="RUNNING", elapsed_s=elapsed, start_time=_ts(now - timedelta(seconds=elapsed)),
                            node_list=f"n{200 + t}", source="squeue", **common))
        else:  # waiting
            jobs.append(Job(state="PENDING", reason="Priority", elapsed_s=0, source="squeue", **common))
    return jobs


class DemoCollector(Collector):
    """A Collector that fabricates data instead of talking to clusters."""

    def __init__(self, config: Config, history: HistoryStore, seed: int = 1):
        super().__init__(config, history)
        self._rng = random.Random(seed)

    def connected(self, cluster: ClusterConfig) -> bool | None:
        return cluster.name != "offline-cluster"

    def needs_login(self, cluster: ClusterConfig) -> bool:
        return False  # the demo's offline cluster fails with a network error instead

    def fetch_load_cluster(self, cluster: ClusterConfig) -> dict:
        from .slurm import summarize_load

        time.sleep(self._rng.uniform(0.2, 0.8))
        rec = {"name": cluster.name, "partitions": [], "summary": None, "error": None,
               "fetched_at": time.time(), "filter": list(cluster.load_partitions)}
        if cluster.name == "offline-cluster":
            rec["error"] = "ssh failed: connect to host unreachable.example.org port 22: Connection timed out"
            return rec
        parts = make_demo_load(cluster.name, self._rng)
        if cluster.load_partitions:
            parts = [p for p in parts if p["partition"] in cluster.load_partitions]
        rec["partitions"] = parts
        rec["summary"] = summarize_load(parts)
        return rec

    def poll_cluster(self, cluster: ClusterConfig) -> tuple[list[Job], ClusterStatus]:
        status = self._status[cluster.name]
        status.last_attempt = time.time()
        time.sleep(self._rng.uniform(0.05, 0.4))
        if cluster.name == "offline-cluster":
            status.ok = False
            status.error = "ssh failed: connect to host unreachable.example.org port 22: Connection timed out"
            status.error_kind = "network"
            status.failures += 1
            return [], status
        jobs = make_demo_jobs(cluster.name, self._rng)
        if cluster.name == "tetralith":
            jobs += make_demo_array(cluster.name, self._rng, 620000, "phonon-disp")
        if cluster.name == "lumi":
            jobs += make_demo_array(cluster.name, self._rng, 699000, "md-replicas", total=12)
        status.ok = True
        status.error = None
        status.last_success = time.time()
        status.poll_seconds = self._rng.uniform(0.5, 3.0)
        return jobs, status


def demo_config() -> Config:
    import tempfile
    from pathlib import Path

    logo_dir = Path(tempfile.gettempdir()) / "omniqueue-demo-logos"
    logo_dir.mkdir(exist_ok=True)
    (logo_dir / "lumi.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 40 40"><circle cx="20" cy="20" r="18" fill="#053229"/>'
        '<path d="M12 27 20 11l8 16z" fill="#8fb8b4"/><circle cx="20" cy="24" r="3" fill="#e2856c"/></svg>'
    )
    return Config(
        logo_dir=logo_dir,
        clusters=[
            ClusterConfig(name="tetralith", host="tetralith.nsc.liu.se", color="#5f9e99", projects=["naiss2025-1-42"],
                          project_quotas={"naiss2025-1-42": 120000}, nice=0),
            ClusterConfig(name="dardel", host="dardel.pdc.kth.se", color="#e2856c", load_partitions=["main", "gpu"],
                          projects=["naiss2025-3-7"], project_refresh_seconds=3600, nice=2000),
            ClusterConfig(name="lumi", host="lumi.csc.fi", color="#b39a4b", projects=["project_465000123"]),
            ClusterConfig(name="offline-cluster", host="unreachable.example.org", color="#8fb8b4", projects=["naiss2025-9-9"]),
        ],
        refresh_seconds=30,
    )


# ---- demo projects ------------------------------------------------------------------------
_DEMO_PROJECTS = {"tetralith": ["naiss2025-1-42"], "dardel": ["naiss2025-3-7"], "lumi": ["project_465000123"]}
_DEMO_USERS = ["demo", "x_annli", "x_johsm", "x_marle", "x_petbe", "x_saraw"]


def _demo_project_rows(cluster: str, project: str, rng: random.Random, now: float, days: int = 60) -> tuple[list[dict], list[dict]]:
    """Fabricated sacct rows over `days` days plus the queue right now, for one project."""
    weights = [0.32, 0.25, 0.18, 0.12, 0.08, 0.05]
    parts = {"tetralith": ("main", 32, 1), "dardel": ("main", 128, 1), "lumi": ("standard", 256, 2)}
    part, cpn, tpc = parts.get(cluster, ("batch", 64, 1))
    sacct: list[dict] = []
    base = rng.randint(100000, 800000)
    n_jobs = int(days * rng.uniform(3, 7))
    for i in range(n_jobs):
        user = rng.choices(_DEMO_USERS, weights)[0]
        nodes = rng.choice([1, 1, 1, 1, 2, 2, 4, 8])
        hours = rng.choice([0.5, 1, 2, 4, 8, 12, 24])
        start = now - rng.uniform(0, days * 86400)
        elapsed = int(hours * 3600 * rng.uniform(0.3, 1.0))
        end = start + elapsed
        state = "COMPLETED" if rng.random() < 0.85 else rng.choice(["FAILED", "TIMEOUT", "CANCELLED"])
        if end > now:
            state, end = "RUNNING", None
        ts = lambda t: time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(t))  # noqa: E731
        sacct.append({
            "job_id": str(base + i), "account": project, "user": user, "partition": part, "state": state,
            "nodes": nodes, "cpus": nodes * cpn, "elapsed_s": int((now if end is None else end) - start), "cpu_s": 0,
            "submit": ts(start - rng.uniform(60, 6 * 3600)), "start": ts(start), "end": ts(end) if end else "",
            "time_limit_s": int(hours * 3600),
        })
    queue: list[dict] = []
    for r in sacct:
        if r["state"] == "RUNNING":
            queue.append({"job_id": r["job_id"], "account": project, "user": r["user"], "state": "RUNNING", "partition": part,
                          "nodes": r["nodes"], "cpus": r["cpus"], "time_limit_s": r["time_limit_s"], "elapsed_s": r["elapsed_s"], "tasks": 1})
    for i in range(rng.randint(2, 9)):
        user = rng.choices(_DEMO_USERS, weights)[0]
        nodes = rng.choice([1, 2, 4, 8])
        tasks = rng.choice([1, 1, 1, 20, 50])
        queue.append({"job_id": f"{base + n_jobs + i}" + (f"_[1-{tasks}]" if tasks > 1 else ""), "account": project, "user": user,
                      "state": "PENDING", "partition": part, "nodes": nodes, "cpus": nodes * cpn, "time_limit_s": 4 * 3600,
                      "elapsed_s": 0, "tasks": tasks})
    return sacct, queue


def _demo_sshare(project: str, rng: random.Random) -> list[dict]:
    rows = [{"account": project, "user": "", "raw_shares": 1, "norm_shares": 0.01, "raw_usage": rng.randint(2_000_000, 9_000_000),
             "effective_usage": rng.uniform(0.005, 0.02), "fairshare": rng.uniform(0.2, 0.9),
             "grp_tres_mins": {"cpu": 100000 * 60} if project.startswith("naiss2025-3") else {},
             "grp_tres_raw": {"cpu": rng.randint(20000, 80000) * 60} if project.startswith("naiss2025-3") else {}, "tres_run_mins": {}}]
    for u in _DEMO_USERS:
        rows.append({"account": project, "user": u, "raw_shares": 1, "norm_shares": 0.002, "raw_usage": rng.randint(10000, 3_000_000),
                     "effective_usage": rng.uniform(0.0005, 0.005), "fairshare": rng.uniform(0.1, 0.95),
                     "grp_tres_mins": {}, "grp_tres_raw": {}, "tres_run_mins": {}})
    return rows


class DemoProjectPoller(ProjectPoller):
    """Fabricates project usage; the first poll back-fills a month of load samples."""

    def __init__(self, config: Config, store: ProjectStore, collector: Collector, seed: int = 7):
        super().__init__(config, store, collector)
        self._rng = random.Random(seed)

    def fetch(self, cluster: ClusterConfig):
        time.sleep(self._rng.uniform(0.2, 0.6))
        if cluster.name == "offline-cluster":
            from .ssh import RemoteError

            raise RemoteError("ssh failed: connect to host unreachable.example.org port 22: Connection timed out", kind="network")
        now = time.time()
        sacct_all: list[dict] = []
        queue_all: list[dict] = []
        sshare_all: list[dict] = []
        first = self.store.last_poll(cluster.name) is None
        for proj in cluster.projects:
            sacct, queue = _demo_project_rows(cluster.name, proj, self._rng, now, days=60 if first else 2)
            sacct_all += sacct
            queue_all += queue
            sshare_all += _demo_sshare(proj, self._rng)
        parts = make_demo_load(cluster.name, self._rng)
        if cluster.load_partitions:
            parts = [p for p in parts if p["partition"] in cluster.load_partitions]
        if first:  # a month of samples every two hours, so the predictor has something to work with
            for k in range(30 * 12, 0, -1):
                sample = make_demo_load(cluster.name, self._rng)
                if cluster.load_partitions:
                    sample = [p for p in sample if p["partition"] in cluster.load_partitions]
                self.store.add_load_samples(cluster.name, now - k * 7200, sample)
        warnings = ["sacct shows only your own jobs here (demo warning)"] if cluster.name == "dardel" else []
        return queue_all, sacct_all, sshare_all, parts, warnings
