"""Fake clusters and jobs so the dashboard can be tried without any ssh access."""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from .collector import Collector, ClusterStatus
from .config import ClusterConfig, Config
from .history import HistoryStore
from .models import Job

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
    for name, nodes, cpn, limit, default in specs:
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
            "jobs": {"running": alloc // rng.randint(1, 4) + 1, "pending": pend},
            "pending_nodes": pend * rng.randint(1, 6), "pending_cpus": pend * cpn, "running_nodes": alloc,
        })
    return out


class DemoCollector(Collector):
    """A Collector that fabricates data instead of talking to clusters."""

    def __init__(self, config: Config, history: HistoryStore, seed: int = 1):
        super().__init__(config, history)
        self._rng = random.Random(seed)

    def connected(self, cluster: ClusterConfig) -> bool | None:
        return cluster.name != "offline-cluster"

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
        status.partitions = make_demo_load(cluster.name, self._rng)
        from .slurm import summarize_load

        status.load = summarize_load(status.partitions)
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
            ClusterConfig(name="tetralith", host="tetralith.nsc.liu.se", color="#5f9e99"),
            ClusterConfig(name="dardel", host="dardel.pdc.kth.se", color="#e2856c"),
            ClusterConfig(name="lumi", host="lumi.csc.fi", color="#b39a4b"),
            ClusterConfig(name="offline-cluster", host="unreachable.example.org", color="#8fb8b4"),
        ],
        refresh_seconds=30,
    )
