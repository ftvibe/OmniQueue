"""Your own usage per project, computed from the local job history (no cluster access).

CPU jobs are counted in core-hours, GPU jobs (any GPU allocated, or a GPU partition)
in GPU-hours; the two never mix.  Everything comes from the jobs OmniQueue already
polls for you, so this works without watching whole projects.
"""

from __future__ import annotations

import time
from typing import Any

from .config import ClusterConfig, Config
from .models import Job

ACTIVE = {"RUNNING", "PENDING", "COMPLETING", "CONFIGURING", "SUSPENDED", "REQUEUED"}
WINDOWS_DAYS = (7, 30)
DAILY_DAYS = 30


def _ts(text: str) -> float | None:
    if not text:
        return None
    try:
        return time.mktime(time.strptime(text[:19], "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


def _interval(job: Job, now: float) -> tuple[float, float] | None:
    start = _ts(job.start_time)
    if start is None or job.state == "PENDING":
        return None
    if job.state in ACTIVE:
        end = now
    else:
        end = _ts(job.end_time) or (start + (job.elapsed_s or 0))
    return start, max(start, end)


def own_usage(config: Config, jobs_by_cluster: dict[str, list[Job]], now: float | None = None,
              tpc: dict[str, dict[str, int]] | None = None, learned_gpu_partitions: dict[str, set[str]] | None = None,
              gpus_per_node: dict[str, dict[str, int]] | None = None) -> list[dict[str, Any]]:
    """One record per (cluster, account) you have jobs in, newest activity first.

    `gpus_per_node` (cluster -> partition -> GPUs per node, from sinfo) marks GPU
    partitions and sizes whole-node jobs that never asked for a gres."""
    now = now or time.time()
    out: list[dict[str, Any]] = []
    clusters: dict[str, ClusterConfig] = {c.name: c for c in config.enabled_clusters}
    for cname, jobs in jobs_by_cluster.items():
        cfg = clusters.get(cname)
        if cfg is None:
            continue
        gpn = (gpus_per_node or {}).get(cname, {})
        gpu_parts = set(cfg.gpu_partitions) | set((learned_gpu_partitions or {}).get(cname, set()))
        gpu_parts.update(part for part, n in gpn.items() if n > 0)
        gpu_parts.update(j.partition for j in jobs if j.gpus and j.partition)
        tpc_map = (tpc or {}).get(cname, {})
        factor = cfg.gpu_hour_factor

        def cores(j: Job) -> float:
            return (j.cpus or 0) / max(1, tpc_map.get(j.partition, 1))

        def kind(j: Job) -> str:
            return "gpu" if j.gpus or j.partition in gpu_parts else "cpu"

        def gpus(j: Job) -> int:
            """Slurm GPU units of a job; a whole-node job on a GPU partition without a gres gets the node's GPUs."""
            if j.gpus:
                return j.gpus
            return (j.nodes or 1) * gpn.get(j.partition, 0) if j.partition in gpu_parts else 0

        by_account: dict[str, list[Job]] = {}
        for j in jobs:
            by_account.setdefault(j.account or "?", []).append(j)
        for account, ajobs in by_account.items():
            ivs = [(j, iv) for j in ajobs if (iv := _interval(j, now))]

            def bucket() -> dict:
                return {"cpu": {"core_h": 0.0, "jobs": 0}, "gpu": {"gpu_h": 0.0, "core_h": 0.0, "jobs": 0}}

            def add(b: dict, j: Job, overlap: float) -> None:
                if kind(j) == "gpu":
                    b["gpu"]["gpu_h"] += overlap * gpus(j) * factor / 3600
                    b["gpu"]["core_h"] += overlap * cores(j) / 3600
                    b["gpu"]["jobs"] += 1
                else:
                    b["cpu"]["core_h"] += overlap * cores(j) / 3600
                    b["cpu"]["jobs"] += 1

            usage: dict[str, dict] = {}
            for days in WINDOWS_DAYS:
                w0 = now - days * 86400
                b = bucket()
                for j, (s0, e0) in ivs:
                    ov = max(0.0, min(e0, now) - max(s0, w0))
                    if ov > 0:
                        add(b, j, ov)
                usage[str(days)] = b
            lt = time.localtime(now)
            midnight = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1))
            daily = []
            for i in range(DAILY_DAYS - 1, -1, -1):
                d0 = midnight - i * 86400
                d1 = min(now, d0 + 86400)
                b = bucket()
                for j, (s0, e0) in ivs:
                    ov = max(0.0, min(e0, d1) - max(s0, d0))
                    if ov > 0:
                        add(b, j, ov)
                daily.append({"date": time.strftime("%m-%d", time.localtime(d0)), "core_h": b["cpu"]["core_h"], "gpu_h": b["gpu"]["gpu_h"]})
            running = {"cpu": {"jobs": 0, "cores": 0.0, "nodes": 0}, "gpu": {"jobs": 0, "gpus": 0, "cores": 0.0, "nodes": 0}}
            pending = {"cpu": {"jobs": 0, "cores": 0.0}, "gpu": {"jobs": 0, "gpus": 0, "cores": 0.0}}
            for j in ajobs:
                target = running if j.state == "RUNNING" else pending if j.state == "PENDING" else None
                if target is None:
                    continue
                k = kind(j)
                target[k]["jobs"] += 1
                target[k]["cores"] += cores(j)
                if k == "gpu":
                    target[k]["gpus"] += gpus(j)
                if "nodes" in target[k]:
                    target[k]["nodes"] += j.nodes or 0
            has_gpu = bool(gpu_parts) or usage["30"]["gpu"]["jobs"] > 0 or running["gpu"]["jobs"] > 0 or pending["gpu"]["jobs"] > 0
            starts = [s0 for _, (s0, _) in ivs]
            out.append({
                "cluster": cname, "account": account, "pi": cfg.project_pis.get(account),
                "usage": usage, "daily": daily, "running": running, "pending": pending,
                "has_gpu": has_gpu, "gpu_factor": factor, "gpu_partitions": sorted(gpu_parts),
                "gpus_per_node": {p: n for p, n in gpn.items() if n > 0},
                "jobs_known": len(ajobs), "oldest": min(starts) if starts else None,
                "quota": _quota(cfg.project_quotas.get(account), usage["30"]["cpu"]["core_h"]),
                "gpu_quota": _quota(cfg.project_gpu_quotas.get(account), usage["30"]["gpu"]["gpu_h"]),
                "last_activity": max([e0 for _, (_, e0) in ivs], default=0),
            })
    out.sort(key=lambda r: (-(r["running"]["cpu"]["jobs"] + r["running"]["gpu"]["jobs"]), -r["last_activity"], r["cluster"], r["account"]))
    return out


def _quota(limit: float | None, used: float) -> dict | None:
    if not limit:
        return None
    return {"limit_h": float(limit), "used_h": used, "source": "config", "window": "30 d", "fraction": min(1.0, used / limit)}
