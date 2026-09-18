"""Experimental: where would a job start fastest?

A stand-alone heuristic with no I/O: :func:`predict` takes a request and the plain
data the project poller collected (:meth:`ProjectPoller.prediction_data`) and ranks
(cluster, partition, project) candidates by an estimated queue wait.

The estimate is deliberately simple and every factor is reported, so it can be
checked against reality and tuned:

* **free now** - the fraction of recent load samples in which at least the requested
  number of nodes was idle (a proxy for "backfill would start me right away").
* **queue pressure** - pending nodes ahead of you relative to the partition size,
  turned into hours with the typical run time of jobs on that partition.
* **fairshare** - your (or the account's) Slurm fairshare factor: 1.0 means you go to
  the front of the pending list, 0.0 to the back.
* **nice** - the ``--nice`` you usually submit with on that cluster (lower priority).
* **history** - the median wait your own similar-sized jobs actually had there.
* **quota** - a project with too few core-hours (GPU-hours for a GPU job) left is
  excluded or flagged.

GPU jobs (``gpus > 0``) are only matched against GPU partitions, CPU jobs only
against CPU partitions, unless partitions are named explicitly.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any

# tunables --------------------------------------------------------------------------------
RECENT_DAYS = 7  # load samples considered for "free now"
HISTORY_DAYS = 30  # own queue waits considered
NICE_HALF_LIFE = 5000  # a --nice of this size doubles the estimated wait
DEFAULT_TYPICAL_HOURS = 6.0  # run time assumed for the queue when the project data has none
STALE_FACTOR = 2.5  # a latest sample older than this x the poll interval counts as stale


@dataclass
class Request:
    nodes: int = 1
    hours: float = 1.0
    cores: int | None = None  # total cores wanted; None = whole nodes
    gpus: int = 0  # GPUs per node; > 0 restricts the search to GPU partitions, 0 to CPU partitions
    projects: list[str] | None = None  # restrict to these Slurm accounts
    clusters: list[str] | None = None  # restrict to these clusters
    partitions: list[str] | None = None  # restrict to these partitions


@dataclass
class Candidate:
    cluster: str
    partition: str
    project: str | None
    estimated_wait_h: float | None  # None when excluded
    immediate_probability: float
    confidence: str  # good | fair | low
    factors: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    excluded: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _median(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def _nice_factor(nice: int) -> float:
    return 2 ** (max(0, nice) / NICE_HALF_LIFE)


def _fairshare_factor(fs: float | None) -> float:
    """1.0 fairshare -> 0.5x the queue wait, 0.0 -> 1.5x; unknown -> 1.0."""
    if fs is None:
        return 1.0
    return 1.5 - max(0.0, min(1.0, fs))


def _confidence(n_recent: int, stale: bool, hist_n: int) -> str:
    if stale or n_recent < 3:
        return "low"
    if n_recent >= 24 or hist_n >= 3:
        return "good"
    return "fair"


def predict(request: Request, data: dict[str, Any]) -> dict[str, Any]:
    """Rank candidates; returns ``{"candidates": [...], "excluded": [...], "notes": [...]}``."""
    now = data.get("now") or 0.0
    cands: list[Candidate] = []
    excluded: list[Candidate] = []
    notes: list[str] = []
    for cname, cl in data.get("clusters", {}).items():
        if request.clusters and cname not in request.clusters:
            continue
        nice = int(cl.get("nice") or 0)
        interval = float(cl.get("interval") or 7200)
        gpu_factor = float(cl.get("gpu_factor") or 1.0)  # billed GPU-hours per Slurm GPU unit and hour
        projects = cl.get("projects") or {}
        proj_names = [p for p in projects if not request.projects or p in request.projects] or [None]
        if request.projects and proj_names == [None]:
            continue
        if not cl.get("partitions"):
            notes.append(f"{cname}: no load samples yet (the project poll collects them)")
            continue
        for pname, part in cl["partitions"].items():
            if request.partitions and pname not in request.partitions:
                continue
            is_gpu = bool(part.get("gpu"))
            if not request.partitions and is_gpu != (request.gpus > 0):
                continue  # GPU jobs only go to GPU partitions and CPU jobs only to CPU ones
            samples = [s for s in part.get("samples", []) if s.get("ts")]
            if not samples:
                continue
            latest = max(samples, key=lambda s: s["ts"])  # newest, whatever order the samples come in
            stale = (now - latest["ts"]) > STALE_FACTOR * interval
            recent = [s for s in samples if now - s["ts"] <= RECENT_DAYS * 86400] or samples[-1:]
            total_nodes = int(latest.get("total") or 0)
            cpn = int(part.get("cores_per_node") or 0)
            nodes_needed = request.nodes
            if request.cores and cpn:
                nodes_needed = max(nodes_needed, -(-request.cores // cpn))
            limit_s = part.get("time_limit_s")
            base_reasons: list[str] = []
            if limit_s and request.hours * 3600 > limit_s:
                for proj in proj_names:
                    excluded.append(Candidate(cname, pname, proj, None, 0.0, "good",
                                              excluded=f"time limit {limit_s / 3600:g} h is shorter than {request.hours:g} h"))
                continue
            if total_nodes and nodes_needed > total_nodes:
                for proj in proj_names:
                    excluded.append(Candidate(cname, pname, proj, None, 0.0, "good",
                                              excluded=f"only {total_nodes} nodes in the partition"))
                continue
            gpn = int(part.get("gpus_per_node") or 0)
            if request.gpus and gpn and request.gpus > gpn:
                for proj in proj_names:
                    excluded.append(Candidate(cname, pname, proj, None, 0.0, "good",
                                              excluded=f"only {gpn} GPUs per node"))
                continue
            free_hits = sum(1 for s in recent if (s.get("idle") or 0) >= nodes_needed)
            p_free = free_hits / len(recent)
            idle_now = int(latest.get("idle") or 0)
            pending_nodes = int(latest.get("pending_nodes") or 0)
            pressure = pending_nodes / total_nodes if total_nodes else 0.0
            typical_h = part.get("typical_hours") or DEFAULT_TYPICAL_HOURS
            queue_wait_h = pressure * typical_h
            # your own record on this partition, similar sizes only
            waits = [w["wait_s"] / 3600 for w in cl.get("own_waits", [])
                     if w.get("partition") == pname and now - (w.get("start_ts") or 0) <= HISTORY_DAYS * 86400
                     and nodes_needed / 2 <= (w.get("nodes") or 1) <= nodes_needed * 2]
            hist_wait_h = _median(waits)
            if idle_now >= nodes_needed:
                base_reasons.append(f"{idle_now} nodes idle right now (need {nodes_needed})")
            elif pending_nodes:
                base_reasons.append(f"{pending_nodes} nodes requested ahead in the queue")
            if hist_wait_h is not None:
                base_reasons.append(f"your {len(waits)} similar jobs waited {hist_wait_h:.1f} h on median")
            for proj in proj_names:
                pinfo = projects.get(proj, {}) if proj else {}
                fs = pinfo.get("fairshare_me")
                fs_source = "your fairshare"
                if fs is None:
                    fs = pinfo.get("fairshare_account")
                    fs_source = "project fairshare"
                fs_factor = _fairshare_factor(fs)
                nice_factor = _nice_factor(nice)
                model_wait = (1.0 - p_free) * queue_wait_h * fs_factor * nice_factor
                if hist_wait_h is not None:
                    weight = 0.5 if len(waits) >= 3 else 0.3
                    est = weight * hist_wait_h + (1 - weight) * model_wait
                else:
                    est = model_wait
                reasons = list(base_reasons)
                if fs is not None:
                    reasons.append(f"{fs_source} {fs:.2f}")
                if nice:
                    reasons.append(f"--nice {nice} (x{nice_factor:.2f})")
                exclusion = None
                if request.gpus:
                    quota, unit, need = pinfo.get("gpu_quota") if proj else None, "GPU-h", request.gpus * nodes_needed * request.hours * gpu_factor
                else:
                    quota, unit, need = pinfo.get("quota") if proj else None, "core-h", (request.cores or nodes_needed * (cpn or 1)) * request.hours
                if quota and quota.get("limit_h"):
                    remaining = quota["limit_h"] - quota.get("used_h", 0)
                    if remaining < need:
                        exclusion = f"quota: {remaining:,.0f} {unit} left, job needs {need:,.0f}"
                    elif remaining < 3 * need:
                        reasons.append(f"quota nearly used: {remaining:,.0f} {unit} left")
                        est *= 1.05
                if stale:
                    reasons.append("load sample is stale")
                cand = Candidate(
                    cname, pname, proj, round(est, 2), round(p_free, 2),
                    _confidence(len(recent), stale, len(waits)),
                    factors={"idle_now": idle_now, "pending_nodes": pending_nodes, "total_nodes": total_nodes,
                             "pressure": round(pressure, 3), "typical_hours": round(typical_h, 1), "p_free": round(p_free, 2),
                             "fairshare": fs, "fairshare_factor": round(fs_factor, 2), "nice": nice,
                             "nice_factor": round(nice_factor, 2), "history_wait_h": hist_wait_h,
                             "history_n": len(waits), "samples": len(recent), "nodes_needed": nodes_needed,
                             "model_wait_h": round(model_wait, 2), "gpu": is_gpu, "gpus_per_node": gpn},
                    reasons=reasons, excluded=exclusion)
                (excluded if exclusion else cands).append(cand)
    cands.sort(key=lambda c: (c.estimated_wait_h, -c.immediate_probability, c.cluster, c.partition))
    return {"request": request.__dict__, "candidates": [c.to_dict() for c in cands],
            "excluded": [c.to_dict() for c in excluded], "notes": notes}


def explain(result: dict[str, Any], limit: int = 8) -> str:
    """Plain-text ranking for the terminal."""
    lines = []
    req = result["request"]
    lines.append(f"job: {req['nodes']} node(s), {req['hours']:g} h" + (f", {req['cores']} cores" if req.get("cores") else "")
                 + (f", {req['gpus']} GPU(s) per node" if req.get("gpus") else ""))
    if not result["candidates"]:
        lines.append("no candidates: no load samples yet? (projects must be configured and polled once)")
    for i, c in enumerate(result["candidates"][:limit], 1):
        proj = f" [{c['project']}]" if c["project"] else ""
        lines.append(f"{i}. {c['cluster']}/{c['partition']}{proj}: ~{c['estimated_wait_h']:.1f} h wait, "
                     f"{c['immediate_probability'] * 100:.0f}% chance of an immediate start ({c['confidence']} confidence)")
        for r in c["reasons"]:
            lines.append(f"     - {r}")
    for c in result["excluded"][:limit]:
        proj = f" [{c['project']}]" if c["project"] else ""
        lines.append(f"x  {c['cluster']}/{c['partition']}{proj}: {c['excluded']}")
    for n in result["notes"]:
        lines.append(f"note: {n}")
    return "\n".join(lines)
