"""Data model shared by the collector, history store and HTTP API."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

_ARRAY_ID = re.compile(r"^(?P<base>\d+)_(?P<task>\d+|\[(?P<spec>[^\]]*)\])$")


def array_info(job_id: str) -> tuple[str | None, int]:
    """(array job id, number of tasks this row stands for).

    ``1234`` -> (None, 1); ``1234_7`` -> ("1234", 1); ``1234_[5-100%4]`` -> ("1234", 96).
    """
    m = _ARRAY_ID.match(job_id or "")
    if not m:
        return None, 1
    spec = m.group("spec")
    if spec is None:
        return m.group("base"), 1
    total = 0
    for chunk in spec.split("%")[0].split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            lo, hi = chunk.split("-", 1)
            try:
                total += int(hi.split(":")[0]) - int(lo) + 1
            except ValueError:
                total += 1
        else:
            total += 1
    return m.group("base"), max(total, 1)

# Slurm states grouped the way the dashboard shows them.
ACTIVE_STATES = {"RUNNING", "COMPLETING", "CONFIGURING", "STAGE_OUT", "SIGNALING"}
PENDING_STATES = {"PENDING", "SUSPENDED", "REQUEUED", "REQUEUE_HOLD", "RESIZING", "REQUEUE_FED", "REVOKED"}
OK_STATES = {"COMPLETED"}
PROBLEM_STATES = {
    "FAILED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "CANCELLED",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "SPECIAL_EXIT",
}
TERMINAL_STATES = OK_STATES | PROBLEM_STATES


def normalize_state(raw: str) -> str:
    """Map Slurm's free-form state strings onto a fixed vocabulary.

    sacct emits things like ``CANCELLED by 12345`` and squeue can emit
    abbreviations such as ``PD`` or ``CG``.
    """
    s = (raw or "").strip().upper()
    if not s:
        return "UNKNOWN"
    s = s.split()[0]
    if s.endswith("+"):  # sacct marks jobs with steps in other states as e.g. "FAILED+"
        s = s[:-1]
    abbreviations = {
        "PD": "PENDING",
        "R": "RUNNING",
        "CG": "COMPLETING",
        "CD": "COMPLETED",
        "F": "FAILED",
        "TO": "TIMEOUT",
        "OOM": "OUT_OF_MEMORY",
        "NF": "NODE_FAIL",
        "CA": "CANCELLED",
        "PR": "PREEMPTED",
        "S": "SUSPENDED",
        "CF": "CONFIGURING",
        "BF": "BOOT_FAIL",
        "DL": "DEADLINE",
        "RQ": "REQUEUED",
        "RH": "REQUEUE_HOLD",
        "SE": "SPECIAL_EXIT",
    }
    return abbreviations.get(s, s)


def category(state: str) -> str:
    """Coarse bucket used for colouring and tabs: running/pending/ok/problem/unknown."""
    if state in ACTIVE_STATES:
        return "running"
    if state in PENDING_STATES:
        return "pending"
    if state in OK_STATES:
        return "ok"
    if state in PROBLEM_STATES:
        return "problem"
    return "unknown"


@dataclass
class Job:
    cluster: str
    job_id: str
    name: str
    state: str
    user: str = ""
    partition: str = ""
    account: str = ""
    nodes: int = 0
    cpus: int = 0
    gpus: int = 0  # Slurm GPU units allocated (or requested while pending); 0 for CPU jobs
    node_list: str = ""
    reason: str = ""
    exit_code: str = ""
    elapsed_s: int | None = None
    time_limit_s: int | None = None
    submit_time: str = ""  # ISO-like strings exactly as the cluster reports them
    start_time: str = ""
    end_time: str = ""
    work_dir: str = ""
    source: str = ""  # squeue | sacct | history
    last_seen: float = 0.0  # unix time when we last saw this job on the cluster
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.cluster}:{self.job_id}"

    @property
    def category(self) -> str:
        return category(self.state)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["key"] = self.key
        d["category"] = self.category
        d["terminal"] = self.is_terminal
        d["array_job_id"], d["array_tasks"] = array_info(self.job_id)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Job":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in d.items() if k in known})
