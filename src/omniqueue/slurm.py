"""Build the squeue/sacct commands and parse their output into Job objects.

Both commands are asked for pipe-separated output with the job name as the
*last* field, so a job name containing ``|`` cannot shift the other columns.
"""

from __future__ import annotations

import re
import shlex
import time
from datetime import datetime, timedelta

from .models import Job, normalize_state

# squeue -o format codes. %j (name) must stay last, see module docstring.
SQUEUE_FIELDS = [
    ("%i", "job_id"),
    ("%T", "state"),
    ("%u", "user"),
    ("%P", "partition"),
    ("%a", "account"),
    ("%D", "nodes"),
    ("%C", "cpus"),
    ("%N", "node_list"),
    ("%r", "reason"),
    ("%M", "elapsed"),
    ("%l", "time_limit"),
    ("%V", "submit_time"),
    ("%S", "start_time"),
    ("%Z", "work_dir"),
    ("%j", "name"),
]

SACCT_FIELDS = [
    "JobID",
    "State",
    "User",
    "Partition",
    "Account",
    "NNodes",
    "NCPUS",
    "NodeList",
    "Reason",
    "Elapsed",
    "Timelimit",
    "Submit",
    "Start",
    "End",
    "ExitCode",
    "WorkDir",
    "JobName",  # must stay last
]

SEP = "|"


def _user_arg(user: str | None) -> str:
    # $USER is expanded by the remote shell when no explicit user is configured.
    return shlex.quote(user) if user else '"$USER"'


def squeue_command(user: str | None, extra_args: list[str] | None = None) -> str:
    fmt = SEP.join(code for code, _ in SQUEUE_FIELDS)
    parts = ["squeue", "--noheader", "--array", f"--user={_user_arg(user)}", f"--format={shlex.quote(fmt)}"]
    parts += [shlex.quote(a) for a in (extra_args or [])]
    return " ".join(parts)


def sacct_command(user: str | None, lookback_hours: int, extra_args: list[str] | None = None) -> str:
    start = (datetime.now() - timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%S")
    parts = [
        "sacct",
        "--noheader",
        "--parsable2",
        "--allocations",
        f"--user={_user_arg(user)}",
        f"--starttime={start}",
        "--endtime=now",
        f"--format={','.join(SACCT_FIELDS)}",
    ]
    parts += [shlex.quote(a) for a in (extra_args or [])]
    return " ".join(parts)


_DURATION_RE = re.compile(r"^(?:(?P<days>\d+)-)?(?:(?P<h>\d+):)?(?P<m>\d+):(?P<s>\d+)(?:\.\d+)?$")


def parse_duration(text: str) -> int | None:
    """Parse Slurm durations like ``1-02:03:04``, ``02:03:04``, ``03:04``, ``UNLIMITED``.

    Returns seconds, or None when the value is not a duration.
    """
    t = (text or "").strip()
    if not t or t in {"UNLIMITED", "INVALID", "N/A", "NONE", "Partition_Limit", "Unknown", "None"}:
        return None
    m = _DURATION_RE.match(t)
    if not m:
        return None
    days = int(m.group("days") or 0)
    hours = int(m.group("h") or 0)
    return days * 86400 + hours * 3600 + int(m.group("m")) * 60 + int(m.group("s"))


def _int(text: str) -> int:
    try:
        return int(text.strip())
    except (ValueError, AttributeError):
        return 0


def _clean_time(text: str) -> str:
    t = (text or "").strip()
    return "" if t in {"N/A", "Unknown", "None", "NONE"} else t


def parse_squeue(output: str, cluster: str, now: float | None = None) -> list[Job]:
    now = now or time.time()
    n = len(SQUEUE_FIELDS)
    jobs: list[Job] = []
    for line in output.splitlines():
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = line.split(SEP, n - 1)
        if len(parts) < n:
            continue  # not one of our lines (e.g. a warning printed to stdout)
        rec = {key: value for (_, key), value in zip(SQUEUE_FIELDS, parts)}
        reason = rec["reason"].strip()
        if reason in {"None", "N/A"}:
            reason = ""
        jobs.append(
            Job(
                cluster=cluster,
                job_id=rec["job_id"].strip(),
                name=rec["name"].strip(),
                state=normalize_state(rec["state"]),
                user=rec["user"].strip(),
                partition=rec["partition"].strip(),
                account=rec["account"].strip(),
                nodes=_int(rec["nodes"]),
                cpus=_int(rec["cpus"]),
                node_list=rec["node_list"].strip(),
                reason=reason,
                elapsed_s=parse_duration(rec["elapsed"]),
                time_limit_s=parse_duration(rec["time_limit"]),
                submit_time=_clean_time(rec["submit_time"]),
                start_time=_clean_time(rec["start_time"]),
                work_dir=rec["work_dir"].strip(),
                source="squeue",
                last_seen=now,
            )
        )
    return jobs


def parse_sacct(output: str, cluster: str, now: float | None = None) -> list[Job]:
    now = now or time.time()
    n = len(SACCT_FIELDS)
    jobs: list[Job] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split(SEP, n - 1)
        if len(parts) < n:
            continue
        rec = dict(zip(SACCT_FIELDS, parts))
        job_id = rec["JobID"].strip()
        if "." in job_id:  # a job step slipped through despite --allocations
            continue
        raw_state = rec["State"].strip()
        reason = rec["Reason"].strip()
        if reason in {"None", "N/A"}:
            reason = ""
        if raw_state.upper().startswith("CANCELLED BY"):
            reason = f"cancelled by uid {raw_state.split()[-1]}" if not reason else reason
        jobs.append(
            Job(
                cluster=cluster,
                job_id=job_id,
                name=rec["JobName"].strip(),
                state=normalize_state(raw_state),
                user=rec["User"].strip(),
                partition=rec["Partition"].strip(),
                account=rec["Account"].strip(),
                nodes=_int(rec["NNodes"]),
                cpus=_int(rec["NCPUS"]),
                node_list=rec["NodeList"].strip() if rec["NodeList"].strip() != "None assigned" else "",
                reason=reason,
                exit_code=rec["ExitCode"].strip(),
                elapsed_s=parse_duration(rec["Elapsed"]),
                time_limit_s=parse_duration(rec["Timelimit"]),
                submit_time=_clean_time(rec["Submit"]),
                start_time=_clean_time(rec["Start"]),
                end_time=_clean_time(rec["End"]),
                work_dir=rec["WorkDir"].strip(),
                source="sacct",
                last_seen=now,
            )
        )
    return jobs


def describe_exit(job: Job) -> str:
    """Human-readable failure summary: ``exit 1``, ``signal 9 (OOM)``, ``time limit`` ..."""
    if job.state == "TIMEOUT":
        return "hit time limit"
    if job.state == "OUT_OF_MEMORY":
        return "out of memory"
    if job.state == "NODE_FAIL":
        return "node failure"
    if job.state == "CANCELLED":
        return job.reason or "cancelled"
    if job.state == "PREEMPTED":
        return "preempted"
    if not job.exit_code or ":" not in job.exit_code:
        return ""
    code, sig = job.exit_code.split(":", 1)
    if sig not in ("", "0"):
        return f"killed by signal {sig}"
    if code not in ("", "0"):
        return f"exit code {code}"
    return ""


def merge_jobs(squeue_jobs: list[Job], sacct_jobs: list[Job]) -> list[Job]:
    """Combine both sources for one cluster.

    squeue is authoritative for anything it lists (it is real time). sacct
    fills in finished jobs and anything squeue does not know about.
    """
    by_id: dict[str, Job] = {j.job_id: j for j in sacct_jobs}
    for j in squeue_jobs:
        prev = by_id.get(j.job_id)
        if prev is not None:
            # keep accounting-only details (exit code, end time) that squeue lacks
            if not j.exit_code:
                j.exit_code = prev.exit_code
            if not j.end_time:
                j.end_time = prev.end_time
            if not j.account:
                j.account = prev.account
        by_id[j.job_id] = j
    return list(by_id.values())
