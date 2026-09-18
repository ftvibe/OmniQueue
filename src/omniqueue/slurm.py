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


MARK = "@@OMNIQUEUE"


# cluster load: node states per partition and queue pressure from all users
SINFO_FIELDS = "%P|%a|%D|%T|%C|%l|%z|%c|%G"  # partition, avail, nodes, state, cpus A/I/O/T, time limit, S:C:T, cpus/node, gres
SQUEUE_ALL_FIELDS = "%i|%P|%T|%D|%C"  # job id (arrays as ranges), partition, state, nodes, cpus (every user)


def _partition_arg(partitions: list[str] | None) -> str:
    return f" --partition={shlex.quote(','.join(partitions))}" if partitions else ""


def sinfo_command(partitions: list[str] | None = None) -> str:
    return f"sinfo --noheader --format={shlex.quote(SINFO_FIELDS)}{_partition_arg(partitions)}"


def squeue_all_command(partitions: list[str] | None = None) -> str:
    return (f"squeue --noheader --states=RUNNING,PENDING --format={shlex.quote(SQUEUE_ALL_FIELDS)}"
            f"{_partition_arg(partitions)}")


def combined_command(user: str | None, lookback_hours: int, squeue_args: list[str] | None,
                     sacct_args: list[str] | None, use_sacct: bool) -> str:
    """squeue and sacct in one remote shell invocation, so a poll costs one ssh round trip.

    Each command is followed by a marker line carrying its exit status.
    """
    parts = [squeue_command(user, squeue_args), f'echo "{MARK} squeue rc=$?"']
    if use_sacct:
        parts += [sacct_command(user, lookback_hours, sacct_args), f'echo "{MARK} sacct rc=$?"']
    return "; ".join(parts)


def load_command(partitions: list[str] | None = None) -> str:
    """sinfo plus an all-users squeue, fetched on demand for the cluster load view."""
    return "; ".join([sinfo_command(partitions), f'echo "{MARK} sinfo rc=$?"',
                      squeue_all_command(partitions), f'echo "{MARK} squeue_all rc=$?"'])


# sinfo node states -> the four buckets the dashboard shows
_STATE_BUCKET = {
    "idle": "idle",
    "mixed": "mixed",
    "allocated": "allocated",
    "completing": "allocated",
    "planned": "idle",
    "reserved": "unavailable",
    "down": "unavailable",
    "drained": "unavailable",
    "draining": "allocated",
    "fail": "unavailable",
    "failing": "unavailable",
    "maint": "unavailable",
    "future": "unavailable",
    "unknown": "unavailable",
    "inval": "unavailable",
    "perfctrs": "unavailable",
    "power_down": "unavailable",
    "powered_down": "unavailable",
    "powering_down": "unavailable",
    "powering_up": "idle",
    "no_respond": "unavailable",
}


def _bucket(state: str) -> str:
    base = state.strip().lower().rstrip("*~#!%$@^-+")
    for flag in ("+cloud", "+drain", "+maint", "+reserved"):
        base = base.replace(flag, "")
    return _STATE_BUCKET.get(base, "unavailable")


def parse_load(sinfo_out: str, squeue_all_out: str) -> list[dict]:
    """Combine sinfo rows and an all-users squeue into one record per partition."""
    parts: dict[str, dict] = {}

    def part(name: str) -> dict:
        return parts.setdefault(name, {
            "partition": name, "default": False, "avail": "up", "time_limit_s": None,
            "nodes": {"idle": 0, "mixed": 0, "allocated": 0, "unavailable": 0, "total": 0},
            "cpus": {"allocated": 0, "idle": 0, "other": 0, "total": 0},
            "threads_per_core": 1,  # >1 when Slurm counts hyperthreads as CPUs (e.g. LUMI: 2)
            "cpus_per_node": 0,  # largest node size seen in the partition (Slurm CPUs)
            "gpus_per_node": 0,  # from the gres column (gpu:4, gpu:a100:4 ...); 0 = a CPU partition
            "gpu": False,
            "cores": {"allocated": 0, "idle": 0, "other": 0, "total": 0},
            "jobs": {"running": 0, "pending": 0},
            "pending_nodes": 0, "pending_cpus": 0, "running_nodes": 0,
        })

    for line in sinfo_out.splitlines():
        cols = line.split("|")
        if len(cols) < 6:
            continue
        raw_name, avail, nodes, state, cpus, limit = (c.strip() for c in cols[:6])
        sct = cols[6].strip() if len(cols) > 6 else ""
        cpn = cols[7].strip() if len(cols) > 7 else ""
        gres = cols[8].strip() if len(cols) > 8 else ""
        name = raw_name.rstrip("*")
        p = part(name)
        p["cpus_per_node"] = max(p["cpus_per_node"], _int(cpn.rstrip("+")))
        p["gpus_per_node"] = max(p["gpus_per_node"], gpus_from_gres(gres))
        p["gpu"] = p["gpus_per_node"] > 0
        try:  # %z is sockets:cores:threads; the thread count says what a Slurm CPU is
            tpc = int(sct.split(":")[2])
            if tpc > 1:
                p["threads_per_core"] = max(p["threads_per_core"], tpc)
        except (IndexError, ValueError):
            pass
        p["default"] = p["default"] or raw_name.endswith("*")
        p["avail"] = avail or p["avail"]
        p["time_limit_s"] = parse_duration(limit) if p["time_limit_s"] is None else p["time_limit_s"]
        n = _int(nodes)
        p["nodes"][_bucket(state)] += n
        p["nodes"]["total"] += n
        try:
            a, i, o, t = (int(x) for x in cpus.split("/"))
        except ValueError:
            a = i = o = t = 0
        if _bucket(state) == "unavailable":
            o += a + i  # cpus on down/drained nodes are not usable
            a = i = 0
        p["cpus"]["allocated"] += a
        p["cpus"]["idle"] += i
        p["cpus"]["other"] += o
        p["cpus"]["total"] += t

    for p in parts.values():
        tpc = p["threads_per_core"]
        p["cores"] = {k: v // tpc for k, v in p["cpus"].items()}

    for line in squeue_all_out.splitlines():
        cols = line.split("|")
        if len(cols) < 5:
            continue
        job_id, raw_name, state, nodes, cpus = (c.strip() for c in cols[:5])
        count = array_task_count(job_id)  # a pending array shows as one row: count its tasks
        for name in raw_name.split(","):  # a job may list several partitions
            p = part(name.rstrip("*"))
            st = normalize_state(state)
            if st == "RUNNING":
                p["jobs"]["running"] += 1
                p["running_nodes"] += _int(nodes)
            elif st == "PENDING":
                # `-n 512` without `-N` reports 1 node: estimate nodes from the CPU request too
                by_cpus = -(-_int(cpus) // p["cpus_per_node"]) if p["cpus_per_node"] else 0
                p["jobs"]["pending"] += count
                p["pending_nodes"] += max(_int(nodes), by_cpus) * count
                p["pending_cpus"] += _int(cpus) * count
    for p in parts.values():
        p["pending_cores"] = p["pending_cpus"] // p["threads_per_core"]
        p["gpus"] = {"total": p["nodes"]["total"] * p["gpus_per_node"],
                     "idle": p["nodes"]["idle"] * p["gpus_per_node"]}  # whole idle nodes only; mixed nodes unknown

    out = list(parts.values())
    out.sort(key=lambda p: (not p["default"], p["partition"]))
    return out


_GRES_GPU_RE = re.compile(r"(?:gres/)?gpu(?::[A-Za-z0-9_.-]+)?[:=](?P<n>\d+)")


def gpus_from_gres(text: str) -> int:
    """GPUs per node from a gres string: ``gpu:4``, ``gpu:a100:4``, ``gres/gpu:4``,
    ``gpu:a100:4(S:0-1)``, ``gpu:4,mps:400`` -> 4; ``(null)``, ``N/A`` -> 0."""
    if not text:
        return 0
    return max((int(m.group("n")) for m in _GRES_GPU_RE.finditer(text)), default=0)


def gpus_from_tres(text: str) -> int:
    """Allocated GPUs of a job from an AllocTRES string such as
    ``billing=128,cpu=128,gres/gpu=4,gres/gpu:a100=4,mem=200G,node=1``.
    The typeless ``gres/gpu=`` entry is authoritative; typed entries are summed otherwise."""
    if not text:
        return 0
    typed = 0
    for chunk in text.split(","):
        key, sep, val = chunk.partition("=")
        if not sep or not key.startswith("gres/gpu"):
            continue
        try:
            n = int(float(val))
        except ValueError:
            continue
        if key == "gres/gpu":
            return n
        typed += n
    return typed


_ARRAY_RE = re.compile(r"_\[(?P<spec>[^\]]+)\]$")


def array_task_count(job_id: str) -> int:
    """Number of tasks a squeue job id stands for: ``123`` -> 1, ``123_7`` -> 1,
    ``123_[1-100]`` -> 100, ``123_[1-10,20-25%4]`` -> 16."""
    m = _ARRAY_RE.search(job_id)
    if not m:
        return 1
    spec = m.group("spec").split("%")[0]  # drop a throttle like %4
    total = 0
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            lo, hi = chunk.split("-", 1)
            hi = hi.split(":")[0]  # a step such as 1-10:2 is rare; count the range
            try:
                total += int(hi) - int(lo) + 1
            except ValueError:
                total += 1
        else:
            total += 1
    return max(total, 1)


def summarize_load(partitions: list[dict]) -> dict:
    """Whole-cluster numbers from the partition records (nodes may appear in several
    partitions, so this is an upper bound; it is fine for a gauge)."""
    total = sum(p["cpus"]["total"] for p in partitions)
    alloc = sum(p["cpus"]["allocated"] for p in partitions)
    usable = total - sum(p["cpus"]["other"] for p in partitions)
    return {
        "cpus_total": total,
        "cpus_allocated": alloc,
        "cores_total": sum(p["cores"]["total"] for p in partitions),
        "cores_allocated": sum(p["cores"]["allocated"] for p in partitions),
        "threads_per_core": max([p["threads_per_core"] for p in partitions] or [1]),
        "utilisation": (alloc / usable) if usable else None,
        "nodes_idle": sum(p["nodes"]["idle"] for p in partitions),
        "nodes_total": sum(p["nodes"]["total"] for p in partitions),
        "jobs_running": sum(p["jobs"]["running"] for p in partitions),
        "jobs_pending": sum(p["jobs"]["pending"] for p in partitions),
    }


_MARK_RE = re.compile(rf"^{re.escape(MARK)} (?P<name>\w+) rc=(?P<rc>\d+)\s*$")


def split_combined_output(stdout: str) -> dict[str, tuple[str, int]]:
    """Split the combined command's stdout into ``{name: (output, exit_code)}``."""
    sections: dict[str, tuple[str, int]] = {}
    buf: list[str] = []
    for line in stdout.splitlines():
        m = _MARK_RE.match(line)
        if m:
            sections[m.group("name")] = ("\n".join(buf), int(m.group("rc")))
            buf = []
        else:
            buf.append(line)
    return sections


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


# ---- project usage (slow poll, all users of a project) --------------------------------
# squeue for every job of the configured projects: id, account, user, state, partition,
# nodes, cpus, time limit, elapsed.  sacct for the finished/running ones; CPUTimeRAW is
# elapsed x allocated CPUs in seconds (Slurm CPUs: threads on hyperthreaded clusters).
PROJECT_SQUEUE_FIELDS = "%i|%a|%u|%T|%P|%D|%C|%l|%M|%b|%j"  # %b: gres per node (gpu:4); %j (name) last
PROJECT_SACCT_FIELDS = ["JobID", "Account", "User", "Partition", "State", "AllocNodes", "AllocCPUS",
                        "ElapsedRaw", "CPUTimeRAW", "Submit", "Start", "End", "Timelimit", "AllocTRES", "ReqTRES"]
SSHARE_FIELDS = ["Account", "User", "RawShares", "NormShares", "RawUsage", "EffectvUsage", "FairShare",
                 "GrpTRESMins", "GrpTRESRaw", "TRESRunMins"]


def _slurm_time(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts))


def project_sacct_command(projects: list[str], start_ts: float, end_ts: float | None = None) -> str:
    """The projects' accounting (all users) between two unix times; the heavy part of a poll."""
    accounts = shlex.quote(",".join(projects))
    end = _slurm_time(end_ts) if end_ts else "now"
    return (f"sacct --noheader --parsable2 --allocations --allusers --accounts={accounts} "
            f"--starttime={_slurm_time(start_ts)} --endtime={end} --format={','.join(PROJECT_SACCT_FIELDS)}")


# squeue's long format can print a job's TRES, which the short %b cannot: allocated TRES for
# running jobs (gres/gpu=8), the per-node or per-job request for waiting ones (gres/gpu:8).
# Fields are padded to the given width and followed by the "|" suffix; parse_squeue_tres strips them.
SQUEUE_TRES_FORMAT = "JobID:60|,NumNodes:12|,tres-alloc:400|,tres-per-node:200|,tres-per-job:200|"


def project_queue_command(projects: list[str]) -> str:
    """Only the projects' queue (running and waiting jobs): the cheap part of a poll."""
    accounts = shlex.quote(",".join(projects))
    return "; ".join([
        f"squeue --noheader --states=RUNNING,PENDING --account={accounts} --format={shlex.quote(PROJECT_SQUEUE_FIELDS)}",
        f'echo "{MARK} squeue_proj rc=$?"',
        f"squeue --noheader --states=RUNNING,PENDING --account={accounts} --Format={shlex.quote(SQUEUE_TRES_FORMAT)}",
        f'echo "{MARK} squeue_tres rc=$?"',
    ])


def parse_squeue_tres(output: str) -> dict[str, int]:
    """job id -> GPUs (Slurm units) from the long-format squeue: allocated TRES first,
    then the per-node request times the node count, then the per-job request."""
    out: dict[str, int] = {}
    for line in output.splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 5 or not cols[0]:
            continue
        job_id, nodes, alloc, per_node, per_job = cols[:5]
        gpus = gpus_from_tres(alloc)
        if not gpus:
            gpus = gpus_from_gres(per_node) * max(1, _int(nodes))
        if not gpus:
            gpus = gpus_from_gres(per_job)
        if gpus:
            out[job_id] = gpus
    return out


def project_command(projects: list[str], start_ts: float, partitions: list[str] | None = None) -> str:
    """Everything one project poll needs, in one ssh round trip: the projects' queue,
    their accounting since `start_ts`, fairshare/usage from sshare, and a load sample
    (sinfo + all-users squeue) for the predictor.  Keep the sacct window short: that
    query is the slow one on a busy accounting database."""
    accounts = shlex.quote(",".join(projects))
    parts = [
        project_queue_command(projects),
        project_sacct_command(projects, start_ts),
        f'echo "{MARK} sacct_proj rc=$?"',
        f"sshare --noheader --parsable2 --all --accounts={accounts} --format={','.join(SSHARE_FIELDS)}",
        f'echo "{MARK} sshare rc=$?"',
        sinfo_command(partitions), f'echo "{MARK} sinfo rc=$?"',
        squeue_all_command(partitions), f'echo "{MARK} squeue_all rc=$?"',
    ]
    return "; ".join(parts)


def project_backfill_command(projects: list[str], start_ts: float, end_ts: float) -> str:
    """Only sacct, for one older chunk of history (the back-fill after the first poll)."""
    return f"{project_sacct_command(projects, start_ts, end_ts)}; echo \"{MARK} sacct_proj rc=$?\""


def parse_project_queue(output: str) -> list[dict]:
    """Rows of the projects' squeue: one dict per (array) job with a task count."""
    rows: list[dict] = []
    for line in output.splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 9 or not cols[0]:
            continue
        job_id, account, user, state, partition, nodes, cpus, limit, elapsed = cols[:9]
        gres = cols[9] if len(cols) > 9 else ""
        rows.append({
            "job_id": job_id, "account": account, "user": user, "state": normalize_state(state),
            "partition": partition, "nodes": _int(nodes), "cpus": _int(cpus),
            "time_limit_s": parse_duration(limit), "elapsed_s": parse_duration(elapsed) or 0,
            "tasks": array_task_count(job_id),
            "gpus": gpus_from_gres(gres) * max(1, _int(nodes)),  # %b is per node
            "name": "|".join(cols[10:]) if len(cols) > 10 else "",  # last field, may contain |
        })
    return rows


def parse_project_sacct(output: str) -> list[dict]:
    """Rows of the projects' sacct (allocations only) as plain dicts keyed for the store."""
    rows: list[dict] = []
    for line in output.splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 13 or not cols[0]:
            continue
        job_id, account, user, partition, state, nodes, cpus, elapsed, cputime, submit, start, end, limit = cols[:13]
        alloc_tres = cols[13] if len(cols) > 13 else ""
        req_tres = cols[14] if len(cols) > 14 else ""
        rows.append({
            "job_id": job_id, "account": account, "user": user, "partition": partition,
            "state": normalize_state(state), "nodes": _int(nodes), "cpus": _int(cpus),
            "elapsed_s": _int(elapsed), "cpu_s": _int(cputime),
            "submit": _clean_time(submit), "start": _clean_time(start), "end": _clean_time(end),
            "time_limit_s": parse_duration(limit),
            # allocated GPUs; for a job that has not started yet, the requested ones
            "gpus": gpus_from_tres(alloc_tres) or gpus_from_tres(req_tres),
        })
    return rows


def _tres(text: str) -> dict[str, int]:
    """``cpu=1200,mem=0,node=3`` -> {"cpu": 1200, ...}."""
    out: dict[str, int] = {}
    for chunk in text.split(","):
        k, sep, v = chunk.partition("=")
        if sep:
            try:
                out[k.strip()] = int(float(v))
            except ValueError:
                continue
    return out


def _float(text: str) -> float | None:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_sshare(output: str) -> list[dict]:
    """sshare rows: the account line has an empty User, user lines carry the user."""
    rows: list[dict] = []
    for line in output.splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 7 or not cols[0]:
            continue
        cols += [""] * (len(SSHARE_FIELDS) - len(cols))
        account, user, raw_shares, norm_shares, raw_usage, eff_usage, fairshare, grp_mins, grp_raw, run_mins = cols[:10]
        rows.append({
            "account": account.lstrip(" "), "user": user,
            "raw_shares": _int(raw_shares), "norm_shares": _float(norm_shares),
            "raw_usage": _int(raw_usage), "effective_usage": _float(eff_usage), "fairshare": _float(fairshare),
            "grp_tres_mins": _tres(grp_mins), "grp_tres_raw": _tres(grp_raw), "tres_run_mins": _tres(run_mins),
        })
    return rows
