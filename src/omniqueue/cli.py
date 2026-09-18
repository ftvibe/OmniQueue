"""Command line entry point: ``omniqueue serve | list | init | check``."""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

from . import __version__
from .collector import Collector
from .config import Config, ConfigError, default_config_path, load_config, permission_warnings, write_example_config
from .history import HistoryStore
from .models import Job
from .projects import ProjectPoller, ProjectStore
from .server import make_server
from .slurm import describe_exit
from .ssh import close_connection, connection_alive, last_use, login, master_pid


def fmt_duration(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    d, rem = divmod(int(seconds), 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h:02d}:{m:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"


def _load(args) -> Config:
    if getattr(args, "demo", False):
        from .demo import demo_config

        return demo_config()
    return load_config(Path(args.config) if args.config else None)


def _collector(cfg: Config, args) -> Collector:
    history = HistoryStore(cfg.data_dir / ("history-demo.json" if getattr(args, "demo", False) else "history.json"),
                           cfg.history_days)
    if getattr(args, "demo", False):
        from .demo import DemoCollector

        return DemoCollector(cfg, history)
    return Collector(cfg, history)


def _projects(cfg: Config, collector: Collector, args) -> ProjectPoller | None:
    """The slow project poller, or None when no cluster lists projects."""
    if not cfg.project_clusters:
        return None
    store = ProjectStore(cfg.data_dir / ("projects-demo.json" if getattr(args, "demo", False) else "projects.json"),
                         cfg.project_history_days)
    if getattr(args, "demo", False):
        from .demo import DemoProjectPoller

        poller = DemoProjectPoller(cfg, store, collector)
    else:
        poller = ProjectPoller(cfg, store, collector)
    # "my usage" borrows what the project poll learned: threads per core and GPU partitions
    collector.tpc_hook = lambda: {c.name: store.tpc_map(c.name) for c in cfg.enabled_clusters}
    collector.gpu_partitions_hook = lambda: {c.name: store.gpu_partitions(c.name, list(c.gpu_partitions) + list(poller.gpus_per_node(c))) for c in cfg.enabled_clusters}
    return poller


WIDGET_WIDTH, WIDGET_HEIGHT = 390, 780


def safari_widget_script(url: str, width: int = WIDGET_WIDTH, height: int = WIDGET_HEIGHT) -> str:
    """AppleScript that opens `url` in a new Safari window sized like the widget,
    docked at the right edge of the main screen."""
    return f"""
tell application "Finder" to set screenBounds to bounds of window of desktop
set screenW to item 3 of screenBounds
set x1 to screenW - {width} - 16
set y1 to 60
tell application "Safari"
    make new document with properties {{URL:"{url}"}}
    set bounds of front window to {{x1, y1, x1 + {width}, y1 + {height}}}
    activate
end tell
"""


def open_widget_window(url: str) -> bool:
    """On macOS, open the widget in a small Safari window; elsewhere let the caller fall back."""
    if sys.platform != "darwin" or not shutil.which("osascript"):
        return False
    try:
        proc = subprocess.run(["osascript", "-e", safari_widget_script(url)], capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return False
    if proc.returncode != 0:
        logging.getLogger("omniqueue").warning("could not open Safari window: %s", proc.stderr.strip())
        return False
    return True


def _perm_warnings(cfg: Config, args) -> list[str]:
    if getattr(args, "demo", False):
        return []
    return permission_warnings(Path(args.config) if args.config else default_config_path(), cfg)


# -- subcommands --------------------------------------------------------------------
def cmd_init(args) -> int:
    path = Path(args.config) if args.config else default_config_path()
    write_example_config(path, force=args.force)
    print(f"Wrote example config to {path} (mode 600)\nEdit it, then run: omniqueue check")
    return 0


def cmd_serve(args) -> int:
    cfg = _load(args)
    if args.port:
        cfg.listen_port = args.port
    if args.host:
        cfg.listen_host = args.host
    collector = _collector(cfg, args)
    collector.start()
    projects = _projects(cfg, collector, args)
    if projects:
        projects.start()
    server = make_server(collector, cfg.listen_host, cfg.listen_port, cfg.access_token, projects)
    url = f"http://{cfg.listen_host}:{server.server_address[1]}/"
    if cfg.access_token:
        url += f"?token={cfg.access_token}"
    if getattr(args, "view", "dashboard") == "widget":
        # the token cookie is set by "/?token=..." and then redirects to "/"; go straight to the widget otherwise
        url = f"http://{cfg.listen_host}:{server.server_address[1]}/widget" if not cfg.access_token else url
    for w in _perm_warnings(cfg, args):
        print(f"warning: {w}")
    names = ", ".join(c.name for c in cfg.enabled_clusters)
    view = getattr(args, "view", "dashboard")
    print(f"OmniQueue {__version__} watching {names}\n{'Widget' if view == 'widget' else 'Dashboard'}: {url}  (Ctrl-C to stop)")
    if projects:
        plist = ", ".join(f"{c.name}: {', '.join(c.projects)} (every {cfg.project_interval(c) / 3600:g} h)" for c in cfg.project_clusters)
        print(f"project usage polled in the background: {plist}")
    if view == "widget" and cfg.access_token:
        print("open /widget in that window once the token cookie is set")
    if args.open:
        if getattr(args, "view", "dashboard") == "widget" and not getattr(args, "plain", False):
            def _open():
                if not open_widget_window(url):  # not macOS / Safari unavailable: ordinary browser tab
                    webbrowser.open(url)
            threading.Timer(0.5, _open).start()
        else:
            threading.Timer(0.5, webbrowser.open, args=(url,)).start()
    if cfg.persist_connections and not getattr(args, "demo", False):
        cold = [c.name for c in cfg.enabled_clusters if not c.is_local and not connection_alive(c, cfg)]
        if cold and cfg.connect_on_poll:
            print(f"no open ssh connection yet for: {', '.join(cold)} (first poll opens one; "
                  f"if a password or 2FA code is needed run `omniqueue login` first)")
        elif cold:
            print(f"not logged in: {', '.join(cold)}  -> run `omniqueue login` to start polling them")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        collector.stop()
        if projects:
            projects.stop()
        server.server_close()
    if cfg.persist_connections and not getattr(args, "demo", False):
        hours = cfg.persist_seconds / 3600
        print(f"ssh connections stay open for up to {hours:g} h; run `omniqueue logout` to close them")
    return 0


def cmd_login(args) -> int:
    """Open (or re-open) the persistent ssh connection to clusters interactively."""
    cfg = _load(args)
    if not cfg.persist_connections:
        print("persist_connections is false in the config; nothing to keep open.", file=sys.stderr)
        return 2
    wanted = set(args.cluster or [])
    unknown = wanted - {c.name for c in cfg.clusters}
    if unknown:
        print(f"unknown cluster(s): {', '.join(sorted(unknown))}", file=sys.stderr)
        return 2
    rc = 0
    for c in cfg.enabled_clusters:
        if wanted and c.name not in wanted:
            continue
        if c.is_local:
            continue
        if args.close:
            print(f"{c.name}: {close_connection(c, cfg)}")
            continue
        if connection_alive(c, cfg) and not args.force:
            print(f"{c.name}: connection already open")
            continue
        print(f"{c.name}: connecting to {c.host} ...  (Ctrl-C cancels)")
        r = login(c, cfg)
        if r == 130:
            print(f"{c.name}: cancelled")
            return 130
        print(f"{c.name}: {'connected, connection stays open' if r == 0 else f'ssh exited {r}'}")
        rc = rc or r
    return rc


def cmd_active(args) -> int:
    """List the ssh connections OmniQueue holds and how long they will stay open."""
    cfg = _load(args)
    if not cfg.persist_connections:
        print("persist_connections = false: no connections are kept between polls.")
        return 0
    now = time.time()
    print(f"{'CLUSTER':<16} {'HOST':<28} {'STATE':<10} {'PID':>7}  {'LAST USED':<10} CLOSES IN")
    open_count = 0
    for c in cfg.enabled_clusters:
        if c.is_local:
            print(f"{c.name:<16} {'local':<28} {'n/a':<10} {'':>7}  {'':<10} -")
            continue
        alive = connection_alive(c, cfg)
        pid = master_pid(c, cfg) if alive else None
        used = last_use(c, cfg)
        if alive:
            open_count += 1
            remaining = None if used is None else cfg.persist_seconds - (now - used)
            closes = "unknown" if remaining is None else ("any moment" if remaining <= 0 else fmt_duration(remaining))
            used_txt = time.strftime("%H:%M:%S", time.localtime(used)) if used else "?"
            print(f"{c.name:<16} {(c.host or ''):<28} {'open':<10} {pid or '':>7}  {used_txt:<10} {closes}")
        else:
            print(f"{c.name:<16} {(c.host or ''):<28} {'closed':<10} {'':>7}  {'':<10} -")
    hours = cfg.persist_seconds / 3600
    print(f"\n{open_count} open. Idle connections close {hours:g} h after their last use (persist_seconds); "
          "each poll counts as use while `monitor` runs. `omniqueue logout` closes them now.")
    return 0


def cmd_completion(args) -> int:
    from .completion import script

    sys.stdout.write(script(args.shell))
    return 0


def cmd_clusters(args) -> int:
    """Hidden helper for shell completion: print configured cluster names."""
    try:
        cfg = _load(args)
    except ConfigError:
        return 0
    for c in cfg.clusters:
        print(c.name)
    return 0


def cmd_list(args) -> int:
    """One-shot text listing, handy over a plain terminal."""
    cfg = _load(args)
    collector = _collector(cfg, args)
    collector.refresh()
    snap = collector.snapshot()
    for c in snap["clusters"]:
        counts = c["counts"]
        state = "ok" if c["ok"] else f"ERROR: {c['error']}"
        print(f"== {c['name']} ({c['host']}) {state}  running={counts.get('running',0)} "
              f"pending={counts.get('pending',0)} done={counts.get('ok',0)} failed={counts.get('problem',0)}")
    jobs = [Job.from_dict({k: v for k, v in j.items() if k not in ("key", "category", "terminal", "exit_summary")})
            for j in snap["jobs"]]
    wanted = set(args.state) if args.state else None
    order = {"running": 0, "pending": 1, "problem": 2, "ok": 3, "unknown": 4}
    jobs.sort(key=lambda j: (order.get(j.category, 9), j.cluster, j.job_id))
    print()
    print(f"{'CLUSTER':<12} {'JOBID':<12} {'STATE':<14} {'ELAPSED':>10} {'LIMIT':>10} {'NODES':>5}  NAME / REASON")
    for j in jobs:
        if wanted and j.category not in wanted and j.state not in wanted:
            continue
        note = j.reason if j.category == "pending" else describe_exit(j)
        tail = j.name + (f"  [{note}]" if note else "")
        print(f"{j.cluster:<12} {j.job_id:<12} {j.state:<14} {fmt_duration(j.elapsed_s):>10} "
              f"{fmt_duration(j.time_limit_s):>10} {j.nodes:>5}  {tail}")
    return 0


def _raw_check(cfg: Config, clusters, timeout: int) -> int:
    """Run the exact poll command on each cluster and show every section's exit status,
    first output lines and stderr: what to paste when a card says sacct failed."""
    from .slurm import combined_command, split_combined_output
    from .ssh import RemoteError, run_on_cluster

    failed = 0
    for c in clusters:
        cmd = combined_command(c.user, cfg.lookback_hours, c.squeue_args, c.sacct_args, c.use_sacct, with_partition_gres=True)
        print(f"\n== {c.name} (user = {c.user or '$USER'}, lookback = {cfg.lookback_hours} h, timeout = {timeout} s)")
        print("   remote command, to try by hand on the cluster:\n   " + cmd.replace("; ", ";\n   "))
        t0 = time.time()
        try:
            res = run_on_cluster(c, cmd, timeout, cfg)
        except RemoteError as exc:
            failed += 1
            print(f"   [FAIL] {exc} ({exc.kind}) after {time.time() - t0:.1f}s")
            continue
        sections = split_combined_output(res.stdout)
        print(f"   reply in {time.time() - t0:.1f}s, ssh exit {res.returncode}, sections: {', '.join(sections) or 'none'}")
        for name, (out, rc) in sections.items():
            lines = [ln for ln in out.splitlines() if ln.strip()]
            tag = "ok  " if rc == 0 else "FAIL"
            if rc != 0:
                failed += 1
            print(f"   [{tag}] {name}: exit {rc}, {len(lines)} lines")
            for ln in lines[:3]:
                print(f"          {ln[:160]}")
            if len(lines) > 3:
                print(f"          ... {len(lines) - 3} more")
        if res.stderr.strip():
            print("   stderr:")
            for ln in res.stderr.strip().splitlines()[:10]:
                print(f"          {ln[:200]}")
    return 1 if failed else 0


def cmd_check(args) -> int:
    """Connect to every cluster once and report what works."""
    cfg = _load(args)
    for w in _perm_warnings(cfg, args):
        print(f"[warn]  {w}")
    if getattr(args, "raw", False):
        wanted = set(args.cluster or [])
        clusters = [c for c in cfg.enabled_clusters if not wanted or c.name in wanted]
        if not clusters:
            print("no such cluster; configured: " + ", ".join(c.name for c in cfg.enabled_clusters))
            return 2
        return _raw_check(cfg, clusters, max(cfg.ssh_timeout, cfg.project_timeout))
    collector = _collector(cfg, args)
    t0 = time.time()
    collector.refresh()
    snap = collector.snapshot()
    failed = 0
    for c in snap["clusters"]:
        if c["ok"]:
            n = sum(c["counts"].values())
            warn = f"  (warning: {c['warning']})" if c.get("warning") else ""
            print(f"[ok]    {c['name']:<16} {n} jobs in {c['poll_seconds']:.1f}s{warn}")
        elif c.get("error_kind") == "login":
            print(f"[login] {c['name']:<16} not logged in; run `omniqueue login {c['name']}`")
        else:
            failed += 1
            hint = ""
            if c.get("error_kind") == "auth":
                hint = "  -> run `omniqueue login " + c["name"] + "` to log in / verify the host key"
            print(f"[FAIL]  {c['name']:<16} {c['error']}{hint}")
    print(f"checked {len(snap['clusters'])} clusters in {time.time() - t0:.1f}s")
    return 1 if failed else 0


def cmd_projects(args) -> int:
    """Print the project usage the background poll has collected (no cluster access needed
    unless --poll is given)."""
    cfg = _load(args)
    collector = _collector(cfg, args)
    poller = _projects(cfg, collector, args)
    if poller is None:
        if cfg.mode != "pi":
            print('mode = "user": whole projects are not watched; your own usage per project is on the dashboard (My usage). '
                  'Set mode = "pi" in the config to watch projects.')
        else:
            print("no cluster lists `projects` in the config; add e.g. projects = [\"naiss2025-1-23\"] to a [[clusters]] entry")
        return 2
    if args.poll:
        poller.refresh(cfg.project_clusters)

        def progress(todo):
            print("back-filling history: " + ", ".join(
                f"{c.name} {poller.coverage_days(c) or 0:.0f}/{cfg.project_history_days} d" for c in todo), file=sys.stderr)
        poller.backfill_all(progress)
    snap = poller.snapshot()
    for p in snap["projects"]:
        upd = time.strftime("%Y-%m-%d %H:%M", time.localtime(p["updated"])) if p["updated"] else "never"
        head = f"== {p['cluster']} / {p['project']}" + (f" ({p['pi']})" if p.get("pi") else "") + f"  (updated {upd}, every {p['refresh_seconds'] / 3600:g} h"
        if p.get("coverage_days") is not None:
            head += f", {p['coverage_days']:.0f} d of history" + (" so far" if p.get("backfill_pending") else "")
        head += ")"
        if p.get("error"):
            head += f"  ERROR: {p['error']}"
        print(head)
        if not p["updated"]:
            print("   no data yet: run `omniqueue projects --poll` or leave `omniqueue monitor` running while logged in\n")
            continue
        r, q = p["running"], p["pending"]
        print(f"   CPU now: {r['cpu']['jobs']} jobs on {r['cpu']['nodes']} nodes ({r['cpu']['cores']:,.0f} cores) · waiting: {q['cpu']['jobs']} jobs ({q['cpu']['cores']:,.0f} cores)")
        if p["has_gpu"]:
            print(f"   GPU now: {r['gpu']['jobs']} jobs on {r['gpu']['nodes']} nodes ({r['gpu']['gpus']:,.0f} GPUs) · waiting: {q['gpu']['jobs']} jobs ({q['gpu']['gpus']:,.0f} GPUs)"
                  + (f"   [GPU partitions: {', '.join(p['gpu_partitions'])}]" if p["gpu_partitions"] else "")
                  + (f"   [{p['gpu_factor']:g} GPU-h per Slurm GPU unit]" if p.get("gpu_factor", 1) != 1 else ""))
        if p["shares"]:
            fs = p["shares"].get("fairshare")
            print(f"   fairshare: {fs:.3f}" if fs is not None else "   fairshare: n/a", end="")
            print(f" · raw usage {p['shares'].get('raw_usage') or 0:,}")
        for label, qd, unit in (("quota", p["quota"], "core-h"), ("GPU quota", p["gpu_quota"], "GPU-h")):
            if qd:
                print(f"   {label}: {qd['used_h']:,.0f} / {qd['limit_h']:,.0f} {unit} used ({qd['window']}, {qd['source']})")
        u7c, u30c, u30g = p["usage"]["7"]["cpu"]["users"], p["usage"]["30"]["cpu"]["users"], p["usage"]["30"]["gpu"]["users"]
        gpu_cols = f" {'RUN GPUs':>9} {'30 d GPU-h':>11}" if p["has_gpu"] else ""
        print(f"   {'USER':<14} {'RUN CORES':>10} {'7 d core-h':>12} {'30 d core-h':>12} {'jobs/30 d':>10}{gpu_cols}")
        for u in p["users"][:15]:
            mark = " <- you" if u == p.get("me") else ""
            jobs30 = u30c.get(u, {}).get("jobs", 0) + u30g.get(u, {}).get("jobs", 0)
            line = (f"   {u:<14} {r['cpu']['users'].get(u, {}).get('cores', 0):>10,.0f} {u7c.get(u, {}).get('core_h', 0):>12,.0f} "
                    f"{u30c.get(u, {}).get('core_h', 0):>12,.0f} {jobs30:>10}")
            if p["has_gpu"]:
                line += f" {r['gpu']['users'].get(u, {}).get('gpus', 0):>9,.0f} {u30g.get(u, {}).get('gpu_h', 0):>11,.0f}"
            print(line + mark)
        tot = p["usage"]["30"]
        print(f"   total 30 d: {tot['cpu']['core_h']:,.0f} core-h in {tot['cpu']['jobs']} CPU jobs"
              + (f" · {tot['gpu']['gpu_h']:,.0f} GPU-h in {tot['gpu']['jobs']} GPU jobs" if p["has_gpu"] else ""))
        if args.partitions:
            print(f"   GPU partitions seen as such: {p['gpu_partitions'] or 'none'}")
            print(f"   {'PARTITION':<16} {'KIND':<4} {'JOBS':>6} {'W/ GPUS':>7} {'GPUs/node':>9} {'core-h 30 d':>12} {'GPU-h 30 d':>11}")
            for part, b in sorted(p["by_partition"].items(), key=lambda kv: -kv[1]["core_h"]):
                print(f"   {part:<16} {b['kind']:<4} {b['jobs']:>6} {b['with_gpus']:>7} {b['gpus_per_node']:>9} {b['core_h']:>12,.0f} {b['gpu_h']:>11,.1f}")
        print()
    return 0


def cmd_usage(args) -> int:
    """Your own usage per project from the stored history (no cluster access), with the
    classification of every job when --jobs is given: the place to look when a card
    shows unexpected numbers."""
    from .usage import own_usage

    cfg = _load(args)
    history = HistoryStore(cfg.data_dir / ("history-demo.json" if getattr(args, "demo", False) else "history.json"), cfg.history_days)
    wanted = set(args.cluster or [])
    clusters = [c for c in cfg.enabled_clusters if not wanted or c.name in wanted]
    print(f"mode = {cfg.mode} · history: {history.path} ({len(history.all_jobs())} jobs)")
    jobs_by = {c.name: history.jobs_for(c.name) for c in clusters}
    gpn = {c.name: history.partition_gres(c.name) for c in clusters}
    for c in clusters:
        age = history.partition_gres_age(c.name)
        print(f"\n== {c.name}: {len(jobs_by[c.name])} jobs stored · gpu_partitions = {c.gpu_partitions} · gpus_per_node = {c.gpus_per_node} "
              f"· gpu_hour_factor = {c.gpu_hour_factor:g}")
        print(f"   sinfo gres: {gpn[c.name] or 'not fetched yet'}" + (f" (checked {age / 3600:.1f} h ago)" if age is not None else ""))
    records = own_usage(cfg, jobs_by, gpus_per_node=gpn)
    if not records:
        print("\nno usage records: the history holds no jobs with an account for these clusters")
    for r in records:
        u30, u7 = r["usage"]["30"], r["usage"]["7"]
        print(f"\n-- {r['cluster']} / {r['account']}" + (f" ({r['pi']})" if r["pi"] else "") +
              f": GPU partitions {r['gpu_partitions']} · GPUs per node {r['gpus_per_node']}")
        print(f"   CPU: {u30['cpu']['core_h']:,.0f} core-h in {u30['cpu']['jobs']} jobs (30 d), {u7['cpu']['core_h']:,.0f} (7 d) · "
              f"now {r['running']['cpu']['jobs']} running / {r['pending']['cpu']['jobs']} waiting")
        print(f"   GPU: {u30['gpu']['gpu_h']:,.1f} GPU-h in {u30['gpu']['jobs']} jobs (30 d), {u7['gpu']['gpu_h']:,.1f} (7 d) · "
              f"now {r['running']['gpu']['jobs']} running ({r['running']['gpu']['gpus']} GPUs) / {r['pending']['gpu']['jobs']} waiting")
    if args.jobs:
        print(f"\n{'CLUSTER':<12} {'JOBID':<14} {'ACCOUNT':<18} {'PARTITION':<12} {'STATE':<11} {'NODES':>5} {'CPUS':>5} {'GPUS':>5} {'KIND':<4} START")
        by_cluster_parts = {r["cluster"]: (set(r["gpu_partitions"]), r["gpus_per_node"]) for r in records}
        for c in clusters:
            parts, sizes = by_cluster_parts.get(c.name, (set(c.gpu_partitions) | set(c.gpus_per_node), dict(c.gpus_per_node)))
            for j in sorted(jobs_by[c.name], key=lambda j: j.start_time or j.submit_time, reverse=True):
                kind = "gpu" if j.gpus or j.partition in parts else "cpu"
                gpus = j.gpus or ((j.nodes or 1) * sizes.get(j.partition, 0) if kind == "gpu" else 0)
                print(f"{c.name:<12} {j.job_id:<14} {(j.account or '?'):<18} {j.partition:<12} {j.state:<11} {j.nodes:>5} {j.cpus:>5} "
                      f"{gpus:>5} {kind:<4} {j.start_time or ('(pending)' if j.state == 'PENDING' else '-')}")
    return 0


def cmd_predict(args) -> int:
    """Experimental: rank clusters/partitions by estimated queue wait, from the stored samples."""
    from .predict import Request, explain, predict

    cfg = _load(args)
    collector = _collector(cfg, args)
    poller = _projects(cfg, collector, args)
    if poller is None:
        print("the predictor needs load samples, which the project poll collects: "
              + ('set mode = "pi" and ' if cfg.mode != "pi" else "") + "add `projects` to a cluster first")
        return 2
    req = Request(nodes=args.nodes, hours=args.hours, cores=args.cores, gpus=args.gpus or 0, projects=args.project or None,
                  clusters=args.cluster or None, partitions=args.partition or None)
    print("experimental: a heuristic estimate, check it against what really happens\n")
    print(explain(predict(req, poller.prediction_data())))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="omniqueue", description="Track Slurm jobs on several supercomputers.")
    p.add_argument("--config", "-c", help="path to config.toml (default: ~/.config/omniqueue/config.toml)")
    p.add_argument("--demo", action="store_true", help="use fabricated clusters and jobs instead of ssh")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    p.add_argument("--version", action="version", version=f"omniqueue {__version__}")
    sub = p.add_subparsers(dest="command", required=True,
                           metavar="{init,monitor,serve,login,active,logout,list,usage,projects,predict,check,completion}")

    s = sub.add_parser("init", help="write an example config file")
    s.add_argument("--force", action="store_true", help="overwrite an existing config")
    s.set_defaults(func=cmd_init)

    for name, help_text, open_default in (
        ("monitor", "start polling and open the dashboard in your browser", True),
        ("serve", "start the collector and the web dashboard without opening a browser", False),
    ):
        s = sub.add_parser(name, help=help_text)
        s.add_argument("--port", type=int, help="override listen_port")
        s.add_argument("--host", help="override listen_host")
        if open_default:
            s.add_argument("view", nargs="?", choices=["dashboard", "widget"], default="dashboard",
                           help="what to open: the full dashboard (default) or only the side widget")
            s.add_argument("--no-open", dest="open", action="store_false", help="do not open a browser")
            s.add_argument("--plain", action="store_true",
                           help="with `widget` on macOS: open a normal browser tab instead of a small Safari window")
        else:
            s.add_argument("--open", action="store_true", help="open the dashboard in a browser")
        s.set_defaults(func=cmd_serve, open=open_default)

    s = sub.add_parser("login", help="open the persistent ssh connection to each cluster (allows password / 2FA)")
    s.add_argument("cluster", nargs="*", help="only these clusters (default: all)")
    s.add_argument("--force", action="store_true", help="reconnect even if a connection is already open")
    s.add_argument("--close", action="store_true", help="close the persistent connection(s) instead")
    s.set_defaults(func=cmd_login)

    s = sub.add_parser("active", help="show the open ssh connections and how long they will stay open")
    s.set_defaults(func=cmd_active)

    s = sub.add_parser("logout", help="close the persistent ssh connection(s)")
    s.add_argument("cluster", nargs="*", help="only these clusters (default: all)")
    s.set_defaults(func=cmd_login, close=True, force=False)

    s = sub.add_parser("list", help="poll once and print a table to the terminal")
    s.add_argument("--state", "-s", action="append",
                   help="only show these categories/states (running, pending, ok, problem, FAILED, ...)")
    s.set_defaults(func=cmd_list)

    s = sub.add_parser("projects", help="show who runs how much in your projects (from the slow background poll)")
    s.add_argument("--poll", action="store_true", help="poll the clusters now instead of showing the stored data")
    s.add_argument("--partitions", action="store_true", help="per partition: jobs, GPU classification, core-h and GPU-h (30 d)")
    s.set_defaults(func=cmd_projects)

    s = sub.add_parser("usage", help="your own usage per project from the stored history; --jobs shows how each job was classified")
    s.add_argument("cluster", nargs="*", help="only these clusters (default: all)")
    s.add_argument("--jobs", action="store_true", help="list every stored job with partition, GPUs and CPU/GPU classification")
    s.set_defaults(func=cmd_usage)

    s = sub.add_parser("predict", help="experimental: where would a job start fastest?")
    s.add_argument("--nodes", "-N", type=int, default=1, help="nodes the job needs (default 1)")
    s.add_argument("--hours", "-t", type=float, default=1.0, help="wall time in hours (default 1)")
    s.add_argument("--cores", "-n", type=int, help="total cores instead of whole nodes")
    s.add_argument("--gpus", "-G", type=int, default=0, help="GPUs per node; only GPU partitions are considered then")
    s.add_argument("--project", "-A", action="append", help="only these projects (repeatable)")
    s.add_argument("--cluster", "-M", action="append", help="only these clusters (repeatable)")
    s.add_argument("--partition", "-p", action="append", help="only these partitions (repeatable)")
    s.set_defaults(func=cmd_predict)

    s = sub.add_parser("check", help="test the connection to every configured cluster")
    s.add_argument("--raw", action="store_true",
                   help="run the poll command once and print every part's exit status, output and stderr")
    s.add_argument("cluster", nargs="*", help="with --raw: only these clusters (default all)")
    s.set_defaults(func=cmd_check)

    s = sub.add_parser("completion", help="print a tab-completion script for bash, zsh or fish")
    s.add_argument("shell", choices=["bash", "zsh", "fish"])
    s.set_defaults(func=cmd_completion)

    s = sub.add_parser("_clusters")  # no help text: hidden, used by the completion scripts
    s.set_defaults(func=cmd_clusters)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\ncancelled", file=sys.stderr)
        return 130
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    except BrokenPipeError:  # e.g. `omniqueue list | head`
        return 0


if __name__ == "__main__":
    sys.exit(main())
