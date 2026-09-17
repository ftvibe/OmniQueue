"""Command line entry point: ``omniqueue serve | list | init | check``."""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
import webbrowser
from pathlib import Path

from . import __version__
from .collector import Collector
from .config import Config, ConfigError, default_config_path, load_config, write_example_config
from .history import HistoryStore
from .models import Job
from .server import make_server
from .slurm import describe_exit


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


# -- subcommands --------------------------------------------------------------------
def cmd_init(args) -> int:
    path = Path(args.config) if args.config else default_config_path()
    write_example_config(path, force=args.force)
    print(f"Wrote example config to {path}\nEdit it, then run: omniqueue serve")
    return 0


def cmd_serve(args) -> int:
    cfg = _load(args)
    if args.port:
        cfg.listen_port = args.port
    if args.host:
        cfg.listen_host = args.host
    collector = _collector(cfg, args)
    collector.start()
    server = make_server(collector, cfg.listen_host, cfg.listen_port)
    url = f"http://{cfg.listen_host}:{server.server_address[1]}/"
    names = ", ".join(c.name for c in cfg.enabled_clusters)
    print(f"OmniQueue {__version__} watching {names}\nDashboard: {url}  (Ctrl-C to stop)")
    if args.open:
        threading.Timer(0.5, webbrowser.open, args=(url,)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        collector.stop()
        server.server_close()
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


def cmd_check(args) -> int:
    """Connect to every cluster once and report what works."""
    cfg = _load(args)
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
        else:
            failed += 1
            print(f"[FAIL]  {c['name']:<16} {c['error']}")
    print(f"checked {len(snap['clusters'])} clusters in {time.time() - t0:.1f}s")
    return 1 if failed else 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="omniqueue", description="Track Slurm jobs on several supercomputers.")
    p.add_argument("--config", "-c", help="path to config.toml (default: ~/.config/omniqueue/config.toml)")
    p.add_argument("--demo", action="store_true", help="use fabricated clusters and jobs instead of ssh")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    p.add_argument("--version", action="version", version=f"omniqueue {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("init", help="write an example config file")
    s.add_argument("--force", action="store_true", help="overwrite an existing config")
    s.set_defaults(func=cmd_init)

    s = sub.add_parser("serve", help="start the collector and the web dashboard")
    s.add_argument("--port", type=int, help="override listen_port")
    s.add_argument("--host", help="override listen_host")
    s.add_argument("--open", action="store_true", help="open the dashboard in a browser")
    s.set_defaults(func=cmd_serve)

    s = sub.add_parser("list", help="poll once and print a table to the terminal")
    s.add_argument("--state", "-s", action="append",
                   help="only show these categories/states (running, pending, ok, problem, FAILED, ...)")
    s.set_defaults(func=cmd_list)

    s = sub.add_parser("check", help="test the connection to every configured cluster")
    s.set_defaults(func=cmd_check)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    except BrokenPipeError:  # e.g. `omniqueue list | head`
        return 0


if __name__ == "__main__":
    sys.exit(main())
