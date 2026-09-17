"""Configuration loading (TOML) and the example config written by ``omniqueue init``."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


def default_config_path() -> Path:
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(Path.home(), ".config")
    return Path(base) / "omniqueue" / "config.toml"


def default_data_dir() -> Path:
    base = os.environ.get("XDG_DATA_HOME") or os.path.join(Path.home(), ".local", "share")
    return Path(base) / "omniqueue"


@dataclass
class ClusterConfig:
    name: str
    host: str | None = None  # ssh destination (alias from ~/.ssh/config works); None/"local" runs locally
    user: str | None = None  # Slurm user to query; default is $USER on the remote side
    ssh_options: list[str] = field(default_factory=list)
    squeue_args: list[str] = field(default_factory=list)
    sacct_args: list[str] = field(default_factory=list)
    use_sacct: bool = True
    enabled: bool = True
    color: str | None = None  # optional accent colour for the dashboard

    @property
    def is_local(self) -> bool:
        return self.host in (None, "", "local", "localhost")


@dataclass
class Config:
    clusters: list[ClusterConfig]
    refresh_seconds: int = 60
    lookback_hours: int = 72  # how far back sacct is asked for finished jobs
    history_days: int = 30  # how long finished jobs stay in the local store
    ssh_timeout: int = 20
    persist_connections: bool = True  # keep one ssh master connection per cluster open between polls
    persist_seconds: int = 8 * 3600  # how long an idle master connection stays open
    listen_host: str = "127.0.0.1"
    listen_port: int = 8765
    data_dir: Path = field(default_factory=default_data_dir)

    @property
    def enabled_clusters(self) -> list[ClusterConfig]:
        return [c for c in self.clusters if c.enabled]


EXAMPLE_CONFIG = """\
# OmniQueue configuration.
# Each [[clusters]] entry is one supercomputer. `host` is passed to ssh, so any
# alias from ~/.ssh/config (with ProxyJump, keys, ControlMaster, ...) works.
# Password prompts are not supported: set up keys or an ssh agent first.

refresh_seconds = 60      # how often every cluster is polled
lookback_hours  = 72      # how far back sacct is asked for finished jobs
history_days    = 30      # finished jobs stay in the local history this long
ssh_timeout     = 20      # seconds before a hanging ssh is given up on
persist_connections = true   # keep one ssh connection per cluster open between polls
persist_seconds = 28800      # ... for this long after the last poll (8 h)
listen_host     = "127.0.0.1"
listen_port     = 8765

[[clusters]]
name = "tetralith"
host = "tetralith"               # ssh alias
# user = "x_flotr"               # Slurm user, defaults to $USER on the cluster
# ssh_options = ["-o", "ProxyJump=bastion"]
# squeue_args = ["--partition=main"]
# sacct_args  = ["--account=naiss2024-1-23"]
# use_sacct = true               # set false on clusters without job accounting
# color = "#4f8cff"

[[clusters]]
name = "dardel"
host = "dardel.pdc.kth.se"
user = "flotr"

# [[clusters]]
# name = "login-node"
# host = "local"                  # run squeue/sacct directly, no ssh
"""


class ConfigError(Exception):
    pass


def load_config(path: Path | None = None) -> Config:
    path = path or default_config_path()
    if not path.exists():
        raise ConfigError(
            f"No config found at {path}. Run `omniqueue init` to create an example, "
            "or pass --config."
        )
    with open(path, "rb") as fh:
        try:
            raw = tomllib.load(fh)
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"Could not parse {path}: {exc}") from exc
    return config_from_dict(raw)


def config_from_dict(raw: dict) -> Config:
    clusters_raw = raw.get("clusters") or []
    if not isinstance(clusters_raw, list) or not clusters_raw:
        raise ConfigError("Config needs at least one [[clusters]] entry.")
    clusters: list[ClusterConfig] = []
    seen: set[str] = set()
    for i, c in enumerate(clusters_raw):
        if "name" not in c:
            raise ConfigError(f"clusters[{i}] is missing `name`.")
        if c["name"] in seen:
            raise ConfigError(f"Duplicate cluster name {c['name']!r}.")
        seen.add(c["name"])
        allowed = set(ClusterConfig.__dataclass_fields__)  # type: ignore[attr-defined]
        unknown = set(c) - allowed
        if unknown:
            raise ConfigError(f"clusters[{i}] ({c['name']}): unknown keys {sorted(unknown)}")
        clusters.append(ClusterConfig(**c))

    cfg = Config(clusters=clusters)
    for key in ("refresh_seconds", "lookback_hours", "history_days", "ssh_timeout", "listen_port", "persist_seconds"):
        if key in raw:
            try:
                setattr(cfg, key, int(raw[key]))
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"`{key}` must be an integer.") from exc
    if "persist_connections" in raw:
        cfg.persist_connections = bool(raw["persist_connections"])
    if "listen_host" in raw:
        cfg.listen_host = str(raw["listen_host"])
    if "data_dir" in raw:
        cfg.data_dir = Path(os.path.expanduser(str(raw["data_dir"])))
    if cfg.refresh_seconds < 5:
        raise ConfigError("`refresh_seconds` must be at least 5.")
    return cfg


def write_example_config(path: Path, force: bool = False) -> Path:
    if path.exists() and not force:
        raise ConfigError(f"{path} already exists (use --force to overwrite).")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(EXAMPLE_CONFIG)
    return path
