"""Configuration loading (TOML) and the example config written by ``omniqueue init``."""

from __future__ import annotations

import os
import re
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


def default_config_path() -> Path:
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(Path.home(), ".config")
    return Path(base) / "omniqueue" / "config.toml"


def default_logo_dir() -> Path:
    return default_config_path().parent / "logos"


def default_data_dir() -> Path:
    base = os.environ.get("XDG_DATA_HOME") or os.path.join(Path.home(), ".local", "share")
    return Path(base) / "omniqueue"


@dataclass
class ClusterConfig:
    name: str
    host: str | None = None  # ssh destination (alias from ~/.ssh/config works); None/"local" runs locally
    user: str | None = None  # your account on that cluster: used for the ssh login and for squeue/sacct
    ssh_options: list[str] = field(default_factory=list)
    squeue_args: list[str] = field(default_factory=list)
    sacct_args: list[str] = field(default_factory=list)
    use_sacct: bool = True
    enabled: bool = True
    color: str | None = None  # optional accent colour for the dashboard
    logo: str | None = None  # image file path or http(s) URL shown on the cluster card

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
    persist_seconds: int = 4 * 3600  # how long an idle master connection stays open
    connect_on_poll: bool = False  # False: polls only use connections opened by `omniqueue login`
    accept_new_host_keys: bool = False  # polls trust unknown host keys (TOFU) when true; default requires known_hosts
    allow_remote: bool = False  # permit listen_host other than loopback (needs access_token)
    access_token: str | None = None  # required by every request when listening beyond loopback
    keepalive_seconds: int = 15  # ssh ServerAliveInterval; a dead link is noticed after 3 misses
    retry_seconds: int = 15  # first retry delay after a failed poll (doubles up to refresh_seconds)
    listen_host: str = "127.0.0.1"
    listen_port: int = 8765
    data_dir: Path = field(default_factory=default_data_dir)
    logo_dir: Path = field(default_factory=default_logo_dir)

    @property
    def enabled_clusters(self) -> list[ClusterConfig]:
        return [c for c in self.clusters if c.enabled]

    @property
    def listens_locally(self) -> bool:
        return self.listen_host in ("127.0.0.1", "::1", "localhost")

    def logo_source(self, cluster: ClusterConfig) -> str | None:
        """Where a cluster's logo comes from: an http(s) URL, a local file path, or None.

        Explicit ``logo`` in the cluster entry wins; otherwise a file named after
        the cluster in ``logo_dir`` (``<name>.svg|png|jpg|jpeg|webp``) is used.
        """
        if cluster.logo:
            if cluster.logo.startswith(("http://", "https://")):
                return cluster.logo
            path = Path(os.path.expanduser(cluster.logo))
            return str(path) if path.is_file() else None
        for ext in ("svg", "png", "jpg", "jpeg", "webp"):
            candidate = self.logo_dir / f"{cluster.name}.{ext}"
            if candidate.is_file():
                return str(candidate)
        return None


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
persist_seconds = 14400      # ... for this long after the last poll (4 h); set false above to disable
connect_on_poll = false      # polls never open connections themselves: `omniqueue login` opens them,
                             # `omniqueue logout` closes them. true = reconnect automatically (keys only)
accept_new_host_keys = false # polls only talk to hosts already in ~/.ssh/known_hosts;
                             # `omniqueue login` lets you verify a new fingerprint interactively
keepalive_seconds = 15       # notice a dead connection (new wifi, sleep) within ~45 s
retry_seconds   = 15         # retry a failed cluster after 15 s, 30 s, 60 s ... up to refresh_seconds
listen_host     = "127.0.0.1"   # keep it local; put Tailscale/ssh -L in front for remote viewing
listen_port     = 8765
# allow_remote  = true          # only with an access_token; every request must carry it
# access_token  = "a-long-random-secret"

[[clusters]]
name = "tetralith"
host = "tetralith"               # ssh alias
# user = "x_flotr"               # your account there (ssh login + Slurm user); default: same as your local user
# ssh_options = ["-J", "bastion", "-p", "2222", "-i", "~/.ssh/id_omniqueue"]   # allow-listed options only
# squeue_args = ["--partition=main"]
# sacct_args  = ["--account=naiss2024-1-23"]
# use_sacct = true               # set false on clusters without job accounting
# color = "#5f9e99"
# logo = "~/Pictures/nsc.png"    # or drop <name>.png/.svg into ~/.config/omniqueue/logos/

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


# ---- ssh_options allow-list --------------------------------------------------------------
# Flags that take a value and are safe: identity, port, user, jump host, config-free options.
_SSH_FLAGS_WITH_VALUE = {"-p", "-i", "-l", "-J", "-o", "-c", "-m", "-b", "-B"}
_SSH_FLAGS_NO_VALUE = {"-4", "-6", "-C", "-q", "-T", "-a", "-k"}
# `-o Key=value` keys that cannot run commands, forward ports or bypass host checks.
_SSH_OPTION_KEYS = {
    "user", "port", "hostname", "identityfile", "identitiesonly", "identityagent", "certificatefile",
    "proxyjump", "preferredauthentications", "pubkeyauthentication", "passwordauthentication",
    "kbdinteractiveauthentication", "gssapiauthentication", "connecttimeout", "connectionattempts",
    "serveraliveinterval", "serveralivecountmax", "tcpkeepalive", "addressfamily", "bindaddress",
    "bindinterface", "ciphers", "kexalgorithms", "macs", "hostkeyalgorithms", "pubkeyacceptedalgorithms",
    "pubkeyacceptedkeytypes", "compression", "loglevel", "ipqos", "requesttty", "numberofpasswordprompts",
    "hostbasedauthentication", "updatehostkeys", "hashknownhosts", "canonicalizehostname",
    "canonicaldomains", "canonicalizemaxdots", "canonicalizefallbacklocal", "checkhostip",
    "fingerprinthash", "verifyhostkeydns", "rekeylimit", "setenv", "sendenv",
}
_SSH_OPTION_FIXED = {  # keys allowed only with these values
    "forwardagent": {"no"},
    "forwardx11": {"no"},
    "forwardx11trusted": {"no"},
    "permitlocalcommand": {"no"},
    "stricthostkeychecking": {"yes", "ask", "accept-new"},
    "batchmode": {"yes", "no"},
    "clearallforwardings": {"yes"},
    "gatewayports": {"no"},
    "exitonforwardfailure": {"yes", "no"},
}
_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_.,:@%+~/=\[\]-]+$")


def validate_ssh_options(options: list[str], where: str = "ssh_options") -> None:
    """Reject ssh options that could run commands, forward ports, or bypass host checks."""
    if not isinstance(options, list) or not all(isinstance(o, str) for o in options):
        raise ConfigError(f"{where} must be a list of strings.")
    i = 0
    while i < len(options):
        flag = options[i]
        if flag in _SSH_FLAGS_NO_VALUE:
            i += 1
            continue
        if flag not in _SSH_FLAGS_WITH_VALUE:
            raise ConfigError(f"{where}: {flag!r} is not allowed (permitted flags: -p -i -l -J -o -c -m -b -B -4 -6 -C -q).")
        if i + 1 >= len(options):
            raise ConfigError(f"{where}: {flag} needs a value.")
        value = options[i + 1]
        if not _SAFE_VALUE.match(value):
            raise ConfigError(f"{where}: value {value!r} for {flag} contains characters that are not allowed.")
        if flag == "-o":
            key, sep, val = value.partition("=")
            if not sep:
                raise ConfigError(f"{where}: -o needs Key=value, got {value!r}.")
            k, v = key.lower(), val.lower()
            if k in _SSH_OPTION_FIXED:
                if v not in _SSH_OPTION_FIXED[k]:
                    raise ConfigError(f"{where}: -o {key}={val} is not allowed (permitted: {sorted(_SSH_OPTION_FIXED[k])}).")
            elif k not in _SSH_OPTION_KEYS:
                raise ConfigError(
                    f"{where}: -o {key} is not allowed. ProxyCommand, LocalCommand, port forwarding, "
                    "Control*, Include and known_hosts overrides are blocked; use ProxyJump for bastions."
                )
        i += 2


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
        validate_ssh_options(c.get("ssh_options", []), f"clusters[{i}] ({c['name']}).ssh_options")
        for key in ("squeue_args", "sacct_args"):
            for arg in c.get(key, []):
                if not isinstance(arg, str) or not _SAFE_VALUE.match(arg):
                    raise ConfigError(f"clusters[{i}] ({c['name']}).{key}: {arg!r} contains characters that are not allowed.")
        clusters.append(ClusterConfig(**c))

    cfg = Config(clusters=clusters)
    for key in ("refresh_seconds", "lookback_hours", "history_days", "ssh_timeout", "listen_port",
                "persist_seconds", "keepalive_seconds", "retry_seconds"):
        if key in raw:
            try:
                setattr(cfg, key, int(raw[key]))
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"`{key}` must be an integer.") from exc
    for key in ("persist_connections", "accept_new_host_keys", "allow_remote", "connect_on_poll"):
        if key in raw:
            setattr(cfg, key, bool(raw[key]))
    if "access_token" in raw:
        cfg.access_token = str(raw["access_token"])
        if len(cfg.access_token) < 16:
            raise ConfigError("`access_token` must be at least 16 characters.")
    if "listen_host" in raw:
        cfg.listen_host = str(raw["listen_host"])
    if "data_dir" in raw:
        cfg.data_dir = Path(os.path.expanduser(str(raw["data_dir"])))
    if "logo_dir" in raw:
        cfg.logo_dir = Path(os.path.expanduser(str(raw["logo_dir"])))
    if cfg.refresh_seconds < 5:
        raise ConfigError("`refresh_seconds` must be at least 5.")
    if not cfg.listens_locally:
        if not cfg.allow_remote:
            raise ConfigError(
                f"listen_host = {cfg.listen_host!r} would expose the dashboard beyond this machine. "
                "Prefer `ssh -L` or Tailscale to reach it; if you really want this, set allow_remote = true "
                "and an access_token."
            )
        if not cfg.access_token:
            raise ConfigError("allow_remote = true requires an `access_token` (at least 16 random characters).")
    return cfg


def secure_dir(path: Path) -> Path:
    """Create `path` (if needed) and make it private to the user."""
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass
    return path


def permission_warnings(config_path: Path, cfg: Config) -> list[str]:
    """Human-readable warnings for files that other users could read."""
    warnings: list[str] = []
    for label, path, want in (("config", config_path, 0o600), ("data dir", cfg.data_dir, 0o700)):
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
        except OSError:
            continue
        if mode & 0o077:
            warnings.append(f"{label} {path} is mode {mode:03o}; run: chmod {want:o} {path}")
    return warnings


def write_example_config(path: Path, force: bool = False) -> Path:
    if path.exists() and not force:
        raise ConfigError(f"{path} already exists (use --force to overwrite).")
    secure_dir(path.parent)
    path.write_text(EXAMPLE_CONFIG)
    os.chmod(path, 0o600)
    return path
