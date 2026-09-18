"""Configuration loading (TOML) and the example config written by ``omniqueue init``."""

from __future__ import annotations

import os
import re
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


# "user": your own jobs and your own usage per project; "pi": also the project-wide poll,
# cards and predictor.  The USER-version branch of OmniQueue sets this to "user".
DEFAULT_MODE = "pi"


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
    show_load: bool = True  # include this cluster in the load view (sinfo + all-users squeue, on demand)
    load_partitions: list[str] = field(default_factory=list)  # only these partitions in the load view; [] = all
    enabled: bool = True
    color: str | None = None  # optional accent colour for the dashboard
    logo: str | None = None  # image file path or http(s) URL shown on the cluster card
    projects: list[str] = field(default_factory=list)  # Slurm accounts whose usage (all users) is tracked
    project_quotas: dict[str, float] = field(default_factory=dict)  # project -> core-hours per 30 days (optional)
    project_gpu_quotas: dict[str, float] = field(default_factory=dict)  # project -> GPU-hours per 30 days (optional)
    project_pis: dict[str, str] = field(default_factory=dict)  # project -> PI (or any label) shown next to the project name
    gpu_partitions: list[str] = field(default_factory=list)  # partitions counted as GPU; [] = detect from sinfo gres
    gpus_per_node: dict[str, int] = field(default_factory=dict)  # partition -> GPUs per node, when sinfo reports no gres
    gpu_hour_factor: float = 1.0  # GPU-hours billed per Slurm GPU unit and hour (LUMI-G: 0.5, two units per MI250X)
    project_refresh_seconds: int | None = None  # how often the projects are polled here; None = global default
    nice: int = 0  # the --nice you usually submit with on this cluster (lowers priority; used by the predictor)
    # GPU time booked on a companion account (Dardel: "<project>-gpu") is folded into the project
    project_gpu_suffix: str = "-gpu"  # companion account = project + suffix; "" turns this off
    project_gpu_accounts: dict[str, str] = field(default_factory=dict)  # project -> companion account, when it is not project + suffix

    @property
    def is_local(self) -> bool:
        return self.host in (None, "", "local", "localhost")

    def gpu_account(self, project: str) -> str | None:
        """The companion account whose jobs belong to `project` (None when there is none)."""
        explicit = self.project_gpu_accounts.get(project)
        if explicit:
            return explicit if explicit != project else None
        return project + self.project_gpu_suffix if self.project_gpu_suffix else None

    def accounts_of(self, project: str) -> list[str]:
        gpu = self.gpu_account(project)
        return [project, gpu] if gpu else [project]

    @property
    def account_map(self) -> dict[str, str]:
        """Slurm account -> the project it is shown under, for every watched project."""
        out: dict[str, str] = {}
        for proj in self.projects:
            for acc in self.accounts_of(proj):
                out.setdefault(acc, proj)
        return out

    @property
    def all_accounts(self) -> list[str]:
        """Every account the project poll asks Slurm about (projects plus companions)."""
        return list(self.account_map)

    def canonical_account(self, account: str) -> str:
        """The project an account of yours belongs to: a companion account folds into its
        project (by the explicit table, or by stripping the suffix), anything else is itself."""
        for proj, gpu in self.project_gpu_accounts.items():
            if gpu == account:
                return proj
        if self.project_gpu_suffix and account.endswith(self.project_gpu_suffix) and len(account) > len(self.project_gpu_suffix):
            return account[: -len(self.project_gpu_suffix)]
        return account


@dataclass
class Config:
    clusters: list[ClusterConfig]
    mode: str = DEFAULT_MODE  # "user" | "pi"
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
    project_refresh_seconds: int = 2 * 3600  # slow background poll of project usage, fairshare and load samples
    project_history_days: int = 90  # how long project jobs and load samples are kept for the rolling overview
    project_timeout: int = 300  # seconds allowed for one project poll (sacct over many users is slow)
    project_backfill_days: int = 7  # older history is fetched in chunks of this many days, one chunk a minute
    project_overlap_hours: int = 24  # each poll re-asks sacct for this much before the previous poll (late accounting)
    listen_host: str = "127.0.0.1"
    listen_port: int = 8765
    data_dir: Path = field(default_factory=default_data_dir)
    logo_dir: Path = field(default_factory=default_logo_dir)

    @property
    def enabled_clusters(self) -> list[ClusterConfig]:
        return [c for c in self.clusters if c.enabled]

    def project_interval(self, cluster: ClusterConfig) -> int:
        return cluster.project_refresh_seconds or self.project_refresh_seconds

    @property
    def project_clusters(self) -> list[ClusterConfig]:
        """Clusters whose projects are watched as a whole: none in user mode."""
        if self.mode != "pi":
            return []
        return [c for c in self.enabled_clusters if c.projects]

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

# mode = "pi"             # "user": your jobs and your usage per project; "pi": also whole projects,
                          # their cards and the predictor (needs `projects` on a cluster). Default: __MODE__
refresh_seconds = 60      # how often every cluster is polled
lookback_hours  = 72      # how far back sacct is asked for finished jobs (older history arrives in chunks)
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
project_refresh_seconds = 7200  # project usage / fairshare / load samples: slow background poll (2 h)
project_history_days = 90       # project jobs and load samples kept this long for the rolling overview
project_timeout = 300           # seconds one project poll may take (sacct over all users is slow)
project_backfill_days = 7       # after the first poll, older history arrives in chunks of this many days
project_overlap_hours = 24      # each poll re-reads this much before the previous poll, in case accounting was late
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
# show_load = true               # include in the load view (l); false to leave this cluster out
# load_partitions = ["main", "gpu"]   # only these partitions in the load view; omit for all
# color = "#5f9e99"
# logo = "~/Pictures/nsc.png"    # or drop <name>.png/.svg into ~/.config/omniqueue/logos/
# projects = ["naiss2025-1-23"]  # Slurm accounts to watch: who runs how much, fairshare, quota (all users)
#                                # a companion GPU account (Dardel: "naiss2025-1-23-gpu") is folded into the project
# project_gpu_suffix = "-gpu"    # how the companion account is named (default "-gpu"; "" = none)
# project_gpu_accounts = { "naiss2025-1-23" = "gpu-2025-42" }  # when it is not project + suffix
# project_quotas = { "naiss2025-1-23" = 100000 }   # core-hours per 30 days, when the site does not publish it via sshare
# project_gpu_quotas = { "naiss2025-1-23" = 2000 } # GPU-hours per 30 days (GPU jobs are counted separately from CPU jobs)
# project_pis = { "naiss2025-1-23" = "A. Nilsson" } # PI (or any label) shown next to the project name
# gpu_partitions = ["gpu"]       # partitions whose jobs count as GPU jobs; default: those sinfo reports GPUs for
# gpus_per_node = { gpu = 4 }    # GPUs per node of a partition when sinfo reports no gres (whole-node GPU jobs)
# gpu_hour_factor = 0.5          # GPU-hours billed per Slurm GPU unit: LUMI-G shows 8 units per node for 4 MI250X
                                 # and bills each unit as half a GPU-hour; default 1.0
# project_refresh_seconds = 3600 # poll the projects on this cluster every hour instead of the global 2 h
# nice = 0                       # the --nice you usually submit with here (the predictor accounts for it)

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
        for key in ("squeue_args", "sacct_args", "load_partitions", "projects", "gpu_partitions"):
            for arg in c.get(key, []):
                if not isinstance(arg, str) or not _SAFE_VALUE.match(arg):
                    raise ConfigError(f"clusters[{i}] ({c['name']}).{key}: {arg!r} contains characters that are not allowed.")
        for key, unit in (("project_quotas", "core-hours"), ("project_gpu_quotas", "GPU-hours")):
            quotas = c.get(key, {})
            if not isinstance(quotas, dict):
                raise ConfigError(f"clusters[{i}] ({c['name']}).{key} must be a table of project = {unit}.")
            for proj, hours in quotas.items():
                if not isinstance(hours, (int, float)) or hours <= 0:
                    raise ConfigError(f"clusters[{i}] ({c['name']}).{key}[{proj!r}] must be a positive number of {unit}.")
        gpn = c.get("gpus_per_node", {})
        if not isinstance(gpn, dict) or not all(isinstance(v, int) and v > 0 for v in gpn.values()):
            raise ConfigError(f"clusters[{i}] ({c['name']}).gpus_per_node must be a table of partition = positive integer.")
        pis = c.get("project_pis", {})
        if not isinstance(pis, dict) or not all(isinstance(v, str) for v in pis.values()):
            raise ConfigError(f"clusters[{i}] ({c['name']}).project_pis must be a table of project = \"name\".")
        suffix = c.get("project_gpu_suffix", "-gpu")
        if not isinstance(suffix, str) or (suffix and not _SAFE_VALUE.match(suffix)):
            raise ConfigError(f"clusters[{i}] ({c['name']}).project_gpu_suffix must be a short account suffix such as \"-gpu\" (or \"\").")
        gacc = c.get("project_gpu_accounts", {})
        if not isinstance(gacc, dict) or not all(isinstance(v, str) and _SAFE_VALUE.match(v) for v in gacc.values()):
            raise ConfigError(f"clusters[{i}] ({c['name']}).project_gpu_accounts must be a table of project = \"account\".")
        factor = c.get("gpu_hour_factor", 1.0)
        if not isinstance(factor, (int, float)) or factor <= 0:
            raise ConfigError(f"clusters[{i}] ({c['name']}).gpu_hour_factor must be a positive number.")
        if c.get("project_refresh_seconds") is not None and int(c["project_refresh_seconds"]) < 300:
            raise ConfigError(f"clusters[{i}] ({c['name']}).project_refresh_seconds must be at least 300.")
        clusters.append(ClusterConfig(**c))

    cfg = Config(clusters=clusters)
    for key in ("refresh_seconds", "lookback_hours", "history_days", "ssh_timeout", "listen_port",
                "persist_seconds", "keepalive_seconds", "retry_seconds", "project_refresh_seconds", "project_history_days",
                "project_timeout", "project_backfill_days", "project_overlap_hours"):
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
    if "mode" in raw:
        cfg.mode = str(raw["mode"]).lower()
        if cfg.mode not in ("user", "pi"):
            raise ConfigError('`mode` must be "user" (own jobs and usage) or "pi" (also whole projects).')
    if "data_dir" in raw:
        cfg.data_dir = Path(os.path.expanduser(str(raw["data_dir"])))
    if "logo_dir" in raw:
        cfg.logo_dir = Path(os.path.expanduser(str(raw["logo_dir"])))
    if cfg.refresh_seconds < 5:
        raise ConfigError("`refresh_seconds` must be at least 5.")
    if cfg.project_refresh_seconds < 300:
        raise ConfigError("`project_refresh_seconds` must be at least 300 (this is a slow, all-users query).")
    if cfg.project_backfill_days < 1:
        raise ConfigError("`project_backfill_days` must be at least 1.")
    if cfg.project_overlap_hours < 1:
        raise ConfigError("`project_overlap_hours` must be at least 1.")
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
    path.write_text(EXAMPLE_CONFIG.replace("__MODE__", DEFAULT_MODE))
    os.chmod(path, 0o600)
    return path
