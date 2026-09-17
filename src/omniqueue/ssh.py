"""Run a shell command on a cluster, over ssh or locally."""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import ClusterConfig, Config, secure_dir


class RemoteError(Exception):
    """The command could not be run or returned an error."""

    def __init__(self, message: str, kind: str = "other"):
        super().__init__(message)
        self.kind = kind  # network | auth | timeout | other


_NETWORK_RE = re.compile(
    r"timed out|no route to host|network is unreachable|could not resolve|name or service not known|"
    r"temporary failure in name resolution|connection refused|connection reset|broken pipe|"
    r"connection closed by|closed by remote host|unable to connect|kex_exchange_identification|"
    r"software caused connection abort|control socket connect",
    re.I,
)
_AUTH_RE = re.compile(
    r"permission denied|authentication|verification code|password|host key|too many authentication failures|"
    r"no .*host key is known|not in the list of known hosts",
    re.I,
)


def classify_error(stderr: str) -> str:
    """Guess why ssh failed from its stderr: network, auth or other."""
    if _AUTH_RE.search(stderr):
        return "auth"
    if _NETWORK_RE.search(stderr):
        return "network"
    return "other"


@dataclass
class CommandResult:
    stdout: str
    stderr: str
    returncode: int


def control_socket_dir(config: Config) -> Path:
    """Directory for the per-cluster ssh master sockets (created 0700)."""
    secure_dir(config.data_dir)
    return secure_dir(config.data_dir / "ssh")


def control_options(config: Config) -> list[str]:
    """ssh options that reuse one master connection per cluster between polls.

    The first ssh to a cluster becomes the master and stays in the background
    for ``persist_seconds`` after the last use; later polls multiplex over it,
    so they skip the TCP/key handshake (and any password or 2FA prompt, which
    ``omniqueue login`` can satisfy once by hand).
    """
    # Keepalives make the master notice a dead link (new network, laptop woke up)
    # within keepalive_seconds * 3 and exit, so the next poll opens a fresh one.
    opts = [
        "-o", f"ServerAliveInterval={config.keepalive_seconds}",
        "-o", "ServerAliveCountMax=3",
        "-o", "TCPKeepAlive=yes",
    ]
    if not config.persist_connections:
        return opts
    sock = control_socket_dir(config) / "cm-%C"  # %C = hash of user@host:port
    return opts + [
        "-o", "ControlMaster=auto",
        "-o", f"ControlPath={sock}",
        "-o", f"ControlPersist={config.persist_seconds}",
    ]


def _user_args(cluster: ClusterConfig) -> list[str]:
    return ["-l", cluster.user] if cluster.user else []


def build_ssh_argv(
    cluster: ClusterConfig,
    remote_command: str,
    timeout: int,
    config: Config | None = None,
    batch: bool = True,
) -> list[str]:
    # Unattended polls only talk to hosts already in known_hosts (or accept new keys when the
    # config opts in). The interactive `omniqueue login` asks, so a fingerprint can be verified.
    if not batch:
        host_keys = "ask"
    elif config is not None and config.accept_new_host_keys:
        host_keys = "accept-new"
    else:
        host_keys = "yes"
    argv = ["ssh", "-o", f"ConnectTimeout={timeout}", "-o", f"StrictHostKeyChecking={host_keys}",
            "-o", "ClearAllForwardings=yes", "-o", "ForwardAgent=no", "-o", "ForwardX11=no"]
    if batch:
        argv += ["-o", "BatchMode=yes", "-T"]  # never hang on a password prompt
    if config is not None:
        argv += control_options(config)
    if cluster.user:
        argv += ["-l", cluster.user]  # the account on that cluster: ssh login and Slurm user alike
    argv += cluster.ssh_options
    argv += ["--", cluster.host or ""]
    if remote_command:
        argv.append(remote_command)
    return argv


def login(cluster: ClusterConfig, config: Config) -> int:
    """Open the master connection interactively (password / 2FA allowed).

    Runs ``ssh host true`` attached to the terminal with the same ControlPath
    the poller uses, so the resulting master is what later polls reuse.
    """
    if cluster.is_local:
        return 0
    argv = build_ssh_argv(cluster, "true", config.ssh_timeout, config, batch=False)
    return subprocess.call(argv)


def connection_alive(cluster: ClusterConfig, config: Config) -> bool:
    """True when a master connection for this cluster is currently open."""
    if cluster.is_local or not config.persist_connections:
        return False
    argv = ["ssh", "-O", "check"] + control_options(config) + _user_args(cluster) + cluster.ssh_options + ["--", cluster.host or ""]
    try:
        return subprocess.run(argv, capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def close_connection(cluster: ClusterConfig, config: Config) -> None:
    if cluster.is_local or not config.persist_connections:
        return
    argv = ["ssh", "-O", "exit"] + control_options(config) + _user_args(cluster) + cluster.ssh_options + ["--", cluster.host or ""]
    try:
        subprocess.run(argv, capture_output=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        pass


def run_on_cluster(
    cluster: ClusterConfig, remote_command: str, timeout: int, config: Config | None = None
) -> CommandResult:
    if cluster.is_local:
        argv: list[str] = ["sh", "-c", remote_command]
        # local runs still get a timeout so a hung Slurm controller cannot block polling
        total_timeout = timeout
    else:
        argv = build_ssh_argv(cluster, remote_command, timeout, config)
        total_timeout = timeout * 3  # connect + slow slurmctld + transfer
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=total_timeout,
            stdin=subprocess.DEVNULL,
        )
    except FileNotFoundError as exc:
        raise RemoteError(f"{argv[0]} not found: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RemoteError(f"timed out after {total_timeout}s", kind="timeout") from exc
    if proc.returncode == 255 and not cluster.is_local:
        err = proc.stderr.strip() or "connection error"
        raise RemoteError(f"ssh failed: {err}", kind=classify_error(err))
    return CommandResult(proc.stdout, proc.stderr, proc.returncode)
