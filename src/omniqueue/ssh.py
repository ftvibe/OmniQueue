"""Run a shell command on a cluster, over ssh or locally."""

from __future__ import annotations

import os
import re
import subprocess
import time
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


def socket_path(cluster: ClusterConfig, config: Config) -> Path:
    """The master socket for one cluster, named after the cluster so it can be found again."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", cluster.name)
    return control_socket_dir(config) / f"cm-{safe}"


def control_options(config: Config, cluster: ClusterConfig | None = None) -> list[str]:
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
    if not config.persist_connections or cluster is None:
        return opts
    sock = socket_path(cluster, config)
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
        argv += control_options(config, cluster)
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
    if config.persist_connections and socket_path(cluster, config).exists() and not connection_alive(cluster, config):
        close_connection(cluster, config)  # a stale or hung master would otherwise be reused
    argv = build_ssh_argv(cluster, "true", config.ssh_timeout, config, batch=False)
    proc = subprocess.Popen(argv)
    try:
        return proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        # a master that got as far as forking must not survive a cancelled login
        close_connection(cluster, config)
        return 130


def _mux(cluster: ClusterConfig, config: Config, command: str) -> list[str]:
    return (["ssh", "-O", command] + control_options(config, cluster) + _user_args(cluster)
            + cluster.ssh_options + ["--", cluster.host or ""])


def shell(cluster: ClusterConfig, config: Config, command: str = "") -> int:
    """Interactive ssh session over the shared connection (no second login when
    the master is open; otherwise this login opens it)."""
    if cluster.is_local:
        return subprocess.call([os.environ.get("SHELL", "sh")] + (["-c", command] if command else []))
    if config.persist_connections and socket_path(cluster, config).exists() and not connection_alive(cluster, config):
        close_connection(cluster, config)
    argv = build_ssh_argv(cluster, command, config.ssh_timeout, config, batch=False)
    if not command:
        argv.insert(1, "-t")  # force a tty for the interactive shell
    return subprocess.call(argv)


def connection_alive(cluster: ClusterConfig, config: Config) -> bool:
    """True when a master connection for this cluster is open and answering."""
    if cluster.is_local or not config.persist_connections or not socket_path(cluster, config).exists():
        return False
    try:
        return subprocess.run(_mux(cluster, config, "check"), capture_output=True, timeout=5).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def close_connection(cluster: ClusterConfig, config: Config) -> str:
    """Close the master for this cluster; returns what happened.

    Asks the master to exit over its socket first. A master hung on a dead link
    ignores that, so the master process is then killed by its socket path
    (OpenSSH puts the ControlPath in the mux process title) and the socket file
    is removed, so the next connection starts fresh.
    """
    if cluster.is_local or not config.persist_connections:
        return "n/a"
    sock = socket_path(cluster, config)
    if not sock.exists():
        return "not open"
    try:
        subprocess.run(_mux(cluster, config, "exit"), capture_output=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass
    for _ in range(10):  # give the master a moment to remove its socket
        if not sock.exists():
            return "closed"
        time.sleep(0.1)
    killed = False
    try:
        killed = subprocess.run(["pkill", "-f", str(sock)], capture_output=True, timeout=5).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        sock.unlink()
    except OSError:
        pass
    return "killed" if killed else "stale socket removed"


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
