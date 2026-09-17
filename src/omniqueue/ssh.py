"""Run a shell command on a cluster, over ssh or locally."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import ClusterConfig, Config


class RemoteError(Exception):
    """The command could not be run or returned an error."""


@dataclass
class CommandResult:
    stdout: str
    stderr: str
    returncode: int


def control_socket_dir(config: Config) -> Path:
    """Directory for the per-cluster ssh master sockets (created 0700)."""
    d = config.data_dir / "ssh"
    d.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(d, 0o700)
    except OSError:
        pass
    return d


def control_options(config: Config) -> list[str]:
    """ssh options that reuse one master connection per cluster between polls.

    The first ssh to a cluster becomes the master and stays in the background
    for ``persist_seconds`` after the last use; later polls multiplex over it,
    so they skip the TCP/key handshake (and any password or 2FA prompt, which
    ``omniqueue login`` can satisfy once by hand).
    """
    if not config.persist_connections:
        return []
    sock = control_socket_dir(config) / "cm-%C"  # %C = hash of user@host:port
    return [
        "-o", "ControlMaster=auto",
        "-o", f"ControlPath={sock}",
        "-o", f"ControlPersist={config.persist_seconds}",
    ]


def build_ssh_argv(
    cluster: ClusterConfig,
    remote_command: str,
    timeout: int,
    config: Config | None = None,
    batch: bool = True,
) -> list[str]:
    argv = ["ssh", "-o", f"ConnectTimeout={timeout}", "-o", "StrictHostKeyChecking=accept-new"]
    if batch:
        argv += ["-o", "BatchMode=yes", "-T"]  # never hang on a password prompt
    if config is not None:
        argv += control_options(config)
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
    argv = ["ssh", "-O", "check"] + control_options(config) + cluster.ssh_options + ["--", cluster.host or ""]
    try:
        return subprocess.run(argv, capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def close_connection(cluster: ClusterConfig, config: Config) -> None:
    if cluster.is_local or not config.persist_connections:
        return
    argv = ["ssh", "-O", "exit"] + control_options(config) + cluster.ssh_options + ["--", cluster.host or ""]
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
        raise RemoteError(f"timed out after {total_timeout}s") from exc
    if proc.returncode == 255 and not cluster.is_local:
        raise RemoteError(f"ssh failed: {proc.stderr.strip() or 'connection error'}")
    return CommandResult(proc.stdout, proc.stderr, proc.returncode)
