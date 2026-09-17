"""Run a shell command on a cluster, over ssh or locally."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass

from .config import ClusterConfig


class RemoteError(Exception):
    """The command could not be run or returned an error."""


@dataclass
class CommandResult:
    stdout: str
    stderr: str
    returncode: int


def build_ssh_argv(cluster: ClusterConfig, remote_command: str, timeout: int) -> list[str]:
    argv = [
        "ssh",
        "-o",
        "BatchMode=yes",  # never hang on a password prompt
        "-o",
        f"ConnectTimeout={timeout}",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-T",
    ]
    argv += cluster.ssh_options
    argv += ["--", cluster.host or "", remote_command]
    return argv


def run_on_cluster(cluster: ClusterConfig, remote_command: str, timeout: int) -> CommandResult:
    if cluster.is_local:
        argv: list[str] = ["sh", "-c", remote_command]
        # local runs still get a timeout so a hung Slurm controller cannot block polling
        total_timeout = timeout
    else:
        argv = build_ssh_argv(cluster, remote_command, timeout)
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
