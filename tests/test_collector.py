import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from omniqueue.collector import Collector
from omniqueue.config import ClusterConfig, Config
from omniqueue.history import HistoryStore
from omniqueue.slurm import combined_command, split_combined_output
from omniqueue.ssh import build_ssh_argv, control_options

from test_slurm import SACCT_OUT, SQUEUE_OUT


def _fake_bin(dirpath: Path, name: str, body: str) -> None:
    p = dirpath / name
    p.write_text("#!/bin/sh\n" + body)
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


class CombinedCommandTests(unittest.TestCase):
    def test_split(self):
        out = "a|b\n@@OMNIQUEUE squeue rc=0\nc|d\ne|f\n@@OMNIQUEUE sacct rc=3\n"
        sections = split_combined_output(out)
        self.assertEqual(sections["squeue"], ("a|b", 0))
        self.assertEqual(sections["sacct"], ("c|d\ne|f", 3))

    def test_command_shape(self):
        cmd = combined_command("me", 24, None, None, use_sacct=True)
        self.assertEqual(cmd.count("@@OMNIQUEUE"), 2)
        self.assertTrue(cmd.startswith("squeue "))
        self.assertIn("; sacct ", cmd)
        self.assertNotIn("sacct", combined_command("me", 24, None, None, use_sacct=False))


class SshArgvTests(unittest.TestCase):
    def test_control_master_options(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="c one", host="c.example")], data_dir=Path(tmp), persist_seconds=60)
            c = cfg.clusters[0]
            opts = control_options(cfg, c)
            self.assertIn("ControlMaster=auto", opts)
            self.assertIn("ControlPersist=60", opts)
            self.assertIn(f"ControlPath={tmp}/ssh/cm-c_one", opts)  # named after the cluster
            self.assertEqual(stat.S_IMODE(os.stat(Path(tmp) / "ssh").st_mode), 0o700)
            cfg.persist_connections = False
            self.assertFalse(any(o.startswith("Control") for o in control_options(cfg, c)))

    def test_argv(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="c", host="c.example", ssh_options=["-p", "2222"])], data_dir=Path(tmp))
            argv = build_ssh_argv(cfg.clusters[0], "squeue", 7, cfg)
            self.assertEqual(argv[0], "ssh")
            self.assertIn("BatchMode=yes", argv)
            self.assertIn("ConnectTimeout=7", argv)
            self.assertEqual(argv[-3:], ["--", "c.example", "squeue"])
            self.assertIn("2222", argv)
            interactive = build_ssh_argv(cfg.clusters[0], "true", 7, cfg, batch=False)
            self.assertNotIn("BatchMode=yes", interactive)
            self.assertNotIn("-l", argv)  # no user configured
            with_user = build_ssh_argv(ClusterConfig(name="d", host="d.example", user="flotr"), "squeue", 7, cfg)
            self.assertIn("flotr", with_user)
            self.assertEqual(with_user[with_user.index("-l") + 1], "flotr")


class LocalClusterEndToEnd(unittest.TestCase):
    """Run the real collector against fake squeue/sacct executables."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.bin = Path(self.tmp.name) / "bin"
        self.bin.mkdir()
        self.env = mock.patch.dict(os.environ, {"PATH": f"{self.bin}:{os.environ['PATH']}", "USER": "tester"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def _collector(self, use_sacct=True):
        cfg = Config(clusters=[ClusterConfig(name="here", host="local", use_sacct=use_sacct)],
                     data_dir=Path(self.tmp.name), persist_connections=False)
        return Collector(cfg, HistoryStore(Path(self.tmp.name) / "h.json"))

    def test_happy_path(self):
        _fake_bin(self.bin, "squeue", f"case \"$*\" in *--user=tester*) ;; *) echo bad user >&2; exit 9;; esac\ncat <<'X'\n{SQUEUE_OUT}X\n")
        _fake_bin(self.bin, "sacct", f"cat <<'X'\n{SACCT_OUT}X\n")
        col = self._collector()
        col.refresh()
        snap = col.snapshot()
        status = snap["clusters"][0]
        self.assertTrue(status["ok"], status)
        self.assertIsNone(status["warning"])
        self.assertEqual(status["counts"], {"running": 1, "pending": 2, "ok": 1, "problem": 4, "unknown": 0})
        self.assertEqual(len(snap["jobs"]), 8)

    def test_squeue_failure_is_an_error(self):
        _fake_bin(self.bin, "squeue", "echo 'slurm_load_jobs error: Unable to contact slurm controller' >&2; exit 1\n")
        _fake_bin(self.bin, "sacct", "exit 0\n")
        col = self._collector()
        col.refresh()
        status = col.snapshot()["clusters"][0]
        self.assertFalse(status["ok"])
        self.assertIn("squeue exited 1", status["error"])
        self.assertIn("slurm controller", status["error"])

    def test_sacct_failure_is_only_a_warning(self):
        _fake_bin(self.bin, "squeue", f"cat <<'X'\n{SQUEUE_OUT}X\n")
        _fake_bin(self.bin, "sacct", "echo 'sacct: error: Problem talking to the database' >&2; exit 1\n")
        col = self._collector()
        col.refresh()
        status = col.snapshot()["clusters"][0]
        self.assertTrue(status["ok"])
        self.assertIn("sacct exited 1", status["warning"])
        self.assertEqual(status["counts"]["running"], 1)

    def test_missing_slurm(self):
        col = self._collector()
        os.environ["PATH"] = f"{self.bin}:/usr/bin:/bin"  # neither squeue nor sacct available
        col.refresh()
        status = col.snapshot()["clusters"][0]
        self.assertFalse(status["ok"])
        self.assertIn("squeue exited 127", status["error"])


if __name__ == "__main__":
    unittest.main()


class ConnectionStateTests(unittest.TestCase):
    def test_snapshot_reports_connection(self):
        from omniqueue import collector as collector_mod

        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="far", host="far.example"), ClusterConfig(name="here", host="local")],
                         data_dir=Path(tmp))
            col = Collector(cfg, HistoryStore(Path(tmp) / "h.json"))
            with mock.patch.object(collector_mod, "connection_alive", return_value=False) as alive:
                snap = col.snapshot()
                by = {c["name"]: c for c in snap["clusters"]}
                self.assertFalse(by["far"]["connected"])
                self.assertIsNone(by["here"]["connected"])  # local cluster: not applicable
                col.snapshot()
                alive.assert_called_once()  # cached between snapshots
            col.conn_cache_seconds = 0
            with mock.patch.object(collector_mod, "connection_alive", return_value=True):
                self.assertTrue({c["name"]: c for c in col.snapshot()["clusters"]}["far"]["connected"])
            cfg.persist_connections = False
            self.assertIsNone({c["name"]: c for c in col.snapshot()["clusters"]}["far"]["connected"])


class CloseConnectionTests(unittest.TestCase):
    def test_stale_socket_is_removed_and_master_killed(self):
        from omniqueue import ssh as ssh_mod

        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="far", host="far.example")], data_dir=Path(tmp))
            c = cfg.clusters[0]
            self.assertEqual(ssh_mod.close_connection(c, cfg), "not open")
            sock = ssh_mod.socket_path(c, cfg)
            sock.write_text("")  # pretend a master left its socket behind
            calls = []

            def fake_run(argv, **kw):
                calls.append(argv)
                return mock.Mock(returncode=0 if argv[0] == "pkill" else 1)

            with mock.patch.object(ssh_mod.subprocess, "run", side_effect=fake_run), \
                 mock.patch.object(ssh_mod.time, "sleep"):
                self.assertEqual(ssh_mod.close_connection(c, cfg), "killed")
            self.assertFalse(sock.exists())
            self.assertEqual(calls[0][:3], ["ssh", "-O", "exit"])
            self.assertEqual(calls[1][:2], ["pkill", "-f"])
            self.assertEqual(calls[1][2], str(sock))
            # no socket -> alive check does not even spawn ssh
            with mock.patch.object(ssh_mod.subprocess, "run") as run:
                self.assertFalse(ssh_mod.connection_alive(c, cfg))
                run.assert_not_called()


class ResilienceTests(unittest.TestCase):
    def test_classify_error(self):
        from omniqueue.ssh import classify_error

        self.assertEqual(classify_error("ssh: connect to host x port 22: Connection timed out"), "network")
        self.assertEqual(classify_error("ssh: Could not resolve hostname x: Temporary failure in name resolution"), "network")
        self.assertEqual(classify_error("client_loop: send disconnect: Broken pipe"), "network")
        self.assertEqual(classify_error("x: Permission denied (publickey,keyboard-interactive)."), "auth")
        self.assertEqual(classify_error("Host key verification failed."), "auth")
        self.assertEqual(classify_error("something odd"), "other")

    def test_keepalive_options_always_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="c", host="c")], data_dir=Path(tmp), keepalive_seconds=7)
            for persist in (True, False):
                cfg.persist_connections = persist
                opts = control_options(cfg, cfg.clusters[0])
                self.assertIn("ServerAliveInterval=7", opts)
                self.assertIn("ServerAliveCountMax=3", opts)
                self.assertEqual("ControlMaster=auto" in opts, persist)

    def test_retry_backoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="a", host="a"), ClusterConfig(name="b", host="b")],
                         data_dir=Path(tmp), refresh_seconds=120, retry_seconds=15)
            col = Collector(cfg, HistoryStore(Path(tmp) / "h.json"))
            self.assertEqual(col.next_delay(), 120)
            a = col._status["a"]
            a.ok, a.failures = False, 1
            self.assertEqual(col.next_delay(), 15)
            a.failures = 2
            self.assertEqual(col.next_delay(), 30)
            a.failures = 3
            self.assertEqual(col.next_delay(), 60)
            a.failures = 10
            self.assertEqual(col.next_delay(), 120)  # capped at the normal interval
            b = col._status["b"]
            b.ok, b.failures = False, 1  # the freshest failure drives the retry
            self.assertEqual(col.next_delay(), 15)

    def test_timeout_tears_down_master_and_reports_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="far", host="far.example")], data_dir=Path(tmp), ssh_timeout=1)
            col = Collector(cfg, HistoryStore(Path(tmp) / "h.json"))
            from omniqueue import collector as collector_mod
            from omniqueue.ssh import RemoteError

            with mock.patch.object(collector_mod, "run_on_cluster", side_effect=RemoteError("timed out after 3s", kind="timeout")), \
                 mock.patch.object(collector_mod, "close_connection") as closed:
                col.refresh()
            closed.assert_called_once()
            snap = col.snapshot()
            st = snap["clusters"][0]
            self.assertEqual(st["error_kind"], "timeout")
            self.assertEqual(st["failures"], 1)
            self.assertTrue(snap["offline"])

            with mock.patch.object(collector_mod, "run_on_cluster",
                                   side_effect=RemoteError("ssh failed: Permission denied (publickey)", kind="auth")), \
                 mock.patch.object(collector_mod, "close_connection") as closed:
                col.refresh()
            closed.assert_not_called()  # an auth failure is not a dead link
            self.assertEqual(col.snapshot()["clusters"][0]["error_kind"], "auth")
            self.assertEqual(col.snapshot()["clusters"][0]["failures"], 2)
