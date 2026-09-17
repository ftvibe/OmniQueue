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
            cfg = Config(clusters=[ClusterConfig(name="c", host="c.example")], data_dir=Path(tmp), persist_seconds=60)
            opts = control_options(cfg)
            self.assertIn("ControlMaster=auto", opts)
            self.assertIn("ControlPersist=60", opts)
            self.assertTrue(any(o.startswith(f"ControlPath={tmp}/ssh/cm-") for o in opts))
            self.assertEqual(stat.S_IMODE(os.stat(Path(tmp) / "ssh").st_mode), 0o700)
            cfg.persist_connections = False
            self.assertEqual(control_options(cfg), [])

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
