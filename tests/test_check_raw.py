import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from omniqueue.cli import build_parser, cmd_check


def _script(directory: Path, name: str, body: str) -> None:
    path = directory / name
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


class CheckRawTest(unittest.TestCase):
    def test_raw_shows_each_section_and_stderr(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fakebin = root / "bin"
            fakebin.mkdir()
            _script(fakebin, "squeue", 'echo "1|main|RUNNING|4|8|(null)|N/A"\n')
            _script(fakebin, "sacct", 'echo "sacct: error: Problem talking to the database" >&2\nexit 1\n')
            _script(fakebin, "sinfo", 'echo "gpu|gpu:8"\n')
            cfg = root / "config.toml"
            cfg.write_text('[[clusters]]\nname = "fake"\nhost = "local"\nuser = "me"\n')
            cfg.chmod(0o600)
            env = {**os.environ, "PATH": f"{fakebin}:{os.environ['PATH']}", "XDG_DATA_HOME": str(root / "data")}
            args = build_parser().parse_args(["-c", str(cfg), "check", "--raw", "fake"])
            with mock.patch.dict(os.environ, env), mock.patch("builtins.print") as out:
                rc = cmd_check(args)
            text = "\n".join(" ".join(str(a) for a in call.args) for call in out.call_args_list)
        self.assertEqual(rc, 1)
        self.assertIn("[ok  ] squeue: exit 0, 1 lines", text)
        self.assertIn("[FAIL] sacct: exit 1", text)
        self.assertIn("Problem talking to the database", text)
        self.assertIn("--user=me", text)  # the command to try by hand

    def test_raw_unknown_cluster(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "config.toml"
            cfg.write_text('[[clusters]]\nname = "fake"\nhost = "local"\n')
            cfg.chmod(0o600)
            args = build_parser().parse_args(["-c", str(cfg), "check", "--raw", "nosuch"])
            with mock.patch.dict(os.environ, {"XDG_DATA_HOME": tmp}), mock.patch("builtins.print"):
                self.assertEqual(cmd_check(args), 2)


if __name__ == "__main__":
    unittest.main()
