import io
import subprocess
import sys
import unittest
from contextlib import redirect_stdout

from omniqueue.cli import main
from omniqueue.completion import COMMANDS, script


class CompletionTests(unittest.TestCase):
    def test_scripts_mention_every_command(self):
        for shell in ("bash", "zsh", "fish"):
            text = script(shell)
            for name in COMMANDS:
                self.assertIn(name, text, (shell, name))
            self.assertIn("_clusters", text)
        with self.assertRaises(ValueError):
            script("powershell")

    def test_monitor_view_argument(self):
        from omniqueue.cli import build_parser

        p = build_parser()
        self.assertEqual(p.parse_args(["monitor"]).view, "dashboard")
        self.assertEqual(p.parse_args(["monitor", "widget"]).view, "widget")
        self.assertTrue(p.parse_args(["monitor", "widget"]).open)
        with self.assertRaises(SystemExit):
            p.parse_args(["monitor", "bogus"])
        for shell in ("bash", "zsh", "fish"):
            self.assertIn("widget", script(shell))

    def test_safari_widget_window(self):
        from unittest import mock

        from omniqueue import cli as cli_mod

        script = cli_mod.safari_widget_script("http://127.0.0.1:8765/widget")
        self.assertIn('URL:"http://127.0.0.1:8765/widget"', script)
        self.assertIn("set bounds of front window", script)
        self.assertIn(str(cli_mod.WIDGET_WIDTH), script)
        # not macOS: the caller falls back to the ordinary browser
        with mock.patch.object(cli_mod.sys, "platform", "linux"):
            self.assertFalse(cli_mod.open_widget_window("http://x/widget"))
        # macOS: osascript is run with the script; a failure also falls back
        with mock.patch.object(cli_mod.sys, "platform", "darwin"), \
             mock.patch.object(cli_mod.shutil, "which", return_value="/usr/bin/osascript"), \
             mock.patch.object(cli_mod.subprocess, "run") as run:
            run.return_value = mock.Mock(returncode=0, stderr="")
            self.assertTrue(cli_mod.open_widget_window("http://x/widget"))
            self.assertEqual(run.call_args[0][0][:2], ["osascript", "-e"])
            self.assertIn("Safari", run.call_args[0][0][2])
            run.return_value = mock.Mock(returncode=1, stderr="Safari got an error")
            self.assertFalse(cli_mod.open_widget_window("http://x/widget"))
        self.assertTrue(cli_mod.build_parser().parse_args(["monitor", "widget", "--plain"]).plain)

    def test_bash_script_parses(self):
        proc = subprocess.run(["bash", "-n"], input=script("bash"), text=True, capture_output=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_cli(self):
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(["completion", "zsh"]), 0)
        self.assertIn("compdef _omniqueue omniqueue", out.getvalue())
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(["--demo", "_clusters"]), 0)
        self.assertEqual(out.getvalue().split(), ["tetralith", "dardel", "lumi", "offline-cluster"])
        # a missing config must not break completion
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(main(["--config", "/nonexistent/config.toml", "_clusters"]), 0)
        self.assertEqual(out.getvalue(), "")

    def test_hidden_command_not_in_help(self):
        out = io.StringIO()
        with redirect_stdout(out):
            try:
                main(["--help"])
            except SystemExit:
                pass
        self.assertNotIn("_clusters", out.getvalue())
        self.assertIn("completion", out.getvalue())


if __name__ == "__main__":
    unittest.main()
