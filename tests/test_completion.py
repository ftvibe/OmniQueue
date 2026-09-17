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
