import json
import os
import stat
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from omniqueue.config import ClusterConfig, Config, ConfigError, config_from_dict, validate_ssh_options, write_example_config
from omniqueue.demo import DemoCollector, demo_config
from omniqueue.history import HistoryStore
from omniqueue.server import make_server
from omniqueue.ssh import build_ssh_argv


class SshOptionAllowList(unittest.TestCase):
    def test_allowed(self):
        validate_ssh_options(["-p", "2222", "-i", "~/.ssh/id_omniqueue", "-J", "bastion.example.org", "-l", "flotr",
                              "-o", "ProxyJump=user@bastion:22", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
                              "-o", "ForwardAgent=no", "-4", "-C"])

    def test_blocked(self):
        for bad in (
            ["-o", "ProxyCommand=nc %h %p"],
            ["-o", "LocalCommand=rm -rf ~"],
            ["-o", "PermitLocalCommand=yes"],
            ["-o", "LocalForward=8080 localhost:80"],
            ["-o", "RemoteForward=2222 localhost:22"],
            ["-o", "DynamicForward=1080"],
            ["-o", "ControlPath=/tmp/x"],
            ["-o", "ControlMaster=yes"],
            ["-o", "StrictHostKeyChecking=no"],
            ["-o", "UserKnownHostsFile=/dev/null"],
            ["-o", "ForwardAgent=yes"],
            ["-o", "Include=/etc/evil"],
            ["-L", "8080:localhost:80"],
            ["-R", "2222:localhost:22"],
            ["-D", "1080"],
            ["-A"], ["-X"], ["-Y"], ["-W", "host:22"], ["-F", "/tmp/cfg"],
            ["-p"],  # missing value
            ["-o", "Port"],  # no '='
            ["-i", "key; rm -rf ~"],
        ):
            with self.assertRaises(ConfigError, msg=bad):
                validate_ssh_options(bad)

    def test_config_rejects_bad_options_and_args(self):
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"name": "x", "host": "x", "ssh_options": ["-o", "ProxyCommand=evil"]}]})
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"name": "x", "host": "x", "squeue_args": ["--partition=main; rm -rf ~"]}]})
        config_from_dict({"clusters": [{"name": "x", "host": "x", "squeue_args": ["--partition=main"], "ssh_options": ["-J", "b"]}]})


class RemoteListening(unittest.TestCase):
    def test_non_loopback_requires_opt_in_and_token(self):
        base = {"clusters": [{"name": "x", "host": "x"}], "listen_host": "0.0.0.0"}
        with self.assertRaises(ConfigError):
            config_from_dict(base)
        with self.assertRaises(ConfigError):
            config_from_dict({**base, "allow_remote": True})
        with self.assertRaises(ConfigError):
            config_from_dict({**base, "allow_remote": True, "access_token": "short"})
        cfg = config_from_dict({**base, "allow_remote": True, "access_token": "a-very-long-random-secret"})
        self.assertEqual(cfg.access_token, "a-very-long-random-secret")
        self.assertTrue(config_from_dict({"clusters": [{"name": "x", "host": "x"}]}).listens_locally)


class HostKeys(unittest.TestCase):
    def test_strict_by_default_ask_on_login(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="c", host="c")], data_dir=Path(tmp))
            c = cfg.clusters[0]
            self.assertIn("StrictHostKeyChecking=yes", build_ssh_argv(c, "true", 5, cfg))
            self.assertIn("StrictHostKeyChecking=ask", build_ssh_argv(c, "true", 5, cfg, batch=False))
            self.assertIn("ClearAllForwardings=yes", build_ssh_argv(c, "true", 5, cfg))
            cfg.accept_new_host_keys = True
            self.assertIn("StrictHostKeyChecking=accept-new", build_ssh_argv(c, "true", 5, cfg))


class FileModes(unittest.TestCase):
    def test_init_and_history_are_private(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "cfg" / "config.toml"
            write_example_config(cfg_path)
            self.assertEqual(stat.S_IMODE(cfg_path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(cfg_path.parent.stat().st_mode), 0o700)
            store = HistoryStore(Path(tmp) / "data" / "history.json")
            store.save()
            self.assertEqual(stat.S_IMODE(store.path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(store.path.parent.stat().st_mode), 0o700)


class WebApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        collector = DemoCollector(demo_config(), HistoryStore(Path(cls.tmp.name) / "h.json"))
        collector.refresh()
        cls.open_server = make_server(collector, "127.0.0.1", 0)
        cls.locked_server = make_server(collector, "127.0.0.1", 0, access_token="s3cret-token-for-tests")
        for srv in (cls.open_server, cls.locked_server):
            threading.Thread(target=srv.serve_forever, daemon=True).start()
        cls.open_url = f"http://127.0.0.1:{cls.open_server.server_address[1]}"
        cls.locked_url = f"http://127.0.0.1:{cls.locked_server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        for srv in (cls.open_server, cls.locked_server):
            srv.shutdown()
            srv.server_close()
        cls.tmp.cleanup()

    @staticmethod
    def req(url, method="GET", headers=None):
        r = urllib.request.Request(url, method=method, headers=headers or {})
        opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler)
        try:
            with opener.open(r) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read()

    def test_post_needs_csrf_token(self):
        status, _, body = self.req(f"{self.open_url}/api/refresh", "POST")
        self.assertEqual(status, 403)
        status, _, body = self.req(f"{self.open_url}/api/refresh", "POST", {"X-OmniQueue-Token": "wrong"})
        self.assertEqual(status, 403)
        # the token is embedded in the page, as the browser would read it
        _, _, page = self.req(f"{self.open_url}/")
        token = page.split(b'name="omniqueue-token" content="')[1].split(b'"')[0].decode()
        self.assertNotIn("__OMNIQUEUE_TOKEN__", token)
        status, _, body = self.req(f"{self.open_url}/api/refresh", "POST", {"X-OmniQueue-Token": token})
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body)["ok"])

    def test_security_headers(self):
        _, headers, _ = self.req(f"{self.open_url}/api/state")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["X-Frame-Options"], "DENY")
        self.assertIn("default-src 'self'", headers["Content-Security-Policy"])

    def test_access_token_flow(self):
        status, _, _ = self.req(f"{self.locked_url}/api/state")
        self.assertEqual(status, 401)
        status, _, _ = self.req(f"{self.locked_url}/", headers={"Cookie": "omniqueue_access=nope"})
        self.assertEqual(status, 401)
        # ?token= sets the cookie and redirects
        r = urllib.request.Request(f"{self.locked_url}/?token=s3cret-token-for-tests")
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None
        try:
            urllib.request.build_opener(NoRedirect).open(r)
            self.fail("expected redirect")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 303)
            self.assertIn("omniqueue_access=s3cret-token-for-tests", e.headers["Set-Cookie"])
            self.assertIn("HttpOnly", e.headers["Set-Cookie"])
        status, _, _ = self.req(f"{self.locked_url}/api/state", headers={"Cookie": "omniqueue_access=s3cret-token-for-tests"})
        self.assertEqual(status, 200)
        status, _, _ = self.req(f"{self.locked_url}/?token=wrong-token-value")
        self.assertEqual(status, 401)


if __name__ == "__main__":
    unittest.main()
