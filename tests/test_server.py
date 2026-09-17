import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from omniqueue.demo import DemoCollector, demo_config
from omniqueue.history import HistoryStore
from omniqueue.server import make_server


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cfg = demo_config()
        cls.collector = DemoCollector(cfg, HistoryStore(Path(cls.tmp.name) / "h.json"))
        cls.collector.refresh()
        cls.server = make_server(cls.collector, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmp.cleanup()

    def get(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}") as r:
            return r.status, r.headers.get("Content-Type", ""), r.read()

    def test_state(self):
        status, ctype, body = self.get("/api/state")
        self.assertEqual(status, 200)
        self.assertIn("application/json", ctype)
        snap = json.loads(body)
        self.assertEqual({c["name"] for c in snap["clusters"]}, {"tetralith", "dardel", "lumi", "offline-cluster"})
        offline = next(c for c in snap["clusters"] if c["name"] == "offline-cluster")
        self.assertFalse(offline["ok"])
        self.assertTrue(snap["jobs"])
        self.assertIn("exit_summary", snap["jobs"][0])

    def test_static(self):
        for path, needle in (("/", b"<title>OmniQueue"), ("/app.js", b"/api/state"), ("/style.css", b"--problem")):
            status, _, body = self.get(path)
            self.assertEqual(status, 200, path)
            self.assertIn(needle, body)

    def test_404(self):
        with self.assertRaises(urllib.error.HTTPError) as cm:
            self.get("/../pyproject.toml")
        self.assertEqual(cm.exception.code, 404)

    def test_refresh(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/refresh", method="POST")
        with urllib.request.urlopen(req) as r:
            self.assertEqual(json.loads(r.read())["ok"], True)


if __name__ == "__main__":
    unittest.main()
