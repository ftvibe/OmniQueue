import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from omniqueue.demo import DemoCollector, DemoProjectPoller, demo_config
from omniqueue.history import HistoryStore
from omniqueue.projects import ProjectStore
from omniqueue.server import make_server


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cfg = demo_config()
        cls.collector = DemoCollector(cfg, HistoryStore(Path(cls.tmp.name) / "h.json"))
        cls.collector.refresh()
        cls.projects = DemoProjectPoller(cfg, ProjectStore(Path(cls.tmp.name) / "p.json"), cls.collector)
        cls.projects.refresh(cfg.project_clusters)
        cls.server = make_server(cls.collector, "127.0.0.1", 0, projects=cls.projects)
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

    def test_logo(self):
        # demo config ships an svg logo for "lumi" and none for the others
        status, ctype, body = self.get("/logo/lumi")
        self.assertEqual(status, 200)
        self.assertIn("image/svg", ctype)
        self.assertIn(b"<svg", body)
        snap = json.loads(self.get("/api/state")[2])
        by_name = {c["name"]: c for c in snap["clusters"]}
        self.assertEqual(by_name["lumi"]["logo"], "/logo/lumi")
        self.assertIsNone(by_name["dardel"]["logo"])
        with self.assertRaises(urllib.error.HTTPError) as cm:
            self.get("/logo/dardel")
        self.assertEqual(cm.exception.code, 404)
        with self.assertRaises(urllib.error.HTTPError) as cm:
            self.get("/logo/../../etc/passwd")
        self.assertEqual(cm.exception.code, 404)

    def test_load_endpoints(self):
        status, _, body = self.get("/api/load")
        self.assertEqual(status, 200)
        snap = json.loads(body)
        self.assertEqual({c["name"] for c in snap["clusters"]}, {"tetralith", "dardel", "lumi", "offline-cluster"})
        self.assertFalse(snap["fetching"])
        page = self.get("/")[2]
        token = page.split(b'name="omniqueue-token" content="')[1].split(b'"')[0].decode()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/load/refresh", method="POST",
                                     headers={"X-OmniQueue-Token": token})
        with urllib.request.urlopen(req) as r:
            self.assertTrue(json.loads(r.read())["ok"])
        for _ in range(100):  # the demo fetch takes under a second
            snap = json.loads(self.get("/api/load")[2])
            if snap["fetched_at"] and not snap["fetching"]:
                break
            import time
            time.sleep(0.05)
        by = {c["name"]: c for c in snap["clusters"]}
        self.assertTrue(by["tetralith"]["partitions"])
        self.assertEqual({p["partition"] for p in by["dardel"]["partitions"]}, {"main", "gpu"})  # load_partitions filter
        self.assertIn("timed out", by["offline-cluster"]["error"])

    def test_etag_304(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/state") as r:
            etag = r.headers.get("ETag")
        self.assertTrue(etag)
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/state", headers={"If-None-Match": etag})
        try:
            with urllib.request.urlopen(req) as r:
                self.fail(f"expected 304, got {r.status}")
        except urllib.error.HTTPError as e:  # urllib reports 304 as an error
            self.assertEqual(e.code, 304)
            self.assertEqual(e.read(), b"")
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/state", headers={"If-None-Match": '"stale"'})
        with urllib.request.urlopen(req) as r:
            self.assertEqual(r.status, 200)
            self.assertTrue(json.loads(r.read())["clusters"])
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/load") as r:
            self.assertTrue(r.headers.get("ETag"))

    def test_demo_has_array_jobs(self):
        snap = json.loads(self.get("/api/state")[2])
        arr = [j for j in snap["jobs"] if j["array_job_id"] == "620000"]
        self.assertEqual(len(arr), 40)
        self.assertEqual({j["category"] for j in arr}, {"running", "pending", "ok", "problem"})
        self.assertEqual(self.get("/arrays.js")[0], 200)

    def test_widget(self):
        for path in ("/widget", "/widget.html"):
            status, ctype, body = self.get(path)
            self.assertEqual(status, 200, path)
            self.assertIn("text/html", ctype)
            self.assertIn(b"OmniQueue widget", body)
            self.assertNotIn(b"__OMNIQUEUE_TOKEN__", body)  # CSRF token substituted like index.html
        status, _, body = self.get("/widget.js")
        self.assertEqual(status, 200)
        self.assertIn(b"Recently", self.get("/widget")[2])

    def test_viewer_headers(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/state",
                                     headers={"X-OmniQueue-Client": "w-test", "X-OmniQueue-Interval": "600"})
        with urllib.request.urlopen(req) as r:
            snap = json.loads(r.read())
        self.assertGreaterEqual(snap["viewers"], 1)
        self.assertEqual(snap["effective_refresh"], 600)
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/state",
                                     headers={"X-OmniQueue-Client": "d-test", "X-OmniQueue-Interval": "0"})
        with urllib.request.urlopen(req) as r:
            snap = json.loads(r.read())
        self.assertEqual(snap["effective_refresh"], snap["refresh_seconds"])

    def test_404(self):
        with self.assertRaises(urllib.error.HTTPError) as cm:
            self.get("/../pyproject.toml")
        self.assertEqual(cm.exception.code, 404)

    def _token(self):
        page = self.get("/")[2]
        return page.split(b'name="omniqueue-token" content="')[1].split(b'"')[0].decode()

    def test_projects_endpoints(self):
        status, ctype, body = self.get("/api/projects")
        self.assertEqual(status, 200)
        pr = json.loads(body)
        self.assertTrue(pr["enabled"])
        by = {p["project"]: p for p in pr["projects"]}
        self.assertIn("naiss2025-1-42", by)
        self.assertGreater(by["naiss2025-1-42"]["usage"]["30"]["core_h"], 0)
        self.assertEqual(by["naiss2025-1-42"]["quota"]["source"], "config")
        self.assertEqual(by["naiss2025-9-9"]["error_kind"], "network")
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/projects/refresh", method="POST",
                                     headers={"X-OmniQueue-Token": self._token()})
        with urllib.request.urlopen(req) as r:
            self.assertEqual(json.loads(r.read())["started"], True)
        # the index page carries the projects section and the (hidden) experimental controls
        page = self.get("/")[2]
        self.assertIn(b'id="projects"', page)
        self.assertIn(b'id="predict-toggle"', page)

    def test_predict_endpoint(self):
        body = json.dumps({"nodes": 2, "hours": 3, "projects": ["naiss2025-1-42"]}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/experimental/predict", data=body, method="POST",
                                     headers={"X-OmniQueue-Token": self._token(), "Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            res = json.loads(r.read())
        self.assertEqual(res["request"]["nodes"], 2)
        self.assertTrue(res["candidates"])
        self.assertTrue(all(c["cluster"] == "tetralith" for c in res["candidates"]))
        self.assertIn("estimated_wait_h", res["candidates"][0])
        # without the CSRF token the endpoint refuses
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/experimental/predict", data=body, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 403)
        # garbage numbers -> 400
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/experimental/predict", data=b'{"nodes": "x"}', method="POST",
                                     headers={"X-OmniQueue-Token": self._token()})
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 400)

    def test_refresh(self):
        page = self.get("/")[2]
        token = page.split(b'name="omniqueue-token" content="')[1].split(b'"')[0].decode()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/refresh", method="POST",
                                     headers={"X-OmniQueue-Token": token})
        with urllib.request.urlopen(req) as r:
            self.assertEqual(json.loads(r.read())["ok"], True)


if __name__ == "__main__":
    unittest.main()
