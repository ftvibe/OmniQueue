import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from omniqueue.collector import Collector
from omniqueue.config import ClusterConfig, Config, ConfigError, config_from_dict
from omniqueue.history import HistoryStore
from omniqueue.predict import Request, explain, predict
from omniqueue.projects import ProjectPoller, ProjectStore, slurm_ts
from omniqueue.slurm import parse_project_queue, parse_project_sacct, parse_sshare, project_command

NOW = time.mktime((2026, 9, 18, 12, 0, 0, 0, 0, -1))


def _t(hours_ago: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(NOW - hours_ago * 3600))


QUEUE_OUT = f"""\
501|proj-a|alice|RUNNING|main|4|128|1-00:00:00|3:10:00
502|proj-a|bob|PENDING|main|2|64|12:00:00|0:00
503_[1-20]|proj-a|bob|PENDING|main|1|32|4:00:00|0:00
504|proj-b|carol|RUNNING|main|1|32|1:00:00|0:30:00
"""
SACCT_OUT = f"""\
401|proj-a|alice|main|COMPLETED|2|64|7200|460800|{_t(30)}|{_t(29)}|{_t(27)}|04:00:00
402|proj-a|bob|main|FAILED|1|32|3600|115200|{_t(10)}|{_t(9)}|{_t(8)}|02:00:00
501|proj-a|alice|main|RUNNING|4|128|11400|1459200|{_t(5)}|{_t(3.17)}|Unknown|1-00:00:00
403|proj-a|alice|main|COMPLETED|1|32|36000|1152000|{_t(900)}|{_t(890)}|{_t(880)}|12:00:00
"""
SSHARE_OUT = """\
proj-a||1|0.010000|3000000|0.012000|0.612345|cpu=6000000|cpu=1800000|cpu=0
 proj-a|alice|1|0.005000|2000000|0.008000|0.412345|||
 proj-a|bob|1|0.005000|1000000|0.004000|0.812345|||
"""
SINFO_OUT = """\
main*|up|10|allocated|320/0/0/320|1-00:00:00|2:16:1|32
main*|up|3|idle|0/96/0/96|1-00:00:00|2:16:1|32
"""
SQUEUE_ALL_OUT = "501|main|RUNNING|4|128\n502|main|PENDING|2|64\n503_[1-20]|main|PENDING|1|32\n"


class ParserTests(unittest.TestCase):
    def test_command(self):
        cmd = project_command(["proj-a", "proj-b"], 48, ["main"])
        self.assertIn("--account=proj-a,proj-b", cmd)
        self.assertIn("--accounts=proj-a,proj-b", cmd)
        self.assertIn("sshare --noheader --parsable2 --all", cmd)
        self.assertIn("--allusers", cmd)
        for name in ("squeue_proj", "sacct_proj", "sshare", "sinfo", "squeue_all"):
            self.assertIn(f"@@OMNIQUEUE {name} rc=$?", cmd)

    def test_queue(self):
        rows = parse_project_queue(QUEUE_OUT)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[2]["tasks"], 20)
        self.assertEqual(rows[0]["time_limit_s"], 86400)
        self.assertEqual(rows[0]["elapsed_s"], 3 * 3600 + 600)

    def test_sacct(self):
        rows = parse_project_sacct(SACCT_OUT)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["cpu_s"], 460800)
        self.assertEqual(rows[2]["end"], "")  # Unknown -> empty
        self.assertEqual(rows[2]["state"], "RUNNING")

    def test_sshare(self):
        rows = parse_sshare(SSHARE_OUT)
        self.assertEqual(rows[0]["user"], "")
        self.assertAlmostEqual(rows[0]["fairshare"], 0.612345)
        self.assertEqual(rows[0]["grp_tres_mins"], {"cpu": 6000000})
        self.assertEqual(rows[1]["account"], "proj-a")
        self.assertEqual(rows[2]["user"], "bob")


class StoreTests(unittest.TestCase):
    def _store(self, tmp: str) -> ProjectStore:
        store = ProjectStore(Path(tmp) / "p.json", retention_days=90)
        from omniqueue.slurm import parse_load

        store.record_poll("c1", ["proj-a"], NOW, parse_project_sacct(SACCT_OUT), parse_project_queue(QUEUE_OUT),
                          parse_sshare(SSHARE_OUT), parse_load(SINFO_OUT, SQUEUE_ALL_OUT))
        return store

    def test_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(tmp)
            s = store.summary("c1", "proj-a", NOW, me="alice", quota_core_h=10000)
            # 7-day window: job 401 (2 h x 64), 402 (1 h x 32), the running 501 (3.17 h x 128)
            self.assertAlmostEqual(s["usage"]["7"]["users"]["alice"]["core_h"], 2 * 64 + 3.17 * 128, delta=1)
            self.assertAlmostEqual(s["usage"]["7"]["users"]["bob"]["core_h"], 32, delta=0.5)
            self.assertEqual(s["usage"]["7"]["jobs"], 3)
            self.assertEqual(s["usage"]["30"]["jobs"], 3)  # job 403 is 37 days old
            self.assertEqual(s["running"], {"jobs": 1, "cores": 128.0, "nodes": 4, "users": {"alice": {"jobs": 1, "cores": 128.0}}})
            self.assertEqual(s["pending"]["jobs"], 21)  # 1 + 20 array tasks
            self.assertEqual(s["pending"]["cores"], 64 + 20 * 32)
            self.assertEqual(s["users"][0], "alice")
            self.assertAlmostEqual(s["shares"]["fairshare"], 0.612345)
            self.assertAlmostEqual(s["shares"]["users"]["alice"]["fairshare"], 0.412345)
            self.assertEqual(s["quota"]["source"], "config")
            self.assertAlmostEqual(s["quota"]["used_core_h"], s["usage"]["30"]["core_h"])
            self.assertEqual(len(s["daily"]), 30)
            self.assertGreater(s["daily"][-1]["core_h"], 0)
            # without a configured quota the sshare group limit is used
            s2 = store.summary("c1", "proj-a", NOW)
            self.assertEqual(s2["quota"]["source"], "sshare")
            self.assertEqual(s2["quota"]["limit_core_h"], 100000)
            self.assertEqual(s2["quota"]["used_core_h"], 30000)

    def test_persist_prune_and_load_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = self._store(tmp)
            store.save()
            self.assertEqual(stat.S_IMODE(os.stat(Path(tmp) / "p.json").st_mode), 0o600)
            again = ProjectStore(Path(tmp) / "p.json", retention_days=90)
            self.assertEqual(again.last_poll("c1"), NOW)
            self.assertEqual(set(again.jobs("c1", "proj-a")), {"401", "402", "501", "403"})
            samples = again.load_samples("c1")["main"]
            self.assertEqual(len(samples), 1)
            self.assertEqual(samples[0]["idle"], 3)
            self.assertEqual(samples[0]["pending_nodes"], 2 + 20)
            self.assertEqual(again.typical_hours("c1", "main"), 2.0)  # median of 2 h, 1 h, 10 h
            # a poll 100 days later prunes the old jobs and samples
            again.record_poll("c1", ["proj-a"], NOW + 100 * 86400, [], [], [], [])
            self.assertEqual(again.jobs("c1", "proj-a"), {})
            self.assertEqual(again.load_samples("c1")["main"], [])


def _fake_bin(dirpath: Path, name: str, body: str) -> None:
    p = dirpath / name
    p.write_text("#!/bin/sh\n" + body)
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


class PollerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.bin = Path(self.tmp.name) / "bin"
        self.bin.mkdir()
        self.env = mock.patch.dict(os.environ, {"PATH": f"{self.bin}:/usr/bin:/bin", "USER": "alice"})
        self.env.start()
        _fake_bin(self.bin, "squeue",
                  "case \"$*\" in\n"
                  f"  *--account=*) cat <<'X'\n{QUEUE_OUT}X\n ;;\n"
                  f"  *--states=RUNNING,PENDING*) cat <<'X'\n{SQUEUE_ALL_OUT}X\n ;;\n"
                  "  *) exit 0 ;;\n"
                  "esac\n")
        _fake_bin(self.bin, "sacct", f"cat <<'X'\n{SACCT_OUT}X\n")
        _fake_bin(self.bin, "sshare", f"cat <<'X'\n{SSHARE_OUT}X\n")
        _fake_bin(self.bin, "sinfo", f"cat <<'X'\n{SINFO_OUT}X\n")

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def _poller(self, **kw):
        cfg = Config(clusters=[ClusterConfig(name="here", host="local", projects=["proj-a"], **kw)],
                     data_dir=Path(self.tmp.name), persist_connections=False, project_refresh_seconds=3600)
        collector = Collector(cfg, HistoryStore(Path(self.tmp.name) / "h.json"))
        store = ProjectStore(Path(self.tmp.name) / "p.json")
        return ProjectPoller(cfg, store, collector), store, collector

    def test_poll_and_snapshot(self):
        poller, store, _ = self._poller(project_quotas={"proj-a": 5000})
        self.assertEqual([c.name for c in poller.due(time.time() + 10)], ["here"])
        poller.refresh([poller.config.clusters[0]])
        st = poller.status["here"]
        self.assertIsNone(st["error"], st)
        self.assertAlmostEqual(st["next_poll"] - st["last_poll"], 3600, delta=5)
        self.assertEqual(poller.due(), [])  # not due again for an hour
        snap = poller.snapshot()
        self.assertTrue(snap["enabled"])
        proj = snap["projects"][0]
        self.assertEqual(proj["project"], "proj-a")
        self.assertEqual(proj["me"], "alice")
        self.assertEqual(proj["running"]["jobs"], 1)
        self.assertEqual(proj["quota"]["limit_core_h"], 5000)
        self.assertTrue((Path(self.tmp.name) / "p.json").exists())
        # the request for a refresh makes it due at once and bumps the etag
        etag = poller.etag()
        self.assertTrue(poller.request_refresh())
        self.assertEqual([c.name for c in poller.due()], ["here"])
        poller.refresh()
        self.assertNotEqual(poller.etag(), etag)

    def test_login_gate(self):
        poller, _, collector = self._poller()
        with mock.patch.object(collector, "needs_login", return_value=True):
            poller.refresh([poller.config.clusters[0]])
        self.assertEqual(poller.status["here"]["error_kind"], "login")
        self.assertIsNone(poller.store.last_poll("here"))

    def test_prediction_data(self):
        poller, _, _ = self._poller(nice=1000)
        poller.refresh([poller.config.clusters[0]])
        data = poller.prediction_data()
        here = data["clusters"]["here"]
        self.assertEqual(here["nice"], 1000)
        self.assertEqual(here["partitions"]["main"]["total_nodes"], 13)
        self.assertEqual(here["partitions"]["main"]["cores_per_node"], 32)
        self.assertAlmostEqual(here["projects"]["proj-a"]["fairshare_me"], 0.412345)

    def test_disabled_without_projects(self):
        cfg = Config(clusters=[ClusterConfig(name="here", host="local")], data_dir=Path(self.tmp.name), persist_connections=False)
        poller = ProjectPoller(cfg, ProjectStore(Path(self.tmp.name) / "p.json"), Collector(cfg, HistoryStore(Path(self.tmp.name) / "h.json")))
        self.assertFalse(poller.enabled)
        self.assertFalse(poller.request_refresh())
        poller.start()  # a no-op
        self.assertIsNone(poller._thread)


class ConfigTests(unittest.TestCase):
    def test_project_keys(self):
        cfg = config_from_dict({"project_refresh_seconds": 3600, "clusters": [
            {"name": "a", "host": "a", "projects": ["p1", "p2"], "project_quotas": {"p1": 5000}, "project_refresh_seconds": 900, "nice": 10},
            {"name": "b", "host": "b"},
        ]})
        self.assertEqual([c.name for c in cfg.project_clusters], ["a"])
        self.assertEqual(cfg.project_interval(cfg.clusters[0]), 900)
        self.assertEqual(cfg.project_interval(cfg.clusters[1]), 3600)
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"name": "a", "host": "a", "projects": ["bad;rm -rf"]}]})
        with self.assertRaises(ConfigError):
            config_from_dict({"clusters": [{"name": "a", "host": "a", "project_quotas": {"p": -1}}]})
        with self.assertRaises(ConfigError):
            config_from_dict({"project_refresh_seconds": 10, "clusters": [{"name": "a", "host": "a"}]})


def _samples(n, idle, pending_nodes, total=100, step=7200, tpc=1):
    return [{"ts": NOW - k * step, "idle": idle, "mixed": 0, "allocated": total - idle, "unavailable": 0, "total": total,
             "free_cores": idle * 32, "total_cores": total * 32, "pending_jobs": pending_nodes, "pending_nodes": pending_nodes,
             "running_jobs": 10, "time_limit_s": 3 * 86400, "tpc": tpc, "cpus_per_node": 32 * tpc} for k in range(n)]


class PredictorTests(unittest.TestCase):
    def _data(self):
        return {"now": NOW, "clusters": {
            "busy": {"nice": 0, "interval": 7200, "own_waits": [], "color": None,
                     "partitions": {"main": {"samples": _samples(40, idle=0, pending_nodes=300), "time_limit_s": 3 * 86400,
                                             "total_nodes": 100, "cores_per_node": 32, "typical_hours": 8.0}},
                     "projects": {"p-busy": {"fairshare_me": 0.2, "fairshare_account": 0.3, "quota": None, "running_cores": 0, "pending_cores": 0}}},
            "free": {"nice": 0, "interval": 7200, "color": None,
                     "own_waits": [{"partition": "main", "nodes": 2, "wait_s": 600, "start_ts": NOW - 86400}] * 3,
                     "partitions": {"main": {"samples": _samples(40, idle=20, pending_nodes=5), "time_limit_s": 86400,
                                             "total_nodes": 100, "cores_per_node": 32, "typical_hours": 4.0},
                                    "short": {"samples": _samples(40, idle=50, pending_nodes=0), "time_limit_s": 3600,
                                              "total_nodes": 60, "cores_per_node": 32, "typical_hours": 0.5}},
                     "projects": {"p-free": {"fairshare_me": 0.9, "fairshare_account": 0.9,
                                             "quota": {"limit_core_h": 1000, "used_core_h": 500}, "running_cores": 0, "pending_cores": 0}}},
        }}

    def test_ranking(self):
        res = predict(Request(nodes=2, hours=4), self._data())
        names = [(c["cluster"], c["partition"]) for c in res["candidates"]]
        self.assertEqual(names[0], ("free", "main"))
        best = res["candidates"][0]
        self.assertEqual(best["immediate_probability"], 1.0)
        self.assertEqual(best["confidence"], "good")
        self.assertEqual(best["factors"]["history_n"], 3)
        self.assertTrue(any(r.startswith("quota nearly used") for r in best["reasons"]))  # 500 of 1000 core-h left, job needs 256
        busy = next(c for c in res["candidates"] if c["cluster"] == "busy")
        self.assertGreater(busy["estimated_wait_h"], 10)  # 3x pressure x 8 h typical x poor fairshare
        self.assertEqual(busy["immediate_probability"], 0.0)
        excluded = {(c["cluster"], c["partition"]): c["excluded"] for c in res["excluded"]}
        self.assertIn("time limit", excluded[("free", "short")])
        text = explain(res)
        self.assertIn("1. free/main [p-free]", text)
        self.assertIn("x  free/short", text)

    def test_quota_excludes_and_filters(self):
        res = predict(Request(nodes=2, hours=20), self._data())  # 2 x 32 x 20 = 1280 core-h > 500 left
        excluded = {(c["cluster"], c["partition"]): c["excluded"] for c in res["excluded"]}
        self.assertIn("quota", excluded[("free", "main")])
        only_busy = predict(Request(nodes=1, hours=1, clusters=["busy"]), self._data())
        self.assertEqual({c["cluster"] for c in only_busy["candidates"]}, {"busy"})
        big = predict(Request(nodes=500, hours=1), self._data())
        self.assertEqual(big["candidates"], [])
        self.assertTrue(all("nodes in the partition" in c["excluded"] for c in big["excluded"]))

    def test_nice_and_stale(self):
        data = self._data()
        base = predict(Request(nodes=2, hours=4), data)["candidates"]
        data["clusters"]["busy"]["nice"] = 5000
        niced = predict(Request(nodes=2, hours=4), data)["candidates"]
        b0 = next(c for c in base if c["cluster"] == "busy")["estimated_wait_h"]
        b1 = next(c for c in niced if c["cluster"] == "busy")["estimated_wait_h"]
        self.assertAlmostEqual(b1 / b0, 2.0, places=1)
        for s in data["clusters"]["busy"]["partitions"]["main"]["samples"]:
            s["ts"] -= 5 * 86400
        stale = next(c for c in predict(Request(nodes=2, hours=4), data)["candidates"] if c["cluster"] == "busy")
        self.assertEqual(stale["confidence"], "low")
        self.assertIn("load sample is stale", stale["reasons"])

    def test_empty(self):
        res = predict(Request(), {"now": NOW, "clusters": {"x": {"partitions": {}, "projects": {}}}})
        self.assertEqual(res["candidates"], [])
        self.assertTrue(res["notes"])
        self.assertIn("no candidates", explain(res))


class TimeTests(unittest.TestCase):
    def test_slurm_ts(self):
        self.assertEqual(slurm_ts(_t(0)), NOW)
        self.assertIsNone(slurm_ts("Unknown"))
        self.assertIsNone(slurm_ts(""))


if __name__ == "__main__":
    unittest.main()
