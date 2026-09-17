import json
import tempfile
import unittest
from pathlib import Path

from omniqueue.history import HistoryStore
from omniqueue.models import Job


def job(cluster, jid, state, seen):
    return Job(cluster=cluster, job_id=jid, name=f"j{jid}", state=state, last_seen=seen)


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "h.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_roundtrip_and_prune(self):
        store = HistoryStore(self.path, retention_days=1)
        now = 1_000_000.0
        store.update_cluster("a", [job("a", "1", "COMPLETED", now), job("a", "2", "RUNNING", now)], now=now)
        store.save()
        again = HistoryStore(self.path, retention_days=1)
        self.assertEqual({j.job_id for j in again.jobs_for("a")}, {"1", "2"})
        self.assertTrue(all(j.source == "history" for j in again.jobs_for("a")))
        # two days later, an unrelated poll prunes old entries
        later = now + 2 * 86400
        again.update_cluster("a", [job("a", "3", "PENDING", later)], now=later)
        self.assertEqual({j.job_id for j in again.jobs_for("a")}, {"3"})

    def test_vanished_marking(self):
        store = HistoryStore(self.path)
        now = 1_000_000.0
        store.update_cluster("a", [job("a", "1", "RUNNING", now), job("a", "2", "COMPLETED", now)], now=now)
        view = {j.job_id: j for j in store.update_cluster("a", [], now=now + 60)}
        self.assertEqual(view["1"].state, "VANISHED")
        self.assertTrue(view["1"].end_time)
        self.assertEqual(view["2"].state, "COMPLETED")  # terminal jobs are left alone

    def test_clusters_are_independent(self):
        store = HistoryStore(self.path)
        store.update_cluster("a", [job("a", "1", "RUNNING", 1.0)], now=1.0)
        store.update_cluster("b", [job("b", "1", "PENDING", 1.0)], now=1.0)
        self.assertEqual(store.jobs_for("a")[0].state, "RUNNING")
        self.assertEqual(len(store.all_jobs()), 2)
        self.assertTrue(store.forget("a:1"))
        self.assertFalse(store.forget("a:1"))

    def test_corrupt_file_is_ignored(self):
        self.path.write_text("{not json")
        store = HistoryStore(self.path)
        self.assertEqual(store.all_jobs(), [])
        store.save()
        self.assertEqual(json.loads(self.path.read_text())["jobs"], [])


if __name__ == "__main__":
    unittest.main()
