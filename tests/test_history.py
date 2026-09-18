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


class RefetchTests(unittest.TestCase):
    def test_records_without_gpus_trigger_one_refetch(self):
        import json
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "h.json"
            old = {"cluster": "c", "job_id": "1", "name": "a", "state": "COMPLETED", "last_seen": 1.0}  # no gpus field
            new = {"cluster": "d", "job_id": "2", "name": "b", "state": "COMPLETED", "last_seen": 1.0, "gpus": 0}
            path.write_text(json.dumps({"version": 1, "jobs": [old, new]}))
            store = HistoryStore(path, retention_days=10 ** 6)
            self.assertIsNone(store.covered_since("c"))  # the back-fill will run again for c ...
            store.set_covered_since("d", 5.0)
            # ... and a chunk replaces the GPU-less record while keeping fresher ones
            from omniqueue.models import Job

            changed = store.add_older("c", [Job("c", "1", "a", "COMPLETED", gpus=4, last_seen=2.0), Job("c", "9", "n", "COMPLETED", last_seen=2.0)])
            self.assertEqual(changed, 2)
            self.assertEqual({j.job_id: j.gpus for j in store.jobs_for("c")}, {"1": 4, "9": 0})
            self.assertEqual(store.add_older("c", [Job("c", "1", "a", "FAILED", last_seen=3.0)]), 0)  # fresher record kept
            store.save()
            again = HistoryStore(path, retention_days=10 ** 6)
            self.assertEqual(again.covered_since("d"), 5.0)
            self.assertEqual(again.jobs_for("c")[0].state if len(again.jobs_for("c")) == 1 else "ok", "ok")


if __name__ == "__main__":
    unittest.main()
