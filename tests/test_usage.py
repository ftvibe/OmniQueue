import time
import unittest

from omniqueue.config import ClusterConfig, Config
from omniqueue.models import Job
from omniqueue.usage import own_usage

NOW = time.mktime((2026, 9, 18, 12, 0, 0, 0, 0, -1))


def _t(hours_ago: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(NOW - hours_ago * 3600))


def _cfg(**kw) -> Config:
    return Config(clusters=[ClusterConfig(name="c", host="c", **kw)])


JOBS = [
    Job("c", "1", "a", "COMPLETED", account="proj", partition="main", nodes=2, cpus=64, start_time=_t(30), end_time=_t(28)),  # 2 h x 64
    Job("c", "2", "b", "RUNNING", account="proj", partition="main", nodes=1, cpus=32, start_time=_t(1)),  # 1 h x 32 so far
    Job("c", "3", "g", "COMPLETED", account="proj", partition="gpu", nodes=1, cpus=16, gpus=4, start_time=_t(10), end_time=_t(8)),  # 2 h x 4 GPUs
    Job("c", "4", "w", "PENDING", account="proj", partition="gpu", nodes=1, cpus=16, gpus=2),
    Job("c", "5", "old", "COMPLETED", account="proj", partition="main", nodes=1, cpus=32, start_time=_t(900), end_time=_t(890)),
    Job("c", "6", "other", "COMPLETED", account="other-proj", partition="main", nodes=1, cpus=32, start_time=_t(5), end_time=_t(4)),
]


class OwnUsageTests(unittest.TestCase):
    def test_split_and_windows(self):
        recs = own_usage(_cfg(project_quotas={"proj": 1000}, project_gpu_quotas={"proj": 50}, project_pis={"proj": "Prof. X"}), {"c": JOBS}, now=NOW)
        by = {r["account"]: r for r in recs}
        p = by["proj"]
        self.assertEqual(recs[0]["account"], "proj")  # the one with running jobs first
        self.assertEqual(p["pi"], "Prof. X")
        self.assertAlmostEqual(p["usage"]["7"]["cpu"]["core_h"], 2 * 64 + 32, delta=0.5)
        self.assertEqual(p["usage"]["7"]["cpu"]["jobs"], 2)
        self.assertEqual(p["usage"]["30"]["cpu"]["jobs"], 2)  # job 5 is 37 days old
        self.assertAlmostEqual(p["usage"]["7"]["gpu"]["gpu_h"], 8, delta=0.05)
        self.assertEqual(p["usage"]["7"]["gpu"]["jobs"], 1)
        self.assertTrue(p["has_gpu"])
        self.assertEqual(p["gpu_partitions"], ["gpu"])  # learned from the job with GPUs
        self.assertEqual(p["running"]["cpu"], {"jobs": 1, "cores": 32.0, "nodes": 1})
        self.assertEqual(p["pending"]["gpu"]["gpus"], 2)
        self.assertEqual(len(p["daily"]), 30)
        self.assertGreater(p["daily"][-1]["core_h"], 0)
        self.assertAlmostEqual(p["quota"]["used_h"], p["usage"]["30"]["cpu"]["core_h"])
        self.assertAlmostEqual(p["gpu_quota"]["used_h"], 8, delta=0.05)
        self.assertEqual(p["jobs_known"], 5)
        o = by["other-proj"]
        self.assertTrue(o["has_gpu"])  # the cluster has a GPU partition, so the GPU rows show (at zero)
        self.assertAlmostEqual(o["usage"]["7"]["cpu"]["core_h"], 32, delta=0.5)
        self.assertIsNone(o["quota"])

    def test_gpu_factor_and_partition_config(self):
        cfg = _cfg(gpu_hour_factor=0.5, gpu_partitions=["accel"])
        jobs = JOBS + [Job("c", "7", "unk", "COMPLETED", account="proj", partition="accel", nodes=1, cpus=8, start_time=_t(3), end_time=_t(2))]
        p = {r["account"]: r for r in own_usage(cfg, {"c": jobs}, now=NOW)}["proj"]
        self.assertAlmostEqual(p["usage"]["7"]["gpu"]["gpu_h"], 4, delta=0.05)  # 8 unit-hours x 0.5
        self.assertEqual(p["usage"]["7"]["gpu"]["jobs"], 2)  # the accel job counts as GPU (configured partition), 0 GPU-h known
        self.assertEqual(p["pending"]["gpu"]["gpus"], 2)  # in-use counts stay in Slurm units
        self.assertEqual(p["gpu_partitions"], ["accel", "gpu"])

    def test_whole_node_gpu_jobs_without_gres(self):
        # Dardel-style: a job on the gpu partition allocates whole nodes and never asks for a gres,
        # so Slurm's TRES has no gres/gpu; sinfo says the partition has 4 GPUs per node
        jobs = [Job("c", "9", "wn", "COMPLETED", account="proj", partition="gpu", nodes=2, cpus=128, start_time=_t(4), end_time=_t(2)),
                Job("c", "10", "run", "RUNNING", account="proj", partition="gpu", nodes=1, cpus=64, start_time=_t(1))]
        p = own_usage(_cfg(), {"c": jobs}, now=NOW, gpus_per_node={"c": {"gpu": 4, "main": 0}})[0]
        self.assertEqual(p["gpu_partitions"], ["gpu"])
        self.assertEqual(p["usage"]["7"]["gpu"]["jobs"], 2)
        self.assertEqual(p["usage"]["7"]["cpu"]["jobs"], 0)
        self.assertAlmostEqual(p["usage"]["7"]["gpu"]["gpu_h"], 2 * 2 * 4 + 1 * 4, delta=0.05)
        self.assertEqual(p["running"]["gpu"]["gpus"], 4)
        self.assertEqual(p["gpus_per_node"], {"gpu": 4})
        # without the sinfo knowledge the same jobs would count as CPU jobs
        q = own_usage(_cfg(), {"c": jobs}, now=NOW)[0]
        self.assertEqual(q["usage"]["7"]["cpu"]["jobs"], 2)
        # ... unless the config states the size (Dardel: sinfo shows no gres for the gpu partition)
        r = own_usage(_cfg(gpus_per_node={"gpu": 4}), {"c": jobs}, now=NOW)[0]
        self.assertEqual(r["usage"]["7"]["gpu"]["jobs"], 2)
        self.assertAlmostEqual(r["usage"]["7"]["gpu"]["gpu_h"], 20, delta=0.05)
        # and the config wins over a sinfo that reports a smaller/absent gres
        t = own_usage(_cfg(gpus_per_node={"gpu": 4}), {"c": jobs}, now=NOW, gpus_per_node={"c": {"gpu": 0}})[0]
        self.assertAlmostEqual(t["usage"]["7"]["gpu"]["gpu_h"], 20, delta=0.05)

    def test_threads_per_core(self):
        p = own_usage(_cfg(), {"c": JOBS}, now=NOW, tpc={"c": {"main": 2}})[0]
        self.assertAlmostEqual(p["usage"]["7"]["cpu"]["core_h"], (2 * 64 + 32) / 2, delta=0.5)

    def test_user_mode_has_no_project_clusters(self):
        cfg = Config(clusters=[ClusterConfig(name="c", host="c", projects=["proj"])], mode="user")
        self.assertEqual(cfg.project_clusters, [])
        cfg.mode = "pi"
        self.assertEqual([c.name for c in cfg.project_clusters], ["c"])


if __name__ == "__main__":
    unittest.main()


class CompanionAccountUsageTests(unittest.TestCase):
    def test_gpu_companion_folds_into_project(self):
        jobs = JOBS + [Job("c", "4", "me", "COMPLETED", account="proj-gpu", partition="gpu", nodes=1, cpus=16, gpus=4,
                           start_time=_t(6), end_time=_t(5))]
        recs = own_usage(_cfg(), {"c": jobs}, now=NOW)
        self.assertEqual(sorted(r["account"] for r in recs), ["other-proj", "proj"])
        proj = next(r for r in recs if r["account"] == "proj")
        self.assertEqual(proj["accounts"], ["proj", "proj-gpu"])
        self.assertEqual(proj["usage"]["30"]["gpu"]["jobs"], 2)
        off = own_usage(_cfg(project_gpu_suffix=""), {"c": jobs}, now=NOW)
        self.assertEqual(sorted(r["account"] for r in off), ["other-proj", "proj", "proj-gpu"])
