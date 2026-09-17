import unittest

from omniqueue.slurm import combined_command, load_command, parse_load, summarize_load

SINFO = """\
main*|up|1500|allocated|48000/0/0/48000|3-00:00:00|2:16:1
main*|up|120|idle|0/3840/0/3840|3-00:00:00|2:16:1
main*|up|40|mixed|640/640/0/1280|3-00:00:00|2:16:1
main*|up|12|drained|0/0/384/384|3-00:00:00|2:16:1
main*|up|3|down*|0/0/96/96|3-00:00:00|2:16:1
gpu|up|30|allocated|3840/0/0/3840|1-00:00:00|2:64:1
gpu|up|2|idle|0/256/0/256|1-00:00:00|2:64:1
debug|down|4|idle|0/128/0/128|30:00|2:16:1
"""
# LUMI-style: Slurm counts two hardware threads per core
SINFO_THREADS = """\
small|up|2|idle|0/512/0/512|3-00:00:00|2:64:2
small|up|4|mixed|124/900/0/1024|3-00:00:00|2:64:2
"""
SQUEUE_ALL = """\
main|RUNNING|4|128
main|RUNNING|1|32
main|PENDING|8|256
main|PENDING|2|64
gpu|PENDING|1|128
main,gpu|PENDING|1|32
"""


class LoadParsing(unittest.TestCase):
    def test_partitions(self):
        parts = {p["partition"]: p for p in parse_load(SINFO, SQUEUE_ALL)}
        self.assertEqual(list(p["partition"] for p in parse_load(SINFO, SQUEUE_ALL))[0], "main")  # default first
        main = parts["main"]
        self.assertTrue(main["default"])
        self.assertEqual(main["nodes"], {"idle": 120, "mixed": 40, "allocated": 1500, "unavailable": 15, "total": 1675})
        self.assertEqual(main["cpus"]["idle"], 3840 + 640)
        self.assertEqual(main["cpus"]["allocated"], 48000 + 640)
        self.assertEqual(main["cpus"]["other"], 384 + 96)
        self.assertEqual(main["time_limit_s"], 3 * 86400)
        self.assertEqual(main["jobs"], {"running": 2, "pending": 3})
        self.assertEqual(main["pending_nodes"], 8 + 2 + 1)
        self.assertEqual(main["running_nodes"], 5)
        gpu = parts["gpu"]
        self.assertFalse(gpu["default"])
        self.assertEqual(gpu["jobs"]["pending"], 2)  # incl. the multi-partition job
        self.assertEqual(parts["debug"]["avail"], "down")
        self.assertEqual(parts["debug"]["time_limit_s"], 1800)

    def test_summary(self):
        summary = summarize_load(parse_load(SINFO, SQUEUE_ALL))
        self.assertEqual(summary["nodes_idle"], 120 + 2 + 4)
        self.assertEqual(summary["jobs_pending"], 3 + 2)
        self.assertAlmostEqual(summary["utilisation"], (48640 + 3840) / (48000 + 3840 + 1280 + 3840 + 256 + 128), places=6)
        self.assertIsNone(summarize_load([])["utilisation"])

    def test_threads_are_converted_to_cores(self):
        (p,) = parse_load(SINFO_THREADS, "")
        self.assertEqual(p["threads_per_core"], 2)
        self.assertEqual(p["cpus"]["idle"], 1412)  # what sinfo says
        self.assertEqual(p["cores"]["idle"], 706)  # what a human expects
        self.assertEqual(p["cores"]["total"], 768)  # 6 nodes x 128 cores
        self.assertEqual(p["nodes"]["idle"], 2)
        summary = summarize_load([p])
        self.assertEqual(summary["threads_per_core"], 2)
        self.assertEqual(summary["cores_total"], 768)
        # clusters that count cores are unchanged
        main = {q["partition"]: q for q in parse_load(SINFO, "")}["main"]
        self.assertEqual(main["threads_per_core"], 1)
        self.assertEqual(main["cores"], main["cpus"])

    def test_garbage_tolerated(self):
        self.assertEqual(parse_load("slurm_load_partitions: error\n", "nonsense"), [])

    def test_load_command_is_separate_and_filters_partitions(self):
        self.assertNotIn("sinfo", combined_command("me", 24, None, None, use_sacct=True))  # not in the poll
        cmd = load_command()
        self.assertIn("sinfo --noheader", cmd)
        self.assertIn("--states=RUNNING,PENDING", cmd)
        self.assertEqual(cmd.count("@@OMNIQUEUE"), 2)
        self.assertNotIn("--partition", cmd)
        cmd = load_command(["main", "gpu"])
        self.assertEqual(cmd.count("--partition=main,gpu"), 2)


class OnDemandLoad(unittest.TestCase):
    def test_fetch_load_uses_login_gate_and_filter(self):
        import tempfile
        from pathlib import Path
        from unittest import mock

        from omniqueue import collector as collector_mod
        from omniqueue.collector import Collector
        from omniqueue.config import ClusterConfig, Config
        from omniqueue.history import HistoryStore
        from omniqueue.ssh import CommandResult

        with tempfile.TemporaryDirectory() as tmp:
            cfg = Config(clusters=[ClusterConfig(name="a", host="a", load_partitions=["main"]),
                                   ClusterConfig(name="b", host="b", show_load=False)], data_dir=Path(tmp))
            col = Collector(cfg, HistoryStore(Path(tmp) / "h.json"))
            out = SINFO + "@@OMNIQUEUE sinfo rc=0\n" + SQUEUE_ALL + "@@OMNIQUEUE squeue_all rc=0\n"
            with mock.patch.object(collector_mod, "connection_alive", return_value=True), \
                 mock.patch.object(collector_mod, "run_on_cluster", return_value=CommandResult(out, "", 0)) as run:
                col.fetch_load()
            self.assertEqual(run.call_count, 1)  # cluster b is left out
            self.assertIn("--partition=main", run.call_args[0][1])
            snap = col.load_snapshot()
            by = {c["name"]: c for c in snap["clusters"]}
            self.assertEqual(len(by["a"]["partitions"]), 3)
            self.assertEqual(by["a"]["filter"], ["main"])
            self.assertIn("show_load", by["b"]["error"])
            self.assertIsNotNone(snap["fetched_at"])
            # the regular poll must not touch sinfo
            with mock.patch.object(collector_mod, "connection_alive", return_value=True), \
                 mock.patch.object(collector_mod, "run_on_cluster",
                                   return_value=CommandResult("@@OMNIQUEUE squeue rc=0\n@@OMNIQUEUE sacct rc=0\n", "", 0)) as run:
                col.refresh()
            for call in run.call_args_list:
                self.assertNotIn("sinfo", call[0][1])
            # not logged in -> no ssh, explained in the record
            with mock.patch.object(collector_mod, "connection_alive", return_value=False), \
                 mock.patch.object(collector_mod, "run_on_cluster") as run:
                col.fetch_load()
                run.assert_not_called()
            self.assertEqual({c["name"]: c for c in col.load_snapshot()["clusters"]}["a"]["error"], "not logged in")


if __name__ == "__main__":
    unittest.main()
