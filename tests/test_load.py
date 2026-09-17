import unittest

from omniqueue.slurm import combined_command, parse_load, summarize_load

SINFO = """\
main*|up|1500|allocated|48000/0/0/48000|3-00:00:00
main*|up|120|idle|0/3840/0/3840|3-00:00:00
main*|up|40|mixed|640/640/0/1280|3-00:00:00
main*|up|12|drained|0/0/384/384|3-00:00:00
main*|up|3|down*|0/0/96/96|3-00:00:00
gpu|up|30|allocated|3840/0/0/3840|1-00:00:00
gpu|up|2|idle|0/256/0/256|1-00:00:00
debug|down|4|idle|0/128/0/128|30:00
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

    def test_garbage_tolerated(self):
        self.assertEqual(parse_load("slurm_load_partitions: error\n", "nonsense"), [])

    def test_combined_command_includes_load(self):
        cmd = combined_command("me", 24, None, None, use_sacct=True, load=True)
        self.assertIn("sinfo --noheader", cmd)
        self.assertIn("--states=RUNNING,PENDING", cmd)
        self.assertEqual(cmd.count("@@OMNIQUEUE"), 4)
        self.assertNotIn("sinfo", combined_command("me", 24, None, None, use_sacct=True, load=False))


if __name__ == "__main__":
    unittest.main()
