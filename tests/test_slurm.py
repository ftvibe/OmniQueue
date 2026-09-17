import unittest

from omniqueue.models import category, normalize_state
from omniqueue.slurm import (
    SACCT_FIELDS,
    SQUEUE_FIELDS,
    describe_exit,
    merge_jobs,
    parse_duration,
    parse_sacct,
    parse_squeue,
    sacct_command,
    squeue_command,
)

SQUEUE_OUT = (
    "1234567|RUNNING|x_flotr|tetralith|naiss2024-1-1|4|128|n[101-104]|None|1:02:03|1-00:00:00"
    "|2026-09-16T08:00:00|2026-09-16T09:00:00|/proj/x/run1|vasp-relax|weird|name\n"
    "1234568|PENDING|x_flotr|tetralith|naiss2024-1-1|8|256||Priority|0:00|12:00:00"
    "|2026-09-16T10:00:00|N/A|/proj/x/run2|qe-scf\n"
    "1234570_[1-3]|PD|x_flotr|tetralith|naiss2024-1-1|1|32||Resources|0:00|02:00:00"
    "|2026-09-16T10:05:00|N/A|/proj/x/arr|array-job\n"
    "slurm_load_jobs error: garbage line\n"
)

SACCT_OUT = (
    "1234560|COMPLETED|x_flotr|tetralith|naiss2024-1-1|2|64|n[5-6]|None|03:10:00|04:00:00"
    "|2026-09-15T20:00:00|2026-09-15T20:30:00|2026-09-15T23:40:00|0:0|/proj/x/done|md-npt\n"
    "1234560.batch|COMPLETED|||naiss2024-1-1|2|64|n5||03:10:00||2026-09-15T20:30:00|2026-09-15T20:30:00|2026-09-15T23:40:00|0:0||batch\n"
    "1234561|FAILED|x_flotr|tetralith|naiss2024-1-1|1|32|n7|None|00:00:12|01:00:00"
    "|2026-09-15T21:00:00|2026-09-15T21:01:00|2026-09-15T21:01:12|1:0|/proj/x/bad|crashy\n"
    "1234562|CANCELLED by 12345|x_flotr|tetralith|naiss2024-1-1|1|32|None assigned|None|00:00:00|01:00:00"
    "|2026-09-15T21:00:00|Unknown|2026-09-15T21:30:00|0:0|/proj/x/c|cancelled-one\n"
    "1234563|TIMEOUT|x_flotr|tetralith|naiss2024-1-1|1|32|n8|None|01:00:05|01:00:00"
    "|2026-09-15T21:00:00|2026-09-15T21:01:00|2026-09-15T22:01:05|0:0|/proj/x/t|slow\n"
    "1234564|OUT_OF_MEMORY|x_flotr|tetralith|naiss2024-1-1|1|32|n9|None|00:10:00|01:00:00"
    "|2026-09-15T21:00:00|2026-09-15T21:01:00|2026-09-15T21:11:00|0:125|/proj/x/o|hungry\n"
    "1234567|RUNNING|x_flotr|tetralith|naiss2024-1-1|4|128|n[101-104]|None|01:00:00|1-00:00:00"
    "|2026-09-16T08:00:00|2026-09-16T09:00:00|Unknown|0:0|/proj/x/run1|vasp-relax|weird|name\n"
)


class ArrayInfoTests(unittest.TestCase):
    def test_array_info(self):
        from omniqueue.models import Job, array_info

        self.assertEqual(array_info("1234"), (None, 1))
        self.assertEqual(array_info("1234_7"), ("1234", 1))
        self.assertEqual(array_info("1234_[5-100%4]"), ("1234", 96))
        self.assertEqual(array_info("12_[1,3,5-6]"), ("12", 4))
        d = Job(cluster="c", job_id="55_[1-10]", name="arr", state="PENDING").to_dict()
        self.assertEqual((d["array_job_id"], d["array_tasks"]), ("55", 10))
        self.assertIsNone(Job(cluster="c", job_id="55", name="x", state="RUNNING").to_dict()["array_job_id"])
        # round trip through the history store keeps only real fields
        self.assertEqual(Job.from_dict(d).job_id, "55_[1-10]")


class DurationTests(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(parse_duration("1-02:03:04"), 93784)
        self.assertEqual(parse_duration("02:03:04"), 7384)
        self.assertEqual(parse_duration("03:04"), 184)
        self.assertEqual(parse_duration("0:00"), 0)
        self.assertEqual(parse_duration("00:00:12.345"), 12)

    def test_non_durations(self):
        for text in ("", "UNLIMITED", "N/A", "Partition_Limit", "garbage"):
            self.assertIsNone(parse_duration(text), text)


class StateTests(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(normalize_state("CANCELLED by 12345"), "CANCELLED")
        self.assertEqual(normalize_state("PD"), "PENDING")
        self.assertEqual(normalize_state("FAILED+"), "FAILED")
        self.assertEqual(normalize_state(""), "UNKNOWN")

    def test_category(self):
        self.assertEqual(category("RUNNING"), "running")
        self.assertEqual(category("PENDING"), "pending")
        self.assertEqual(category("COMPLETED"), "ok")
        for s in ("FAILED", "TIMEOUT", "OUT_OF_MEMORY", "CANCELLED", "NODE_FAIL"):
            self.assertEqual(category(s), "problem")
        self.assertEqual(category("VANISHED"), "unknown")


class SqueueTests(unittest.TestCase):
    def test_parse(self):
        jobs = parse_squeue(SQUEUE_OUT, "tetralith", now=1.0)
        self.assertEqual([j.job_id for j in jobs], ["1234567", "1234568", "1234570_[1-3]"])
        run = jobs[0]
        self.assertEqual(run.name, "vasp-relax|weird|name")  # pipes in the name survive
        self.assertEqual(run.state, "RUNNING")
        self.assertEqual(run.elapsed_s, 3723)
        self.assertEqual(run.time_limit_s, 86400)
        self.assertEqual(run.nodes, 4)
        self.assertEqual(run.node_list, "n[101-104]")
        self.assertEqual(run.reason, "")
        self.assertEqual(run.work_dir, "/proj/x/run1")
        pend = jobs[1]
        self.assertEqual(pend.state, "PENDING")
        self.assertEqual(pend.reason, "Priority")
        self.assertEqual(pend.start_time, "")
        self.assertEqual(jobs[2].state, "PENDING")

    def test_command(self):
        cmd = squeue_command(None)
        self.assertIn('--user="$USER"', cmd)
        self.assertTrue(cmd.endswith("%j'"), cmd)  # name is the last field
        self.assertEqual(SQUEUE_FIELDS[-1][1], "name")
        self.assertIn("--user=x_flotr", squeue_command("x_flotr", ["--partition=main"]))
        self.assertIn("--partition=main", squeue_command("x_flotr", ["--partition=main"]))


class SacctTests(unittest.TestCase):
    def test_parse(self):
        jobs = {j.job_id: j for j in parse_sacct(SACCT_OUT, "tetralith", now=1.0)}
        self.assertNotIn("1234560.batch", jobs)  # steps dropped
        self.assertEqual(jobs["1234560"].state, "COMPLETED")
        self.assertEqual(jobs["1234560"].elapsed_s, 3 * 3600 + 600)
        self.assertEqual(jobs["1234561"].exit_code, "1:0")
        self.assertEqual(describe_exit(jobs["1234561"]), "exit code 1")
        self.assertEqual(jobs["1234562"].state, "CANCELLED")
        self.assertEqual(jobs["1234562"].reason, "cancelled by uid 12345")
        self.assertEqual(jobs["1234562"].node_list, "")
        self.assertEqual(jobs["1234562"].start_time, "")
        self.assertEqual(describe_exit(jobs["1234563"]), "hit time limit")
        self.assertEqual(describe_exit(jobs["1234564"]), "out of memory")
        self.assertEqual(jobs["1234567"].name, "vasp-relax|weird|name")

    def test_command(self):
        cmd = sacct_command("flotr", 48, ["--account=abc"])
        self.assertIn("--allocations", cmd)
        self.assertIn("--parsable2", cmd)
        self.assertIn("--starttime=", cmd)
        self.assertIn("--account=abc", cmd)
        self.assertEqual(SACCT_FIELDS[-1], "JobName")


class MergeTests(unittest.TestCase):
    def test_squeue_wins_but_keeps_accounting(self):
        sq = parse_squeue(SQUEUE_OUT, "t", now=1.0)
        sa = parse_sacct(SACCT_OUT, "t", now=1.0)
        merged = {j.job_id: j for j in merge_jobs(sq, sa)}
        self.assertEqual(len(merged), 3 + 6 - 1)  # 3 from squeue, 6 from sacct, 1 overlap
        run = merged["1234567"]
        self.assertEqual(run.source, "squeue")
        self.assertEqual(run.elapsed_s, 3723)  # squeue's fresher value
        self.assertEqual(run.exit_code, "0:0")  # carried over from sacct
        self.assertEqual(merged["1234561"].source, "sacct")


if __name__ == "__main__":
    unittest.main()
