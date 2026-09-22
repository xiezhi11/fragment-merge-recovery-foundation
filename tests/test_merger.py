import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))

from shardlog import (LogMerger, Fragment, MergeError, BatchJournal)
from shardlog.storage import checksum


def fixed_fragments(source, count, start=0):
    """Deterministic, reproducible test data."""
    return [Fragment(source, i, "payload-%s-%04d" % (source, i))
            for i in range(start, start + count)]


class Base(unittest.TestCase):
    def setUp(self):
        self.path = self.id().replace(".", "_") + ".journal"
        if os.path.exists(self.path):
            os.remove(self.path)
        self.journal = BatchJournal(self.path)
        self.merger = LogMerger(self.journal)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)


class TestIntake(Base):
    def test_out_of_order_and_gap_not_confirmed(self):
        frags = fixed_fragments("a", 4)
        for f in [frags[2], frags[0], frags[3]]:
            self.assertEqual(self.merger.accept(f), "buffered")
        view = self.merger.advance()
        # seq 1 missing: nothing past the gap may be confirmed
        self.assertEqual(view.confirmed["a"], 0)
        self.assertEqual(view.gaps["a"], [1])
        self.merger.accept(frags[1])
        view = self.merger.advance()
        self.assertEqual(view.confirmed["a"], 3)
        self.assertTrue(view.complete)

    def test_duplicate_is_idempotent(self):
        f = fixed_fragments("a", 1)[0]
        self.assertEqual(self.merger.accept(f), "buffered")
        self.assertEqual(self.merger.accept(f), "duplicate")
        self.assertEqual(self.merger.buffered["a"].__len__(), 1)

    def test_conflict_keeps_both_and_never_overwrites(self):
        f1 = Fragment("a", 0, "first")
        f2 = Fragment("a", 0, "second")
        self.merger.accept(f1)
        self.assertEqual(self.merger.accept(f2), "conflict")
        self.assertEqual(len(self.merger.conflicts), 1)
        c = self.merger.conflicts[0]
        self.assertEqual(c["kept"], "first")
        self.assertEqual(c["rejected"], "second")
        self.merger.advance()
        # confirmed content stays the first arrival
        self.assertEqual(self.merger.confirmed["a"], 0)
        recs, _ = self.journal.recover()
        self.assertEqual(recs[0]["payload"], "first")
        # late duplicate of confirmed seq never overwrites
        self.assertEqual(self.merger.accept(
            Fragment("a", 0, "evil")), "duplicate")

    def test_validation_errors_carry_source_seq_stage(self):
        bad = Fragment("srcX", 5, "tampered")
        bad.checksum = "deadbeef"
        with self.assertRaises(MergeError) as ctx:
            self.merger.accept(bad)
        err = ctx.exception
        self.assertEqual((err.stage, err.source, err.seq),
                         ("validate", "srcX", 5))
        with self.assertRaises(MergeError):
            self.merger.accept(Fragment("srcX", -1, "x"))
        with self.assertRaises(MergeError):
            self.merger.accept(Fragment("srcX", 0, "y" * 5000))

    def test_capacity_limit(self):
        m = LogMerger(BatchJournal(self.path + ".cap"), max_buffered=3)
        for i in range(3):
            m.accept(Fragment("a", i + 1, "p%d" % i))
        with self.assertRaises(MergeError) as ctx:
            m.accept(Fragment("a", 9, "overflow"))
        self.assertEqual(ctx.exception.stage, "capacity")
        os.remove(self.path + ".cap")


class TestRecovery(Base):
    def test_restart_resumes_from_last_committed(self):
        for f in fixed_fragments("a", 3):
            self.merger.accept(f)
        self.merger.advance()
        m2 = LogMerger(BatchJournal(self.path))
        res = m2.recover()
        self.assertTrue(res["complete"])
        self.assertEqual(m2.confirmed["a"], 2)
        self.assertEqual(res["restored"], 3)

    def test_partial_batch_detected_and_awaited(self):
        for f in fixed_fragments("a", 4):
            self.merger.accept(f)
        self.merger.advance()
        # simulate crash mid-batch: truncate last COMMIT line
        with open(self.path, "r") as fh:
            lines = fh.readlines()
        with open(self.path, "w") as fh:
            fh.writelines(lines[:-2])  # drop a REC and the COMMIT
        m2 = LogMerger(BatchJournal(self.path))
        res = m2.recover()
        self.assertEqual(res["restored"], 0)
        self.assertEqual(len(res["incomplete_batches"]), 1)
        self.assertTrue(res["complete"])

    def test_multiple_recoveries_idempotent(self):
        for f in fixed_fragments("a", 2):
            self.merger.accept(f)
        self.merger.advance()
        for _ in range(3):
            m = LogMerger(BatchJournal(self.path))
            m.recover()
            self.assertEqual(m.confirmed["a"], 1)


class TestReadAndStats(Base):
    def test_read_is_deterministic_and_non_mutating(self):
        frags = fixed_fragments("a", 3)
        self.merger.accept(frags[1])
        self.merger.accept(frags[0])
        self.merger.accept(frags[2])
        before = self.merger.progress().to_dict()
        r1 = self.merger.read(source="a")
        r2 = self.merger.read(source="a")
        self.assertEqual(r1, r2)  # repeat request -> same boundaries
        self.assertEqual(self.merger.progress().to_dict(), before)
        self.assertEqual(r1["a"]["confirmed"], -1)
        self.assertEqual([f["seq"] for f in r1["a"]["fragments"]],
                         [0, 1, 2])

    def test_read_gap_returns_continuable_info(self):
        frags = fixed_fragments("a", 3)
        self.merger.accept(frags[0])
        self.merger.accept(frags[2])
        self.merger.advance()
        r = self.merger.read(source="a")
        self.assertEqual(r["a"]["next_needed"], 1)
        self.assertEqual(r["a"]["gaps"], [1])

    def test_stats_read_only_and_atomic_progress(self):
        frags = fixed_fragments("a", 3)
        self.merger.accept(frags[0])
        self.merger.accept(frags[2])
        self.merger.advance()
        s1 = self.merger.stats()
        self.assertEqual(s1["gap_count"], 1)
        self.assertEqual(s1["last_confirmed"]["a"], 0)
        before = self.merger.progress().to_dict()
        self.merger.stats()
        self.assertEqual(self.merger.progress().to_dict(), before)
        # confirmed + buffered + gaps in one response
        view = self.merger.progress().to_dict()
        self.assertIn("confirmed", view)
        self.assertIn("buffered", view)
        self.assertIn("gaps", view)

    def test_read_by_time_range_uses_logical_clock(self):
        ticks = iter([10, 20, 30])
        m = LogMerger(BatchJournal(self.path),
                      clock=lambda: next(ticks, 99))
        for f in fixed_fragments("a", 3):
            m.accept(f)
        r = m.read(source="a", since=15, until=25)
        self.assertEqual([f["seq"] for f in r["a"]["fragments"]], [1])


class TestReplayAndSources(Base):
    def test_replay_only_touches_target_source(self):
        for f in fixed_fragments("a", 2):
            self.merger.accept(f)
        b = fixed_fragments("b", 3)
        self.merger.accept(b[0])
        self.merger.accept(b[2])
        self.merger.advance()
        order_before = list(self.merger.output_order)
        rep = self.merger.replay("b")
        self.assertEqual(rep["gaps"], [1])
        self.assertTrue(rep["complete"])
        self.assertEqual(self.merger.confirmed["a"], 1)
        self.assertEqual(self.merger.output_order, order_before)

    def test_repeated_replay_stable(self):
        b = fixed_fragments("b", 3)
        self.merger.accept(b[0])
        self.merger.accept(b[2])
        self.merger.advance()
        r1 = self.merger.replay("b")
        r2 = self.merger.replay("b")
        self.assertEqual(r1, r2)

    def test_pause_and_resume_source(self):
        self.merger.set_source_status("a", "paused")
        with self.assertRaises(MergeError):
            self.merger.accept(Fragment("a", 0, "x"))
        self.merger.set_source_status("a", "active")
        self.assertEqual(self.merger.accept(Fragment("a", 0, "x")),
                         "buffered")

    def test_classify_expired_and_jump(self):
        for f in fixed_fragments("a", 2):
            self.merger.accept(f)
        self.merger.advance()
        r = self.merger.classify_incoming("a", 1)
        self.assertEqual(r["manual"]["reason"], "expired")
        self.assertIsNone(r["receivable"])
        r = self.merger.classify_incoming("a", 10 ** 9)
        self.assertEqual(r["manual"]["reason"], "sequence_jump")
        r = self.merger.classify_incoming("a", 2)
        self.assertIsNone(r["manual"])
        self.assertEqual(r["receivable"]["from"], 2)


class TestStorageFormat(Base):
    def test_header_records_algo_and_version(self):
        with open(self.path) as fh:
            header = json.loads(fh.readline())
        self.assertEqual(header["checksum_algo"], "crc32")
        self.assertEqual(header["protocol_version"], 1)

    def test_migration_migrates_complete_first(self):
        for f in fixed_fragments("a", 3):
            self.merger.accept(f)
        self.merger.advance()
        new_path = self.path + ".v2"
        count, incomplete = self.journal.migrate(new_path, 2)
        self.assertEqual(count, 3)
        self.assertEqual(incomplete, [])
        j2 = BatchJournal(new_path)
        self.assertEqual(j2.protocol_version, 2)
        recs, _ = j2.recover()
        self.assertEqual([r["seq"] for r in recs], [0, 1, 2])
        os.remove(new_path)

    def test_error_offset_accuracy(self):
        for f in fixed_fragments("a", 2):
            self.merger.accept(f)
        self.merger.advance()
        with open(self.path, "a") as fh:
            fh.write(json.dumps({"t": "BEGIN", "batch": 1,
                                 "count": 1}) + "\n")
        _, incomplete = self.journal.recover()
        self.assertEqual(incomplete[0].batch_id, 1)
        self.assertGreater(incomplete[0].offset, 0)


if __name__ == "__main__":
    unittest.main()
