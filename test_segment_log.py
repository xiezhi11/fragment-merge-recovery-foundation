import json
import tempfile
import unittest
from pathlib import Path

from segment_log import (
    CHECKSUM_SHA256, PROTOCOL_VERSION_1, Fragment, FragmentLog, SourceConfig,
    SourceStatus,
)


def frag(source, seq, body=None, ts=None, version=PROTOCOL_VERSION_1):
    body = body if body is not None else f"{source}-{seq}".encode()
    return Fragment.create(source, seq, body, ts if ts is not None else seq * 10,
                           version)


class SegmentLogTest(unittest.TestCase):
    def make_log(self, **config_kwargs):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        log = FragmentLog(temp.name)
        log.configure_source(SourceConfig("a", **config_kwargs))
        log.configure_source(SourceConfig("b"))
        log.save_manifest()
        return log, Path(temp.name)

    def test_out_of_order_duplicate_conflict_and_consistent_progress(self):
        log, _ = self.make_log()
        r3 = log.ingest(frag("a", 3))
        self.assertTrue(r3.accepted)
        self.assertEqual(r3.confirmed_seq, 0)
        self.assertEqual(r3.fragment_count, 0)
        self.assertEqual([(x.start, x.end) for x in r3.gaps], [(1, 2)])
        self.assertFalse(r3.done)

        r2 = log.ingest(frag("a", 2))
        self.assertEqual(r2.confirmed_seq, 0)
        self.assertEqual(r2.fragment_count, 0)
        r1 = log.ingest(frag("a", 1))
        self.assertEqual([x.seq for x in r1.advanced], [1, 2, 3])
        self.assertEqual((r1.confirmed_seq, r1.fragment_count), (3, 3))
        self.assertTrue(r1.done)

        duplicate = log.ingest(frag("a", 2))
        self.assertTrue((duplicate.accepted, duplicate.duplicate))
        self.assertEqual((duplicate.confirmed_seq, duplicate.fragment_count), (3, 3))

        bad = frag("a", 2, b"different-content")
        conflict = log.ingest(bad)
        self.assertTrue(conflict.conflict)
        self.assertEqual(conflict.errors[0].stage, "sequence-boundary")
        self.assertEqual(conflict.errors[0].offset, 2)
        evidence = log.conflict_evidence("a", 2)
        self.assertEqual(len(evidence), 2)
        self.assertNotEqual(evidence[0]["checksum"], evidence[1]["checksum"])
        self.assertEqual(log.confirmed_seq["a"], 3)

    def test_corrupted_length_jump_and_error_coordinates(self):
        log, _ = self.make_log(max_sequence_jump=2)
        broken = frag("a", 2)
        broken = Fragment("a", 2, broken.payload, "0" * 64, 20)
        result = log.ingest(broken)
        self.assertFalse(result.accepted)
        self.assertEqual(result.errors[0].code, "CHECKSUM_INVALID")
        self.assertEqual((result.errors[0].source, result.errors[0].seq,
                          result.errors[0].stage, result.errors[0].offset),
                         ("a", 2, "checksum", 2))

        oversized = Fragment.create("a", 3, b"x", 30)
        oversized = Fragment("a", 3, oversized.payload + b"-too-long",
                             oversized.checksum, 30)
        log.configs["a"].max_payload_bytes = 4
        length_error = log.ingest(oversized)
        self.assertEqual(length_error.errors[0].code, "PAYLOAD_TOO_LARGE")
        self.assertEqual(length_error.errors[0].offset, 10)

        jump = log.ingest(frag("a", 6))
        self.assertEqual(jump.errors[0].code, "SEQUENCE_JUMP_TOO_LARGE")
        self.assertEqual([(x.start, x.end, x.reason) for x in jump.continuable_ranges
                          if x.reason == "send-missing-fragment"], [(1, 1, "send-missing-fragment")])
        self.assertEqual([(x.start, x.end) for x in jump.manual_ranges if x.reason == "sequence-gap"],
                         [(1, 5)])

    def test_read_is_idempotent_non_mutating_and_filters_inputs(self):
        log, _ = self.make_log()
        log.ingest(frag("a", 2, ts=20))
        gap_read = log.read("a")
        self.assertEqual((gap_read["confirmed_seq"], gap_read["fragment_count"]), (0, 0))
        self.assertFalse(gap_read["ended"])
        self.assertEqual(gap_read["continuable"]["resend_from_seq"], 1)
        same = log.read("a")
        self.assertEqual(same["fragment_boundaries"], gap_read["fragment_boundaries"])
        self.assertEqual(same["confirmed_seq"], gap_read["confirmed_seq"])

        log.ingest(frag("a", 1, ts=10))
        log.ingest(frag("b", 1, ts=99))
        page = log.read("a", start_time_ns=15, after_seq=0)
        self.assertEqual([x["seq"] for x in page["records"]], [2])
        self.assertEqual(page["fragment_boundaries"], {"first_seq": 2, "last_seq": 2})
        self.assertTrue(page["ended"])
        again = log.read("a", start_time_ns=15, after_seq=0)
        self.assertEqual(again["records"], page["records"])
        self.assertEqual(log.stats(50)["sources"]["a"]["confirmed_seq"], 2)

    def test_stats_snapshot_reports_gap_wait_and_does_not_advance(self):
        log, _ = self.make_log()
        log.ingest(frag("a", 1, ts=10))
        log.ingest(frag("a", 3, ts=30))
        stats = log.stats(75)
        source = stats["sources"]["a"]
        self.assertEqual((source["confirmed_seq"], source["fragment_count"],
                          source["gap_count"], source["longest_wait_ns"]),
                         (1, 1, 1, 45))
        self.assertEqual(stats["gap_status"]["a"], "open")
        unchanged = log.stats(75)
        self.assertEqual(stats, unchanged)

    def test_multiple_recovery_ignores_only_half_batch(self):
        log, path = self.make_log()
        log.ingest(frag("a", 1, ts=10))
        tmp = path / "batches" / "batch-000000000002.json.tmp"
        tmp.write_text("{not a complete batch", encoding="utf-8")

        first = FragmentLog.recover(path)
        self.assertEqual(first.confirmed_seq["a"], 1)
        self.assertEqual(first.fragment_count["a"], 1)
        self.assertTrue(first.interrupted_batches)
        second = FragmentLog.recover(path)
        self.assertEqual(second.confirmed_seq, first.confirmed_seq)
        self.assertEqual(len(second.interrupted_batches), 1)

        second.ingest(frag("a", 2, ts=20))
        third = FragmentLog.recover(path)
        self.assertEqual((third.confirmed_seq["a"], third.fragment_count["a"]), (2, 2))
        batch = json.loads((path / "batches" / "batch-000000000002.json").read_text())
        self.assertEqual(batch["protocol_version"], PROTOCOL_VERSION_1)
        self.assertEqual(batch["checksum_algorithm"], CHECKSUM_SHA256)

    def test_capacity_near_limit_suspend_resume_expiration(self):
        log, _ = self.make_log(max_pending_fragments=2, fragment_ttl_ns=10)
        self.assertTrue(log.ingest(frag("a", 2, ts=20)).accepted)
        self.assertTrue(log.ingest(frag("a", 3, ts=30)).accepted)
        full = log.ingest(frag("a", 4, ts=40))
        self.assertEqual(full.errors[0].code, "PENDING_CAPACITY_EXCEEDED")

        log.set_source_status("a", SourceStatus.SUSPENDED)
        suspended = log.ingest(frag("a", 1, ts=10))
        self.assertEqual(suspended.errors[0].code, "SOURCE_NOT_ACTIVE")
        log.set_source_status("a", SourceStatus.ACTIVE)
        expired = log.expire_pending("a", 31)
        self.assertEqual(expired["expired"], [2])
        completed = log.ingest(frag("a", 1, ts=10))
        self.assertEqual(completed.confirmed_seq, 1)
        self.assertEqual([x.start for x in completed.manual_ranges if x.reason == "expired-fragment"], [])

    def test_source_isolated_replay_pagination(self):
        log, _ = self.make_log()
        for seq in range(1, 5):
            log.ingest(frag("a", seq, ts=seq * 10))
            log.ingest(frag("b", seq, ts=seq * 10 + 1))
        log.ingest(frag("a", 6, ts=60))

        page1 = log.replay("a", after_seq=0, limit=2)
        self.assertEqual([x["seq"] for x in page1["records"]], [1, 2])
        self.assertFalse(page1["ended"])
        page2 = log.replay("a", after_seq=2, limit=2)
        self.assertEqual((page2["next_seq"], page2["ended"]), (5, False))
        again = log.replay("a", after_seq=2, limit=2)
        self.assertEqual(again["fragment_boundaries"], page2["fragment_boundaries"])

        b = log.replay("b")
        self.assertTrue(b["ended"])
        self.assertEqual(b["confirmed_seq"], 4)
        self.assertEqual(log.replay("a")["gaps"][0],
                         {"start": 5, "end": 6, "reason": "replay-gap"})

    def test_protocol_migration_seals_old_records_before_opening_new(self):
        log, path = self.make_log()
        log.ingest(frag("a", 1))
        log.ingest(frag("a", 2))
        result = log.migrate_protocol("a", "a-v2", 2,
                                      lambda payload, seq: b"v2:" + payload)
        self.assertTrue(result["ended"])
        self.assertEqual(result["confirmed_seq"], 2)
        rejected = log.ingest(frag("a", 3, version=2))
        self.assertEqual(rejected.errors[0].code, "SOURCE_NOT_ACTIVE")
        accepted = log.ingest(frag("a-v2", 3, version=2))
        self.assertTrue(accepted.accepted)
        recovered = FragmentLog.recover(path)
        self.assertEqual(recovered.configs["a"].status, SourceStatus.REPLACED)
        self.assertEqual(recovered.records["a-v2"][0].protocol_version, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
