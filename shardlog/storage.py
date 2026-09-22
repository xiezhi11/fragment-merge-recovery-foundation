"""Append-only batch journal with protocol/checksum header.

Guarantees:
- Only fully committed batches are applied on recovery.
- A batch interrupted mid-write is detected and reported as incomplete;
  its fragments must be re-received.
- The header records the checksum algorithm and protocol version so
  future upgrades migrate complete records before accepting new ones.
"""
import json
import os
import zlib

PROTOCOL_VERSION = 1
CHECKSUM_ALGO = "crc32"

BEGIN = "BEGIN"
RECORD = "REC"
COMMIT = "COMMIT"


def checksum(payload):
    return format(zlib.crc32(payload.encode("utf-8")) & 0xFFFFFFFF, "08x")


class IncompleteBatch(Exception):
    def __init__(self, batch_id, offset):
        self.batch_id = batch_id
        self.offset = offset
        super().__init__(
            "incomplete batch %s at offset %d" % (batch_id, offset))


class BatchJournal:
    """Journal of committed batches on disk.

    Layout:
      line 0: header json {protocol_version, checksum_algo}
      then per batch:
        BEGIN  {batch, count}
        REC    {source, seq, payload, checksum}   (x count)
        COMMIT {batch, crc}
    """

    def __init__(self, path, protocol_version=PROTOCOL_VERSION):
        self.path = path
        self.protocol_version = protocol_version
        self._next_batch = 0
        if os.path.exists(path):
            self._scan_header()
        else:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "protocol_version": protocol_version,
                    "checksum_algo": CHECKSUM_ALGO,
                }) + "\n")

    def _scan_header(self):
        with open(self.path, "r", encoding="utf-8") as fh:
            header = json.loads(fh.readline())
        self.protocol_version = header["protocol_version"]
        self.checksum_algo = header["checksum_algo"]

    def append_batch(self, records):
        """Atomically append one batch of confirmed records."""
        batch_id = self._next_batch
        lines = [json.dumps({"t": BEGIN, "batch": batch_id,
                             "count": len(records)})]
        crc = 0
        for rec in records:
            line = json.dumps({
                "t": RECORD,
                "source": rec["source"],
                "seq": rec["seq"],
                "payload": rec["payload"],
                "checksum": checksum(rec["payload"]),
            })
            crc = zlib.crc32(line.encode("utf-8"), crc)
            lines.append(line)
        lines.append(json.dumps({"t": COMMIT, "batch": batch_id,
                                 "crc": format(crc & 0xFFFFFFFF, "08x")}))
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self._next_batch += 1
        return batch_id

    def recover(self):
        """Return (records, incomplete_batches).

        Only complete batches yield records. A truncated tail batch is
        reported via IncompleteBatch so callers know to re-receive it.
        """
        records = []
        incomplete = []
        with open(self.path, "r", encoding="utf-8") as fh:
            fh.readline()  # header
            pending = None
            offset = 1
            for line in fh:
                offset += 1
                entry = json.loads(line)
                kind = entry["t"]
                if kind == BEGIN:
                    if pending is not None:
                        incomplete.append(
                            IncompleteBatch(pending["batch"], offset))
                    pending = {"batch": entry["batch"], "recs": [],
                               "crc": 0}
                elif kind == RECORD:
                    if pending is None:
                        continue
                    if checksum(entry["payload"]) != entry["checksum"]:
                        incomplete.append(
                            IncompleteBatch(pending["batch"], offset))
                        pending = None
                        continue
                    pending["crc"] = zlib.crc32(
                        line.rstrip("\n").encode("utf-8"), pending["crc"])
                    pending["recs"].append(entry)
                elif kind == COMMIT:
                    if pending is None:
                        continue
                    ok = (entry["batch"] == pending["batch"] and
                          entry["crc"] ==
                          format(pending["crc"] & 0xFFFFFFFF, "08x"))
                    if ok:
                        records.extend(pending["recs"])
                        self._next_batch = max(self._next_batch,
                                               entry["batch"] + 1)
                    else:
                        incomplete.append(
                            IncompleteBatch(pending["batch"], offset))
                    pending = None
            if pending is not None:
                incomplete.append(
                    IncompleteBatch(pending["batch"], offset))
        return records, incomplete

    def migrate(self, new_path, new_version):
        """Migrate complete records to a new protocol version first.

        Returns (migrated_count, incomplete_batches). New-version
        fragments are only accepted after this completes, so protocols
        are never mixed in one record stream.
        """
        records, incomplete = self.recover()
        target = BatchJournal(new_path, protocol_version=new_version)
        if records:
            target.append_batch([{
                "source": r["source"], "seq": r["seq"],
                "payload": r["payload"]} for r in records])
        return len(records), incomplete
