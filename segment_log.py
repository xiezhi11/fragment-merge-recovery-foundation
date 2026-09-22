"""Deterministic, crash-safe per-source log fragment assembly."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

PROTOCOL_VERSION_1 = 1
STORAGE_VERSION = 1
CHECKSUM_SHA256 = "sha256"


class SourceStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    REPLACED = "replaced"
    MIGRATING = "migrating"


class Stage(str, Enum):
    DECODE = "decode"
    SOURCE = "source"
    BOUNDARY = "sequence-boundary"
    LENGTH = "length"
    CHECKSUM = "checksum"
    PROTOCOL = "protocol"
    CAPACITY = "capacity"
    PERSIST = "persistence"
    RECOVERY = "recovery"
    READ = "read"
    REPLAY = "replay"


@dataclass(frozen=True)
class Fragment:
    source: str
    seq: int
    payload: bytes
    checksum: str
    timestamp_ns: int
    protocol_version: int = PROTOCOL_VERSION_1
    checksum_algorithm: str = CHECKSUM_SHA256

    @staticmethod
    def create(source, seq, payload, timestamp_ns, protocol_version=PROTOCOL_VERSION_1,
               checksum_algorithm=CHECKSUM_SHA256):
        if checksum_algorithm != CHECKSUM_SHA256:
            raise ValueError("only sha256 is supported")
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError("payload must be bytes")
        return Fragment(source, seq, bytes(payload),
                        hashlib.sha256(payload).hexdigest(), timestamp_ns,
                        protocol_version, checksum_algorithm)


@dataclass(frozen=True)
class SeqRange:
    start: int
    end: int
    reason: str

    def to_dict(self): return asdict(self)


@dataclass
class Error:
    code: str
    source: str
    seq: Optional[int]
    stage: str
    message: str
    offset: Optional[int] = None

    def to_dict(self): return asdict(self)


@dataclass
class Evidence:
    fragment: Fragment
    received_count: int = 1


@dataclass
class SourceConfig:
    source: str
    status: SourceStatus = SourceStatus.ACTIVE
    start_seq: int = 0
    protocol_version: int = PROTOCOL_VERSION_1
    checksum_algorithm: str = CHECKSUM_SHA256
    max_payload_bytes: int = 1 << 20
    max_pending_fragments: int = 1000
    max_sequence_jump: int = 32
    fragment_ttl_ns: Optional[int] = None


@dataclass
class IngestResult:
    accepted: bool
    confirmed_seq: int
    fragment_count: int
    advanced: List[Fragment] = field(default_factory=list)
    gaps: List[SeqRange] = field(default_factory=list)
    continuable_ranges: List[SeqRange] = field(default_factory=list)
    manual_ranges: List[SeqRange] = field(default_factory=list)
    errors: List[Error] = field(default_factory=list)
    duplicate: bool = False
    conflict: bool = False
    done: bool = True

    def to_dict(self):
        data = asdict(self)
        data["advanced"] = [fragment_to_dict(x) for x in self.advanced]
        return data


@dataclass(frozen=True)
class ConfirmedRecord:
    source: str
    seq: int
    payload: bytes
    checksum: str
    timestamp_ns: int
    batch_id: int
    protocol_version: int
    checksum_algorithm: str


def fragment_to_dict(f):
    return {
        "source": f.source, "seq": f.seq,
        "payload": base64.b64encode(f.payload).decode("ascii"),
        "checksum": f.checksum, "timestamp_ns": f.timestamp_ns,
        "protocol_version": f.protocol_version,
        "checksum_algorithm": f.checksum_algorithm,
    }


def fragment_from_dict(d):
    return Fragment(d["source"], int(d["seq"]), base64.b64decode(d["payload"]),
                    d["checksum"], int(d["timestamp_ns"]),
                    int(d["protocol_version"]), d["checksum_algorithm"])


def record_to_dict(r):
    d = fragment_to_dict(Fragment(r.source, r.seq, r.payload, r.checksum,
                                  r.timestamp_ns, r.protocol_version,
                                  r.checksum_algorithm))
    d["batch_id"] = r.batch_id
    return d


def record_from_dict(d):
    return ConfirmedRecord(d["source"], int(d["seq"]),
                           base64.b64decode(d["payload"]), d["checksum"],
                           int(d["timestamp_ns"]), int(d["batch_id"]),
                           int(d["protocol_version"]), d["checksum_algorithm"])


def compress_ranges(values, reason):
    values = sorted(set(values))
    if not values:
        return []
    out, start, prev = [], values[0], values[0]
    for value in values[1:]:
        if value != prev + 1:
            out.append(SeqRange(start, prev, reason))
            start = value
        prev = value
    out.append(SeqRange(start, prev, reason))
    return out


def _merge_ranges(ranges):
    by_reason = {}
    for rng in ranges:
        by_reason.setdefault(rng.reason, []).append(rng)
    out = []
    for reason, items in by_reason.items():
        points = []
        for rng in items:
            points.extend(range(rng.start, rng.end + 1))
        out.extend(compress_ranges(points, reason))
    return sorted(out, key=lambda x: (x.start, x.end, x.reason))


class FragmentLog:
    """In-memory coordinator with append-only, batch-atomic persistence."""

    def __init__(self, directory=None):
        self.directory = Path(directory) if directory else None
        self.configs: Dict[str, SourceConfig] = {}
        self.confirmed_seq: Dict[str, int] = {}
        self.fragment_count: Dict[str, int] = {}
        self.pending: Dict[str, Dict[int, Fragment]] = {}
        self.records: Dict[str, List[ConfirmedRecord]] = {}
        self.duplicates: Dict[Tuple[str, int], int] = {}
        self.conflicts: Dict[str, Dict[int, List[Evidence]]] = {}
        self.interrupted_batches: List[str] = []
        self.next_batch_id = 1
        if self.directory:
            (self.directory / "batches").mkdir(parents=True, exist_ok=True)

    def configure_source(self, config):
        old = self.configs.get(config.source)
        if old and (old.protocol_version != config.protocol_version or
                    old.checksum_algorithm != config.checksum_algorithm):
            raise ValueError("protocol/checksum changes need migration, not reconfiguration")
        self.configs[config.source] = config
        self.confirmed_seq.setdefault(config.source, config.start_seq)
        self.fragment_count.setdefault(config.source, 0)
        self.pending.setdefault(config.source, {})
        self.records.setdefault(config.source, [])
        self.conflicts.setdefault(config.source, {})

    def set_source_status(self, source, status):
        self._require_config(source)
        self.configs[source].status = status

    def _require_config(self, source):
        if source not in self.configs:
            raise KeyError(f"unknown source: {source}")
        return self.configs[source]

    def ingest(self, fragment, now_ns=None):
        cfg = self.configs.get(fragment.source)
        result = IngestResult(False, self.confirmed_seq.get(fragment.source, 0),
                              self.fragment_count.get(fragment.source, 0))
        if cfg is None:
            result.errors.append(Error("UNKNOWN_SOURCE", fragment.source, fragment.seq,
                                       Stage.SOURCE.value, "source is not configured"))
            return self._finish_result(cfg, result, now_ns)
        if cfg.status != SourceStatus.ACTIVE:
            result.errors.append(Error("SOURCE_NOT_ACTIVE", fragment.source, fragment.seq,
                                       Stage.SOURCE.value,
                                       f"source status is {cfg.status.value}",
                                       fragment.seq - cfg.start_seq))
            result.manual_ranges.append(SeqRange(fragment.seq, fragment.seq,
                                                 f"source-{cfg.status.value}"))
            return self._finish_result(cfg, result, now_ns)
        if fragment.protocol_version != cfg.protocol_version:
            result.errors.append(self._err("PROTOCOL_MISMATCH", cfg, fragment,
                                           Stage.PROTOCOL,
                                           "protocol version differs from open record"))
            result.manual_ranges.append(SeqRange(fragment.seq, fragment.seq, "protocol-mismatch"))
            return self._finish_result(cfg, result, now_ns)
        if fragment.checksum_algorithm != cfg.checksum_algorithm:
            result.errors.append(self._err("CHECKSUM_ALGORITHM_MISMATCH", cfg, fragment,
                                           Stage.CHECKSUM, "checksum algorithm mismatch"))
            return self._finish_result(cfg, result, now_ns)
        if not isinstance(fragment.seq, int) or fragment.seq <= cfg.start_seq:
            result.errors.append(self._err("SEQUENCE_BEFORE_START", cfg, fragment,
                                           Stage.BOUNDARY, "sequence is outside stream start"))
            return self._finish_result(cfg, result, now_ns)
        if not fragment.payload:
            result.errors.append(self._err("EMPTY_PAYLOAD", cfg, fragment,
                                           Stage.LENGTH, "payload length must be positive", 0))
            return self._finish_result(cfg, result, now_ns)
        if len(fragment.payload) > cfg.max_payload_bytes:
            result.errors.append(self._err("PAYLOAD_TOO_LARGE", cfg, fragment,
                                           Stage.LENGTH,
                                           f"length {len(fragment.payload)} exceeds limit",
                                           len(fragment.payload)))
            result.manual_ranges.append(SeqRange(fragment.seq, fragment.seq, "payload-too-large"))
            return self._finish_result(cfg, result, now_ns)
        calculated = hashlib.sha256(fragment.payload).hexdigest()
        if calculated != fragment.checksum:
            result.errors.append(self._err("CHECKSUM_INVALID", cfg, fragment,
                                           Stage.CHECKSUM,
                                           f"calculated {calculated}, claimed {fragment.checksum}"))
            return self._finish_result(cfg, result, now_ns)

        confirmed = self.confirmed_seq[fragment.source]
        if fragment.seq <= confirmed:
            prior = next((x for x in self.records[fragment.source] if x.seq == fragment.seq), None)
            return self._repeat(cfg, incoming=fragment, prior=prior, result=result, now_ns=now_ns)
        prior = self.pending[fragment.source].get(fragment.seq)
        if prior is not None:
            return self._repeat(cfg, incoming=fragment, prior=prior, result=result, now_ns=now_ns)

        expected = confirmed + 1
        if fragment.seq > expected + cfg.max_sequence_jump:
            result.errors.append(self._err("SEQUENCE_JUMP_TOO_LARGE", cfg, fragment,
                                           Stage.BOUNDARY,
                                           f"next expected {expected}; jump limit {cfg.max_sequence_jump}"))
            result.continuable_ranges.append(SeqRange(expected, expected, "send-missing-fragment"))
            result.manual_ranges.append(SeqRange(expected, fragment.seq - 1, "sequence-gap"))
            return self._finish_result(cfg, result, now_ns)
        if len(self.pending[fragment.source]) >= cfg.max_pending_fragments:
            result.errors.append(self._err("PENDING_CAPACITY_EXCEEDED", cfg, fragment,
                                           Stage.CAPACITY,
                                           "pending fragment capacity reached"))
            result.manual_ranges.append(SeqRange(expected, fragment.seq, "capacity"))
            return self._finish_result(cfg, result, now_ns)

        self.pending[fragment.source][fragment.seq] = fragment
        result.accepted = True
        result.advanced = self._advance(cfg)
        return self._finish_result(cfg, result, now_ns)

    @staticmethod
    def _err(code, cfg, fragment, stage, message, offset=None):
        if offset is None:
            offset = fragment.seq - cfg.start_seq
        return Error(code, fragment.source, fragment.seq, stage.value, message, offset)

    def _repeat(self, cfg, incoming, prior, result, now_ns):
        result.confirmed_seq = self.confirmed_seq[cfg.source]
        result.fragment_count = self.fragment_count[cfg.source]
        if prior is not None and prior.payload == incoming.payload and prior.checksum == incoming.checksum:
            key = (incoming.source, incoming.seq)
            self.duplicates[key] = self.duplicates.get(key, 1) + 1
            result.accepted = True
            result.duplicate = True
        else:
            entries = self.conflicts[cfg.source].setdefault(incoming.seq, [])
            for candidate in ((prior or incoming), incoming):
                if not any(e.fragment.checksum == candidate.checksum and
                           e.fragment.payload == candidate.payload for e in entries):
                    entries.append(Evidence(candidate))
            result.conflict = True
            result.errors.append(Error(
                "SEQUENCE_CONTENT_CONFLICT", incoming.source, incoming.seq,
                Stage.BOUNDARY.value,
                f"{len(self.conflicts[cfg.source][incoming.seq])} evidence copies retained",
                incoming.seq - cfg.start_seq))
            result.manual_ranges.append(SeqRange(incoming.seq, incoming.seq, "conflict"))
        return self._finish_result(cfg, result, now_ns)

    def _advance(self, cfg):
        advanced = []
        expected = self.confirmed_seq[cfg.source] + 1
        batch = []
        while expected in self.pending[cfg.source]:
            fragment = self.pending[cfg.source][expected]
            batch.append(fragment)
            advanced.append(fragment)
            expected += 1
        if batch:
            batch_id = self.next_batch_id
            self.next_batch_id += 1
            self._write_batch(batch_id, batch)
            for fragment in batch:
                self.pending[cfg.source].pop(fragment.seq, None)
                record = ConfirmedRecord(
                    fragment.source, fragment.seq, fragment.payload, fragment.checksum,
                    fragment.timestamp_ns, batch_id, fragment.protocol_version,
                    fragment.checksum_algorithm)
                self.records[cfg.source].append(record)
                self.confirmed_seq[cfg.source] = fragment.seq
                self.fragment_count[cfg.source] += 1
        return advanced

    def _finish_result(self, cfg, result, now_ns=None):
        if cfg is not None:
            result.confirmed_seq = self.confirmed_seq[cfg.source]
            result.fragment_count = self.fragment_count[cfg.source]
            result.gaps = self.gap_ranges(cfg.source)
            result.continuable_ranges.append(SeqRange(
                result.confirmed_seq + 1, result.confirmed_seq + 1,
                "next-expected"))
            result.done = not result.gaps and not result.manual_ranges
            if now_ns is not None:
                expired = self.expire_pending(cfg.source, now_ns)
                result.continuable_ranges.extend(expired["continuable"])
                result.manual_ranges.extend(expired["manual"])
                result.gaps = self.gap_ranges(cfg.source)
        # Normalize ranges and remove overlaps only by preserving both labels.
        result.continuable_ranges = _merge_ranges(result.continuable_ranges)
        result.manual_ranges = _merge_ranges(result.manual_ranges)
        return result

    def gap_sequences(self, source):
        cfg = self._require_config(source)
        pending = self.pending[source]
        if not pending:
            return []
        confirmed = self.confirmed_seq[source]
        highest = max(pending)
        return [s for s in range(confirmed + 1, highest) if s not in pending]

    def gap_ranges(self, source):
        return compress_ranges(self.gap_sequences(source), "missing-fragment")

    def expire_pending(self, source, now_ns):
        cfg = self._require_config(source)
        if cfg.fragment_ttl_ns is None:
            return {"continuable": [], "manual": [], "expired": []}
        expired = []
        for seq, fragment in list(self.pending[source].items()):
            if fragment.timestamp_ns + cfg.fragment_ttl_ns < now_ns:
                expired.append(seq)
                del self.pending[source][seq]
        manual = [SeqRange(s, s, "expired-fragment") for s in expired]
        continuable = [SeqRange(s, s, "resend-fragment") for s in expired]
        return {"continuable": _merge_ranges(continuable),
                "manual": _merge_ranges(manual),
                "expired": expired}

    def _write_batch(self, batch_id, fragments):
        if self.directory is None:
            return
        payload = {
            "storage_version": STORAGE_VERSION,
            "batch_id": batch_id,
            "source": fragments[0].source,
            "protocol_version": fragments[0].protocol_version,
            "checksum_algorithm": fragments[0].checksum_algorithm,
            "fragments": [fragment_to_dict(f) for f in fragments],
        }
        if any(f.source != fragments[0].source or
               f.protocol_version != fragments[0].protocol_version or
               f.checksum_algorithm != fragments[0].checksum_algorithm
               for f in fragments):
            raise ValueError("a batch cannot mix sources or protocols")
        payload["batch_checksum"] = hashlib.sha256(
            json.dumps(payload["fragments"], sort_keys=True).encode()).hexdigest()
        final = self.directory / "batches" / f"batch-{batch_id:012d}.json"
        tmp = final.with_suffix(".json.tmp")
        self._atomic_json(tmp, final, payload)

    @staticmethod
    def _atomic_json(tmp, final, payload):
        fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, final)
            dir_fd = os.open(str(final.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if tmp.exists():
                try:
                    tmp.unlink()
                except FileNotFoundError:
                    pass

    def save_manifest(self):
        if self.directory is None:
            return
        payload = {
            "storage_version": STORAGE_VERSION,
            "sources": {
                name: {
                    "status": cfg.status.value,
                    "start_seq": cfg.start_seq,
                    "protocol_version": cfg.protocol_version,
                    "checksum_algorithm": cfg.checksum_algorithm,
                    "max_payload_bytes": cfg.max_payload_bytes,
                    "max_pending_fragments": cfg.max_pending_fragments,
                    "max_sequence_jump": cfg.max_sequence_jump,
                    "fragment_ttl_ns": cfg.fragment_ttl_ns,
                } for name, cfg in self.configs.items()
            },
            "next_batch_id": self.next_batch_id,
        }
        final = self.directory / "manifest.json"
        self._atomic_json(final.with_suffix(".json.tmp"), final, payload)

    @classmethod
    def recover(cls, directory):
        directory = Path(directory)
        batch_dir = directory / "batches"
        batch_dir.mkdir(parents=True, exist_ok=True)
        log = cls(directory)
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("storage_version") != STORAGE_VERSION:
                raise ValueError("unsupported storage version")
            for name, data in manifest["sources"].items():
                data = dict(data)
                data["source"] = name
                data["status"] = SourceStatus(data["status"])
                log.configure_source(SourceConfig(**data))
            log.next_batch_id = int(manifest["next_batch_id"])
        for tmp in sorted(batch_dir.glob("*.tmp")):
            log.interrupted_batches.append(str(tmp))
        for path in sorted(batch_dir.glob("batch-*.json")):
            doc = json.loads(path.read_text(encoding="utf-8"))
            fragments = [fragment_from_dict(x) for x in doc["fragments"]]
            checksum = hashlib.sha256(json.dumps(
                [fragment_to_dict(x) for x in fragments], sort_keys=True).encode()).hexdigest()
            if checksum != doc.get("batch_checksum"):
                raise ValueError(f"complete batch checksum invalid: {path.name}")
            source = doc["source"]
            if source not in log.configs:
                log.configure_source(SourceConfig(
                    source, protocol_version=doc["protocol_version"],
                    checksum_algorithm=doc["checksum_algorithm"]))
            batch_id = int(doc["batch_id"])
            for f in fragments:
                log.records[source].append(record_from_dict(
                    {**fragment_to_dict(f), "batch_id": batch_id}))
                log.confirmed_seq[source] = f.seq
                log.fragment_count[source] += 1
            log.next_batch_id = max(log.next_batch_id, batch_id + 1)
        return log

    def read(self, source, start_time_ns=None, end_time_ns=None,
             after_seq=None, limit=None):
        cfg = self._require_config(source)
        cursor = cfg.start_seq if after_seq is None else after_seq
        selected = []
        for record in self.records[source]:
            if record.seq <= cursor:
                continue
            if start_time_ns is not None and record.timestamp_ns < start_time_ns:
                continue
            if end_time_ns is not None and record.timestamp_ns > end_time_ns:
                continue
            selected.append(record)
        if limit is not None:
            page = selected[:limit]
        else:
            page = selected
        gaps = self.gap_ranges(source)
        first_seq = page[0].seq if page else None
        last_seq = page[-1].seq if page else cursor
        return {
            "source": source,
            "records": [record_to_dict(x) for x in page],
            "fragment_boundaries": {"first_seq": first_seq, "last_seq": last_seq},
            "confirmed_seq": self.confirmed_seq[source],
            "next_confirmed_seq": self.confirmed_seq[source] + 1,
            "fragment_count": self.fragment_count[source],
            "gaps": [x.to_dict() for x in gaps],
            "has_more": len(selected) > len(page),
            "complete": not gaps,
            "ended": len(selected) <= len(page) and not gaps,
            "continuable": {"resend_from_seq": self.confirmed_seq[source] + 1
                            if gaps else None},
        }

    def stats(self, now_ns):
        """Return a consistent snapshot; callers provide deterministic time."""
        sources = {}
        for source, cfg in self.configs.items():
            gaps = self.gap_sequences(source)
            waiting_ns = 0
            if gaps:
                anchors = [f.timestamp_ns for s, f in self.pending[source].items()
                           if s > gaps[0]]
                if anchors:
                    waiting_ns = max(0, now_ns - min(anchors))
            sources[source] = {
                "status": cfg.status.value,
                "confirmed_seq": self.confirmed_seq[source],
                "recent_confirmed_seq": self.confirmed_seq[source],
                "fragment_count": self.fragment_count[source],
                "gap_count": len(gaps),
                "gaps": [x.to_dict() for x in compress_ranges(gaps, "missing-fragment")],
                "longest_wait_ns": waiting_ns,
                "continuable_from_seq": self.confirmed_seq[source] + 1 if gaps else None,
            }
        return {
            "now_ns": now_ns,
            "sources": sources,
            "total_gap_count": sum(x["gap_count"] for x in sources.values()),
            "confirmed_seq": {k: v["confirmed_seq"] for k, v in sources.items()},
            "fragment_count": {k: v["fragment_count"] for k, v in sources.items()},
            "gap_status": {k: ("open" if v["gap_count"] else "closed")
                           for k, v in sources.items()},
        }

    def conflict_evidence(self, source, seq):
        return [fragment_to_dict(e.fragment) for e in self.conflicts.get(source, {}).get(seq, [])]

    def replay(self, source, after_seq=None, to_seq=None, limit=None):
        cfg = self._require_config(source)
        start = cfg.start_seq if after_seq is None else after_seq
        records = [r for r in self.records[source]
                   if r.seq > start and (to_seq is None or r.seq <= to_seq)]
        page = records[:limit] if limit is not None else records
        have = {r.seq for r in self.records[source] if
                (to_seq is None or r.seq <= to_seq)}
        if to_seq is not None:
            end = to_seq
        else:
            end = max([self.confirmed_seq[source], *self.pending[source].keys()])
        missing = [s for s in range(start + 1, end + 1) if s not in have]
        last_seq = page[-1].seq if page else start
        ended = len(records) <= len(page) and not missing
        return {
            "source": source,
            "records": [record_to_dict(x) for x in page],
            "fragment_boundaries": {"first_seq": page[0].seq if page else None,
                                    "last_seq": last_seq},
            "confirmed_seq": self.confirmed_seq[source],
            "fragment_count": self.fragment_count[source],
            "gaps": [x.to_dict() for x in compress_ranges(missing, "replay-gap")],
            "next_seq": last_seq + 1,
            "has_more": len(records) > len(page),
            "complete": not missing,
            "ended": ended,
        }

    def migrate_protocol(self, old_source, new_source, new_protocol_version, converter,
                         new_checksum_algorithm=CHECKSUM_SHA256):
        old = self._require_config(old_source)
        if self.gap_sequences(old_source) or self.pending[old_source]:
            raise ValueError("cannot migrate until every old record is complete")
        if new_source in self.configs:
            raise ValueError("new migration source already exists")
        old.status = SourceStatus.REPLACED
        converted = []
        for record in self.records[old_source]:
            payload = converter(record.payload, record.seq)
            fragment = Fragment.create(new_source, record.seq, payload,
                                       record.timestamp_ns, new_protocol_version,
                                       new_checksum_algorithm)
            converted.append(fragment)
        new_cfg = SourceConfig(new_source, status=SourceStatus.MIGRATING,
                               protocol_version=new_protocol_version,
                               checksum_algorithm=new_checksum_algorithm)
        self.configure_source(new_cfg)
        self.save_manifest()
        if converted:
            batch_id = self.next_batch_id
            self.next_batch_id += 1
            self._write_batch(batch_id, converted)
            for fragment in converted:
                self.records[new_source].append(ConfirmedRecord(
                    fragment.source, fragment.seq, fragment.payload, fragment.checksum,
                    fragment.timestamp_ns, batch_id, fragment.protocol_version,
                    fragment.checksum_algorithm))
                self.confirmed_seq[new_source] = fragment.seq
                self.fragment_count[new_source] += 1
        new_cfg.status = SourceStatus.ACTIVE
        self.save_manifest()
        return {
            "migrated_source": old_source,
            "new_source": new_source,
            "fragment_count": len(converted),
            "confirmed_seq": self.confirmed_seq[new_source],
            "ended": True,
        }
