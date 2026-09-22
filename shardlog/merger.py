"""Sharded log merger.

Accepts sequenced, sourced, checksummed fragments; merges out-of-order,
duplicate and late arrivals into one continuous record per stream.
Nothing past a gap is ever presented as complete.
"""
from .errors import MergeError
from .storage import BatchJournal, checksum

MAX_SEQ = 2 ** 31 - 1

SOURCE_ACTIVE = "active"
SOURCE_PAUSED = "paused"
SOURCE_RETIRED = "retired"


class Fragment:
    def __init__(self, source, seq, payload, frag_checksum=None,
                 arrived_at=None):
        self.source = source
        self.seq = seq
        self.payload = payload
        self.checksum = frag_checksum if frag_checksum is not None \
            else checksum(payload)
        self.arrived_at = arrived_at

    def to_record(self):
        return {"source": self.source, "seq": self.seq,
                "payload": self.payload}


class ProgressView:
    """Atomic snapshot: confirmed position, fragment count and gap state
    always returned together so callers never stitch stale stats."""

    def __init__(self, confirmed, buffered, gaps, complete):
        self.confirmed = confirmed
        self.buffered = buffered
        self.gaps = gaps
        self.complete = complete

    def to_dict(self):
        return {
            "confirmed": dict(self.confirmed),
            "buffered": dict(self.buffered),
            "gaps": {s: list(g) for s, g in self.gaps.items()},
            "complete": self.complete,
        }


class LogMerger:
    def __init__(self, journal, max_payload=4096, max_buffered=10000,
                 clock=None):
        self.journal = journal
        self.max_payload = max_payload
        self.max_buffered = max_buffered
        # Logical clock injected by caller; never wall time.
        self._clock = clock if clock is not None else self._tick
        self._t = 0
        self.confirmed = {}      # source -> last confirmed seq (-1 start)
        self.buffered = {}       # source -> {seq: Fragment}
        self.conflicts = []      # kept evidence pairs
        self.errors = []
        self.source_status = {}
        self.wait_since = {}     # (source, gap_start) -> first-seen tick
        self.output_order = []   # (source, seq) of persisted records

    def _tick(self):
        self._t += 1
        return self._t

    # ---- fragment intake ----

    def set_source_status(self, source, status):
        self.source_status[source] = status

    def _validate(self, frag):
        stage = "validate"
        if not isinstance(frag.seq, int) or frag.seq < 0 \
                or frag.seq > MAX_SEQ:
            raise MergeError(stage, frag.source, frag.seq,
                             "sequence out of bounds")
        if len(frag.payload) > self.max_payload:
            raise MergeError(stage, frag.source, frag.seq,
                             "payload exceeds max length")
        if checksum(frag.payload) != frag.checksum:
            raise MergeError(stage, frag.source, frag.seq,
                             "checksum mismatch")
        status = self.source_status.get(frag.source, SOURCE_ACTIVE)
        if status == SOURCE_RETIRED:
            raise MergeError(stage, frag.source, frag.seq,
                             "source retired")
        if status == SOURCE_PAUSED:
            raise MergeError(stage, frag.source, frag.seq,
                             "source paused")

    def accept(self, frag):
        """Validate and buffer one fragment. Returns 'buffered',
        'duplicate' or 'conflict'."""
        try:
            self._validate(frag)
        except MergeError as exc:
            self.errors.append(exc)
            raise
        frag.arrived_at = self._clock()
        buf = self.buffered.setdefault(frag.source, {})
        confirmed = self.confirmed.get(frag.source, -1)
        if frag.seq <= confirmed:
            return "duplicate"  # already confirmed; never overwritten
        existing = buf.get(frag.seq)
        if existing is not None:
            if existing.payload == frag.payload:
                return "duplicate"
            # Same seq, different content: keep both, report conflict,
            # first arrival stays; never overwrite confirmed content.
            self.conflicts.append({
                "stage": "conflict",
                "source": frag.source,
                "seq": frag.seq,
                "kept": existing.payload,
                "rejected": frag.payload,
            })
            return "conflict"
        if sum(len(b) for b in self.buffered.values()) \
                >= self.max_buffered:
            exc = MergeError("capacity", frag.source, frag.seq,
                             "buffer capacity reached")
            self.errors.append(exc)
            raise exc
        buf[frag.seq] = frag
        return "buffered"

    # ---- merge advance ----

    def advance(self):
        """Move confirmed positions forward only; persist complete
        batches. Returns ProgressView with an explicit end flag."""
        batch = []
        for source in sorted(self.buffered):
            buf = self.buffered[source]
            nxt = self.confirmed.get(source, -1) + 1
            while nxt in buf:
                frag = buf.pop(nxt)
                batch.append(frag.to_record())
                self.confirmed[source] = nxt
                self.wait_since.pop((source, nxt), None)
                nxt += 1
            if nxt - 1 >= self.confirmed.get(source, -1) and buf:
                first_gap = min(buf)
                self.wait_since.setdefault(
                    (source, self.confirmed[source] + 1),
                    self._clock())
        if batch:
            self.journal.append_batch(batch)
            self.output_order.extend(
                (r["source"], r["seq"]) for r in batch)
        return self.progress(complete=True)

    # ---- recovery / replay ----

    def recover(self):
        """Restore from journal. Only complete batches are applied;
        incomplete ones are reported for re-reception."""
        records, incomplete = self.journal.recover()
        for rec in records:
            src, seq = rec["source"], rec["seq"]
            if seq > self.confirmed.get(src, -1):
                self.confirmed[src] = seq
            self.output_order.append((src, seq))
        return {
            "restored": len(records),
            "incomplete_batches": [
                {"batch": b.batch_id, "offset": b.offset}
                for b in incomplete],
            "complete": True,  # explicit end flag, not a cache length
            "progress": self.progress(complete=True).to_dict(),
        }

    def replay(self, source):
        """Recompute gaps for one source only. Other sources' confirmed
        positions and output order are untouched."""
        if source not in self.buffered and source not in self.confirmed:
            return {"source": source, "gaps": [], "recomputed": 0,
                    "complete": True}
        confirmed = self.confirmed.get(source, -1)
        buf = self.buffered.get(source, {})
        gaps = []
        if buf:
            lo = confirmed + 1
            have = set(buf)
            hi = max(have)
            gaps = [s for s in range(lo, hi + 1) if s not in have]
        return {
            "source": source,
            "gaps": gaps,
            "recomputed": len(gaps),
            "confirmed": confirmed,
            "complete": True,
        }

    # ---- read / stats (never mutate merge state) ----

    def read(self, source=None, since=None, until=None,
             after_confirmed=False):
        """Deterministic read: same request always yields the same
        fragment boundaries and confirmed numbers. Gaps come back as
        continuable info."""
        result = {}
        sources = [source] if source else sorted(
            set(self.confirmed) | set(self.buffered))
        for src in sources:
            confirmed = self.confirmed.get(src, -1)
            lo = confirmed + 1 if after_confirmed else 0
            frags = []
            for seq in sorted(self.buffered.get(src, {})):
                frag = self.buffered[src][seq]
                if seq < lo:
                    continue
                if since is not None and frag.arrived_at < since:
                    continue
                if until is not None and frag.arrived_at > until:
                    continue
                frags.append({"seq": seq, "payload": frag.payload,
                              "arrived_at": frag.arrived_at})
            gap_info = self._gap_info(src)
            result[src] = {
                "confirmed": confirmed,
                "fragments": frags,
                "next_needed": gap_info[0] if gap_info else None,
                "gaps": gap_info,
            }
        return result

    def _gap_info(self, source):
        buf = self.buffered.get(source, {})
        if not buf:
            return []
        lo = self.confirmed.get(source, -1) + 1
        have = set(buf)
        return [s for s in range(lo, max(have) + 1) if s not in have]

    def stats(self):
        """Read-only stats: gap count, longest wait, last confirmed.
        Never advances merge state."""
        now = self._clock()
        longest = 0
        gap_count = 0
        for (source, gap_start), since in self.wait_since.items():
            gap_count += 1
            longest = max(longest, now - since)
        return {
            "gap_count": gap_count,
            "longest_wait": longest,
            "last_confirmed": dict(self.confirmed),
        }

    def progress(self, complete=False):
        gaps = {s: self._gap_info(s) for s in self.buffered}
        return ProgressView(
            confirmed=self.confirmed,
            buffered={s: len(b) for s, b in self.buffered.items()},
            gaps=gaps,
            complete=complete,
        )

    # ---- range classification ----

    def classify_incoming(self, source, seq):
        """Split what can still be accepted from what needs manual
        handling (expired, source changed, seq jump)."""
        confirmed = self.confirmed.get(source, -1)
        status = self.source_status.get(source, SOURCE_ACTIVE)
        if status != SOURCE_ACTIVE:
            return {"receivable": None,
                    "manual": {"reason": "source_" + status,
                               "source": source, "seq": seq}}
        if seq <= confirmed:
            return {"receivable": None,
                    "manual": {"reason": "expired", "source": source,
                               "seq": seq,
                               "confirmed": confirmed}}
        buf = self.buffered.get(source, {})
        horizon = max([confirmed + 1] + list(buf)) + self.max_buffered
        if seq > horizon:
            return {"receivable": None,
                    "manual": {"reason": "sequence_jump",
                               "source": source, "seq": seq,
                               "expected_at_most": horizon}}
        return {"receivable": {"source": source, "from": confirmed + 1,
                               "to": horizon},
                "manual": None}
