class MergeError(Exception):
    """Error that always carries source, sequence and processing stage."""

    def __init__(self, stage, source, seq, message):
        self.stage = stage
        self.source = source
        self.seq = seq
        super().__init__("[%s] source=%s seq=%s: %s" % (stage, source, seq, message))

    def to_dict(self):
        return {
            "stage": self.stage,
            "source": self.source,
            "seq": self.seq,
            "message": str(self),
        }
