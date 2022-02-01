from storage import Segment


class KafkaLog:
    def __init__(self, ntp):
        self.ntp = ntp

    def batch_headers(self):
        for batch in self.batches():
            yield batch.header._asdict()

    def batches(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for batch in s:
                yield batch