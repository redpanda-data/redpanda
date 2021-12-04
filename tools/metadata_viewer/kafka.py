from storage import Segment


class KafkaLog:
    def __init__(self, ntp):
        self.ntp = ntp

    def batch_headers(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for batch in s:
                yield batch.header._asdict()
