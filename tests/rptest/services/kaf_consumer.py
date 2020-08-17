import re
from ducktape.services.background_thread import BackgroundThreadService


class KafConsumer(BackgroundThreadService):
    def __init__(self, context, redpanda, topic, num_records=1):
        super(KafConsumer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._num_records = num_records
        self.done = False
        self.offset = dict()

    def _worker(self, idx, node):
        try:
            partition = None
            cmd = "kaf consume -b %s --offset newest %s" % (
                self._redpanda.brokers(), self._topic)
            for line in node.account.ssh_capture(cmd):
                self.logger.debug(line.rstrip())

                m = re.match("Partition:\s+(?P<partition>\d+)", line)
                if m:
                    assert partition is None
                    partition = int(m.group("partition"))
                    continue

                m = re.match("Offset:\s+(?P<offset>\d+)", line)
                if m:
                    assert partition is not None
                    offset = int(m.group("offset"))
                    self.offset[partition] = offset
                    partition = None
        except:
            raise
        finally:
            self.done = True

    def stop_node(self, node):
        node.account.kill_process("kaf", clean_shutdown=False)
