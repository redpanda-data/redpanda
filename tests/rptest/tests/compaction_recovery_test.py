from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools


class CompactionRecoveryTest(RedpandaTest):
    """
    Verify that segment indices are recovered on startup.
    """
    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=2000,
            compacted_log_segment_size=1048576,
        )

        topics = dict(topic=dict(cleanup_policy="compact"))

        super(CompactionRecoveryTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf,
                             topics=topics)

    @cluster(num_nodes=3)
    def test_index_recovery(self):
        partitions = self.produce_until_segments(3)

        for p in partitions:
            self.redpanda.stop_node(p.node)

        for p in partitions:
            p.delete_indices(allow_fail=False)

        for p in partitions:
            self.redpanda.start_node(p.node)

        wait_until(lambda: all(map(lambda p: p.recovered(), partitions)),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Timeout waiting for partitions to recover.")

    def produce_until_segments(self, count):
        partitions = []
        kafka_tools = KafkaCliTools(self.redpanda)

        def check_partitions():
            kafka_tools.produce("topic", 1024, 1024)
            storage = self.redpanda.storage()
            partitions[:] = storage.partitions("kafka", "topic")
            return partitions and all(
                map(lambda p: len(p.segments) > count and p.recovered(),
                    partitions))

        wait_until(lambda: check_partitions(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Segments not found")

        return partitions
