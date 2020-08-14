import collections
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools


class PrefixTruncateRecoveryTest(RedpandaTest):
    """
    Verify that a kafka log that's been prefix truncated due to retention policy
    eventually converges with other raft group nodes.
    """
    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_segment_size=1048576,
            retention_bytes=5242880,
            log_compaction_interval_ms=2000,
        )

        topics = dict(topic=dict(cleanup_policy="delete"))

        super(PrefixTruncateRecoveryTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf,
                             topics=topics)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    @cluster(num_node=3)
    @matrix(acks=[-1, 1])
    def test_prefix_truncate_recovery(self, acks):
        # produce a little data
        self.kafka_tools.produce("topic", 1024, 1024, acks=acks)

        # stop one of the nodes
        node = self.redpanda.controller()
        self.redpanda.stop_node(node)

        # produce data to the topic until we observe that the retention policy
        # has kicked in and one or more segments has been deleted.
        self.produce_until_deleted(node)

        self.redpanda.start_node(node)
        self.verify_recovery(node)

    def produce_until_deleted(self, ignore_node):
        partitions = {}

        #
        # Produce until at least 3 segments per partition appear on disk.
        #
        def produce_until_segments(count):
            self.kafka_tools.produce("topic", 1000, 1000)
            storage = self.redpanda.storage()
            for p in storage.partitions("kafka", "topic"):
                if p.node == ignore_node:
                    continue
                if p.num not in partitions or len(
                        partitions[p.num].segments) < count:
                    partitions[p.num] = p
            self.logger.debug("Found partitions: %s", partitions)
            return partitions and all(
                map(lambda p: len(p[1].segments) >= count, partitions.items()))

        wait_until(lambda: produce_until_segments(3),
                   timeout_sec=60,
                   backoff_sec=1,
                   err_msg="Expected segments did not materialize")

        def make_segment_sets(partitions):
            return {
                p[0]: {s[0]
                       for s in p[1].segments.items()}
                for p in partitions.items()
            }

        orig_segments = make_segment_sets(partitions)
        self.logger.debug("Original segments: %s", orig_segments)

        #
        # Continue producing until the original segments above have been deleted
        # because of the retention / cleanup policy.
        #
        def produce_until_segments_deleted():
            self.kafka_tools.produce("topic", 1000, 1000)
            storage = self.redpanda.storage()
            curr_segments = make_segment_sets(
                {p.num: p
                 for p in storage.partitions("kafka", "topic")})
            for p, segs in orig_segments.items():
                self.logger.debug("Partition %d segment set intersection: %s",
                                  p, segs.intersection(curr_segments[p]))
                if not segs.isdisjoint(curr_segments[p]):
                    return False
            return True

        wait_until(lambda: produce_until_segments_deleted(),
                   timeout_sec=60,
                   backoff_sec=1,
                   err_msg="Original segments were not deleted")

    def verify_recovery(self, node):
        # repeat until true
        #  1. collect segment files from quroum members
        #  2. verify byte-for-byte equivalence of common range
        #  3. success
        pass
