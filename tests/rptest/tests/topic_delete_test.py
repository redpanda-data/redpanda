import collections

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools


class TopicDeleteTest(RedpandaTest):
    """
    Verify that topic deletion cleans up storage.
    """
    def __init__(self, test_context):
        extra_rp_conf = dict(log_segment_size=262144, )

        topics = dict(topic=dict(partitions=3, cleanup_policy="compact"))

        super(TopicDeleteTest, self).__init__(test_context=test_context,
                                              num_brokers=3,
                                              extra_rp_conf=extra_rp_conf,
                                              topics=topics)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    @cluster(num_node=3)
    def topic_delete_test(self):
        def produce_until_partitions():
            self.kafka_tools.produce("topic", 1024, 1024)
            storage = self.redpanda.storage()
            return len(list(storage.partitions("kafka", "topic"))) == 9

        wait_until(lambda: produce_until_partitions(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Expected partition did not materialize")

        self.kafka_tools.delete_topic("topic")

        def topic_storage_purged():
            storage = self.redpanda.storage()
            return all(
                map(lambda n: "topic" not in n.ns["kafka"].topics,
                    storage.nodes))

        wait_until(lambda: topic_storage_purged(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Topic storage was not removed")
