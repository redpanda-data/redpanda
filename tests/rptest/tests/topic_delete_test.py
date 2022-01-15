# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools


class TopicDeleteTest(RedpandaTest):
    """
    Verify that topic deletion cleans up storage.
    """
    topics = (TopicSpec(partition_count=3,
                        cleanup_policy=TopicSpec.CLEANUP_COMPACT), )

    def __init__(self, test_context):
        extra_rp_conf = dict(log_segment_size=262144, )

        super(TopicDeleteTest, self).__init__(test_context=test_context,
                                              num_brokers=3,
                                              extra_rp_conf=extra_rp_conf)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    @cluster(num_nodes=3)
    def topic_delete_test(self):
        def produce_until_partitions():
            self.kafka_tools.produce(self.topic, 1024, 1024)
            storage = self.redpanda.storage()
            return len(list(storage.partitions("kafka", self.topic))) == 9

        wait_until(lambda: produce_until_partitions(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Expected partition did not materialize")

        self.kafka_tools.delete_topic(self.topic)

        def topic_storage_purged():
            storage = self.redpanda.storage()
            return all(
                map(lambda n: self.topic not in n.ns["kafka"].topics,
                    storage.nodes))

        wait_until(lambda: topic_storage_purged(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Topic storage was not removed")
