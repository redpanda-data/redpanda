# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.services.rpk_producer import RpkProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.scale_tests.many_partitions_test import ScaleParameters


class RpkManyPartitionsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(RpkManyPartitionsTest,
              self).__init__(test_ctx,
                             num_brokers=8,
                             extra_rp_conf={
                                 'topic_partitions_per_shard': 10000,
                                 'topic_memory_per_partition': None,
                             },
                             *args,
                             **kwargs)
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=12)
    def test_rpk_simple(self):
        """
        This tests rpk against a cluster with a significant number of partitions.
        This is a simple test, and it does not take into account the stress of
        a large number of consumers.
        """
        msg_count = 1000
        partition_count_1 = 50000
        partition_count_2 = 80000
        group_name = "g1"
        topic_spec = TopicSpec(name="foo",
                               partition_count=partition_count_1,
                               replication_factor=3)

        self._rpk.create_topic(topic=topic_spec.name,
                               partitions=topic_spec.partition_count,
                               replicas=topic_spec.replication_factor)

        # Produce 1k messages of 16KB size to the topic
        # using `rpk topic produce` and then consuming
        # from it.
        def _produce_and_consume():
            rpk_producer = RpkProducer(self.test_context,
                                       self.redpanda,
                                       topic=topic_spec.name,
                                       msg_size=16384,
                                       msg_count=msg_count,
                                       printable=True,
                                       produce_timeout=60)
            rpk_producer.start()

            # Consume messages from the topic with
            # `rpk topic consume`.
            rpk_consumer = RpkConsumer(self.test_context,
                                       self.redpanda,
                                       save_msgs=False,
                                       retry_sec=0.5,
                                       topic=topic_spec.name,
                                       group=group_name)
            rpk_consumer.start()

            wait_until(lambda: rpk_consumer.message_count >= msg_count,
                       timeout_sec=120,
                       backoff_sec=5,
                       err_msg="RPK failed to fetch all messages")

            self.logger.debug(
                f"RPK successfully fetched {rpk_consumer.messages}")

            # `rpk topic consume` is in a loop that will
            # run forever unless we call stop here
            rpk_consumer.stop()

            # `rpk topic produce` should be done already but
            # we call wait and stop just to be sure
            rpk_producer.wait()
            rpk_producer.stop()

        def _describe_topic(topic_name, partition_count):
            try:
                partitions = list(
                    self._rpk.describe_topic(topic_name, tolerant=True))
            except RpkException as e:
                # Retry once.
                self.logger.error(f"Retrying describe_topic for {e}")
                partitions = list(
                    self._rpk.describe_topic(topic_name, tolerant=True))

            if len(partitions) < partition_count:
                self.logger.info(
                    f"rpk topic describe omits partitions for topic {topic_name}"
                )

            return len(partitions) == partition_count

        # Producing 1K messages to a 50K partitions topic.
        _produce_and_consume()

        # Describe the topic with `rpk topic describe`
        wait_until(
            lambda: _describe_topic(topic_spec.name, partition_count_1),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=
            f"RPK failed to describe topic {topic_spec.name} with {partition_count_1} partitions"
        )

        # Then we add the rest of the partitions to get to 80K.
        self._rpk.add_partitions(topic_spec.name,
                                 partition_count_2 - partition_count_1)

        # Producing another 1K messages but with 80K partitions.
        _produce_and_consume()

        wait_until(
            lambda: _describe_topic(topic_spec.name, partition_count_2),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=
            f"RPK failed to describe topic {topic_spec.name} with {partition_count_2} partitions"
        )

        # Check the output of `rpk cluster health`.
        health = self._rpk.cluster_health()
        assert len(health.nodes_down
                   ) == 0, "rpk cluster health failed: too many downed nodes"

        groups = self._rpk.group_list()
        assert group_name in groups, f"unable to find group '{group_name}' in the group list"

        # Make sure that we don't timeout with a large number of partitions.
        self._rpk.group_describe(group_name)
