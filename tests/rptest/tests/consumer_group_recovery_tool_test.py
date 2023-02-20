# Copyright 2020 Redpanda Data, Inc.
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.consumer_offsets_recovery import ConsumerOffsetsRecovery
from rptest.services.cluster import cluster

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize


class ConsumerOffsetsRecoveryToolTest(PreallocNodesTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        self.initial_partition_count = 3
        super(ConsumerOffsetsRecoveryToolTest, self).__init__(
            test_ctx,
            num_brokers=3,
            *args,
            # disable leader balancer to make sure that group will not be realoaded because of leadership changes
            extra_rp_conf={
                # clear topics from the the kafka_nodelete_topics to allow for
                # __consumer_offsets to be configured in this test.
                "kafka_nodelete_topics": [],
                "group_topic_partitions": self.initial_partition_count
            },
            node_prealloc_count=1,
            **kwargs)

    def describe_all_groups(self):
        rpk = RpkTool(self.redpanda)
        all_groups = {}
        for g in rpk.group_list():
            gd = rpk.group_describe(g)
            all_groups[gd.name] = {}
            for p in gd.partitions:
                all_groups[
                    gd.name][f"{p.topic}/{p.partition}"] = p.current_offset

        return all_groups

    @cluster(num_nodes=4)
    def test_consumer_offsets_partition_count_change(self):
        topic = TopicSpec(partition_count=16, replication_factor=3)
        self.client().create_topic([topic])
        msg_size = 1024
        msg_cnt = 10000

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic.name,
                                       msg_size,
                                       msg_cnt,
                                       custom_node=self.preallocated_nodes)

        producer.start(clean=False)

        wait_until(lambda: producer.produce_status.acked > 10,
                   timeout_sec=30,
                   backoff_sec=0.5)

        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size,
            readers=3,
            nodes=self.preallocated_nodes)
        consumer.start(clean=False)

        producer.wait()
        consumer.wait()

        assert consumer.consumer_status.validator.valid_reads >= producer.produce_status.acked

        groups_pre_migration = self.describe_all_groups()

        cgr = ConsumerOffsetsRecovery(self.redpanda)

        # execute recovery tool, ask for 16 partitions in consumer offsets topic
        #
        # this is dry run, expect that partition count didn't change
        cgr.change_partition_count(16, self.redpanda.nodes[0], dry_run=True)
        rpk = RpkTool(self.redpanda)
        tp_desc = list(rpk.describe_topic("__consumer_offsets"))

        # check if topic partition count changed
        assert len(tp_desc) == self.initial_partition_count

        # now allow the tool to execute
        cgr.change_partition_count(16, self.redpanda.nodes[0], dry_run=False)

        # check if topic partition count changed
        wait_until(
            lambda: len(list(rpk.describe_topic("__consumer_offsets"))) == 16,
            20)

        groups_post_migration = self.describe_all_groups()

        assert groups_pre_migration == groups_post_migration
