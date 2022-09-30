# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.services.cluster import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST


class FullNodeRecoveryTest(EndToEndTest):
    PARTIAL_RECOVERY = 'partial'
    FULL_RECOVERY = 'full'
    """
    This test validates recovery of redpanda node after data directory wipe
    """
    def __init__(self, test_context):
        extra_rp_conf = dict(enable_leader_balancer=False,
                             default_topic_replications=3,
                             group_topic_partitions=3)

        super(FullNodeRecoveryTest, self).__init__(test_context=test_context,
                                                   extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @parametrize(recovery_type=PARTIAL_RECOVERY)
    @parametrize(recovery_type=FULL_RECOVERY)
    def test_node_recovery(self, recovery_type):
        self.start_redpanda(num_nodes=3)
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_cat = KafkaCat(self.redpanda)
        # create topics
        topics = []
        for _ in range(0, 6):
            topics.append(TopicSpec(partition_count=random.randint(1, 10)))
        # chose one topic to run the main workload
        DefaultClient(self.redpanda).create_topic(topics)
        self.topic = random.choice(topics).name

        self.start_producer(1)
        self.start_consumer(2)
        self.await_startup()

        # chose another topic and populate it with data
        prepopulated_topic = random.choice(topics)

        while self.topic == prepopulated_topic.name:
            prepopulated_topic = random.choice(topics)

        # populate topic with data
        kafka_tools.produce(prepopulated_topic.name, 20000, 1024)

        def list_offsets():
            offsets = {}
            for p in range(0, prepopulated_topic.partition_count):
                offsets[p] = kafka_cat.list_offsets(prepopulated_topic.name, p)

        # store offsets
        offsets = list_offsets()

        self.redpanda.logger.info(f"Topic offsets: {offsets}")
        # stop one of the nodes and remove its data
        stopped = random.choice(self.redpanda.nodes)
        # prepare seed servers list
        seeds = map(lambda n: {
            "address": n.account.hostname,
            "port": 33145
        }, self.redpanda.nodes)
        seeds = list(
            filter(lambda n: n['address'] != stopped.account.hostname, seeds))

        self.redpanda.stop_node(stopped)
        if recovery_type == FullNodeRecoveryTest.FULL_RECOVERY:
            self.redpanda.clean_node(stopped, preserve_logs=True)

        # produce some more data to make sure that stopped node is behind
        kafka_tools.produce(prepopulated_topic.name, 20000, 1024)

        # start node with the same node id, and not empty seed server list to

        # give node more time to start as it has to recover
        self.redpanda.start_node(stopped,
                                 override_cfg_params={'seed_servers': seeds},
                                 timeout=90)

        def all_topics_recovered():
            metric = self.redpanda.metrics_sample("under_replicated_replicas",
                                                  self.redpanda.nodes)
            under_replicated = filter(lambda s: s.value == 1, metric.samples)
            under_replicated = list(
                map(
                    lambda s: (s.labels['namespace'], s.labels['topic'], s.
                               labels['partition']), under_replicated))
            self.redpanda.logger.info(
                f"under replicated partitions: {list(under_replicated)}")
            return len(under_replicated) == 0

        self.run_validation(min_records=20000,
                            enable_idempotence=False,
                            producer_timeout_sec=60,
                            consumer_timeout_sec=180)

        # wait for prepopulated topic to recover
        wait_until(all_topics_recovered, 60, 1)

        # validate prepopulated topic offsets
        assert offsets == list_offsets()
