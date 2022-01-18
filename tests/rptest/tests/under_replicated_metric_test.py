# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from time import sleep
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from rptest.clients.default import DefaultClient

from rptest.clients.types import TopicSpec
from rptest.services.metrics_check import MetricCheck
from rptest.services.rpk_producer import RpkProducer

from rptest.tests.end_to_end import EndToEndTest
import re
import sys


class UnderReplicatedMetricTest(EndToEndTest):
    def __init__(self, test_context):
        extra_rp_conf = dict(enable_leader_balancer=False,
                             default_topic_replications=3,
                             group_topic_partitions=3)

        super(UnderReplicatedMetricTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    def check_no_under_replicated(self, checks):

        return all(
            c.evaluate([(
                "vectorized_cluster_partition_under_replicated_replicas",
                lambda _, new_value: int(new_value) == 0)]) for c in checks)

    def create_metric_checks(self):
        checks = []
        for n in self.redpanda.nodes:
            checks.append(
                MetricCheck(self.logger,
                            self.redpanda,
                            n,
                            re.compile(".*under_replicated.*"),
                            {'topic': self.topic},
                            reduce=sum))
        return checks

    @cluster(num_nodes=6)
    def test_under_replicated_metric_no_failures(self):
        self.start_redpanda(num_nodes=3)
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

        metric_checks = self.create_metric_checks()

        self.run_validation(min_records=20000,
                            enable_idempotence=False,
                            producer_timeout_sec=60,
                            consumer_timeout_sec=180)

        # all partitions should be fully replicated
        assert self.check_no_under_replicated(metric_checks)

    @cluster(num_nodes=4)
    @parametrize(acks=1)
    @parametrize(acks=-1)
    def test_under_replicated_metric_with_different_acks(self, acks):
        self.start_redpanda(num_nodes=3)
        # create topics
        topics = []
        for _ in range(0, 6):
            topics.append(TopicSpec(partition_count=random.randint(1, 10)))
        # chose one topic to run the main workload
        DefaultClient(self.redpanda).create_topic(topics)
        self.topic = random.choice(topics).name

        rpk_producer = RpkProducer(self.test_context,
                                   self.redpanda,
                                   self.topic,
                                   16384,
                                   20000,
                                   acks=acks)

        metric_checks = self.create_metric_checks()
        rpk_producer.start()
        rpk_producer.wait()
        rpk_producer.stop()

        assert self.check_no_under_replicated(metric_checks)

    @cluster(num_nodes=4)
    @parametrize(acks=1)
    @parametrize(acks=-1)
    def test_under_replicated_when_node_is_down(self, acks):
        # setup recovery read size to speed things up
        self.start_redpanda(
            num_nodes=3,
            extra_rp_conf={"raft_recovery_default_read_size": 4194304})
        topics = []
        for _ in range(0, 6):
            topics.append(TopicSpec(partition_count=random.randint(1, 10)))
        # chose one topic to run the main workload
        DefaultClient(self.redpanda).create_topic(topics)
        self.topic = random.choice(topics).name

        rpk_producer = RpkProducer(self.test_context,
                                   self.redpanda,
                                   self.topic,
                                   128,
                                   sys.maxsize,
                                   acks=acks)

        metric_checks = self.create_metric_checks()
        rpk_producer.start()

        node = random.choice(self.redpanda.nodes)
        self.redpanda.stop_node(node)

        def metric_checks_from_running_nodes():
            return filter(lambda ch: ch.node != node, metric_checks)

        def has_under_replicated_replicas():
            return all(
                c.evaluate([(
                    "vectorized_cluster_partition_under_replicated_replicas",
                    lambda _, new_value: int(new_value) > 0)])
                for c in metric_checks_from_running_nodes())

        def has_no_under_replicated_replicas():
            return self.check_no_under_replicated(metric_checks)

        wait_until(has_under_replicated_replicas, 60, 1,
                   "Waiting for under replicated replicas to be reported")

        # restart redpanda, all under replicated replicas should heal
        self.redpanda.start_node(node)
        wait_until(has_no_under_replicated_replicas, 90, 1,
                   "Waiting for under replicated replicas heal")
