# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.end_to_end import EndToEndTest

from ducktape.utils.util import wait_until


class LeadersInfoApiTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3, replication_factor=3), )

    def __init__(self, test_context):
        super(LeadersInfoApiTest, self).__init__(test_context=test_context,
                                                 num_brokers=3)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    def reset_leaders_info_test(self):
        def check_reset_leaders():
            node = self.redpanda.nodes[0]
            self.admin.reset_leaders_info(node)

            partition_without_leader = 0
            for partition in range(self.topics[0].partition_count):
                leader = self.admin.get_partition_leader(namespace="kafka",
                                                         topic=self.topics[0],
                                                         partition=partition,
                                                         node=node)
                if leader == -1:
                    partition_without_leader += 1

            return partition_without_leader >= 2

        wait_until(check_reset_leaders,
                   timeout_sec=180,
                   backoff_sec=1,
                   err_msg="Can not reset leaders_table for nodes")

        def check_get_leaders():
            for partition in range(self.topics[0].partition_count):
                leader0 = self.admin.get_partition_leader(
                    namespace="kafka",
                    topic=self.topics[0],
                    partition=partition,
                    node=self.redpanda.nodes[0])

                leader1 = self.admin.get_partition_leader(
                    namespace="kafka",
                    topic=self.topics[0],
                    partition=partition,
                    node=self.redpanda.nodes[1])

                return leader0 == leader1

        wait_until(check_get_leaders,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Can not refresh leaders")

    @cluster(num_nodes=3)
    def get_leaders_info_test(self):
        def check_reset_leaders():
            node = self.redpanda.nodes[0]
            self.admin.reset_leaders_info(node)
            leaders = self.admin.get_leaders_info(node)
            return len(leaders) == 0

        wait_until(check_reset_leaders,
                   timeout_sec=180,
                   backoff_sec=1,
                   err_msg="Can not reset leaders_table for nodes")

        def check_get_leaders():
            def compare_key(e):
                return (e["ns"], e["topic"], e["partition_id"])

            leaders_node1 = self.admin.get_leaders_info(self.redpanda.nodes[0])
            leaders_node1.sort(key=compare_key)
            leaders_node2 = self.admin.get_leaders_info(self.redpanda.nodes[1])
            leaders_node2.sort(key=compare_key)
            return leaders_node1 == leaders_node2

        wait_until(check_get_leaders,
                   timeout_sec=180,
                   backoff_sec=1,
                   err_msg="Can not refresh leaders")


class LeadersInfoApiEndToEndTest(EndToEndTest):
    @cluster(num_nodes=5)
    def reset_leaders_info_end_to_end_test(self):
        self.start_redpanda(num_nodes=3)
        self.admin = Admin(self.redpanda)

        self.spec = TopicSpec(partition_count=3, replication_factor=3)

        self.client().create_topic(self.spec)
        self.topic = self.spec.name

        self.start_producer(1, throughput=100)
        self.start_consumer(1)
        self.await_startup(min_records=100, timeout_sec=180)

        for node in self.redpanda.nodes:
            self.admin.reset_leaders_info(node)

        self.run_validation(min_records=10000,
                            producer_timeout_sec=180,
                            consumer_timeout_sec=180)
