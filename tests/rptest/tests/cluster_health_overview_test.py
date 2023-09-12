# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST

from ducktape.utils.util import wait_until


class ClusterHealthOverviewTest(RedpandaTest):
    def __init__(self, test_context):
        super(ClusterHealthOverviewTest, self).__init__(
            test_context=test_context,
            num_brokers=5,
            extra_rp_conf={
                # Work around bug where leadership transfers cause bad health reports
                # https://github.com/redpanda-data/redpanda/issues/5253
                'enable_leader_balancer': False
            })

        self.admin = Admin(self.redpanda)

    def create_topics(self):
        topics = []
        for i in range(0, 8):
            topics.append(
                TopicSpec(partition_count=random.randint(1, 6),
                          replication_factor=3))

        self.client().create_topic(topics)

    def get_health(self):
        """Wrapper around admin.get_cluster_health_overview which validates some invariants
        about each health report"""

        hov = self.admin.get_cluster_health_overview()

        # these invariants should always hold
        if hov['is_healthy']:
            assert len(hov['nodes_down']) == 0
            assert len(hov['leaderless_partitions']) == 0
            assert hov['leaderless_count'] == 0
            assert len(hov['under_replicated_partitions']) == 0
            assert hov['under_replicated_count'] == 0
            assert len(hov['unhealthy_reasons']) == 0
            assert len(hov['all_nodes']) > 0
        else:
            assert len(hov['unhealthy_reasons']) > 0
            # these next two are true just because we don't go over the max of 128
            # reported partitions in these tests
            assert len(hov['leaderless_partitions']) == hov['leaderless_count']
            assert len(hov['under_replicated_partitions']
                       ) == hov['under_replicated_count']

        return hov

    def wait_until_healthy(self):
        def is_healthy():
            res = self.get_health()
            return res['is_healthy'] == True and len(res['all_nodes']) == 5

        wait_until(is_healthy, 30, 2)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def cluster_health_overview_baseline_test(self):
        self.create_topics()

        # in initial state after all nodes joined cluster should be healthy
        self.wait_until_healthy()

        # stop one node, cluster should become unhealthy with one node down
        # reported and no leaderless partitions

        first_down = random.choice(self.redpanda.nodes)
        self.redpanda.stop_node(first_down)

        def one_node_down():
            hov = self.get_health()
            if not hov['is_healthy'] and len(hov['nodes_down']) > 0:
                # when the health report flips to not healthy, we check that
                # the expected node is reported as down and unhealthy reasons line up
                assert [self.redpanda.idx(first_down)] == hov['nodes_down']
                # next check is "in" instead of "==" because we may also have under_replicated_partitions
                assert 'nodes_down' in hov['unhealthy_reasons']
                return True
            return False

        wait_until(one_node_down, 30, 2)

        # stop another node, cluster should start reporting leaderless
        # partitions with two out of five nodes down

        second_down = random.choice(self.redpanda.nodes)
        while self.redpanda.idx(second_down) == self.redpanda.idx(first_down):
            second_down = random.choice(self.redpanda.nodes)

        self.redpanda.stop_node(second_down)

        def two_nodes_down():
            hov = self.get_health()
            if hov['is_healthy'] or len(hov['nodes_down']) != 2:
                return False

            if len(hov['leaderless_partitions']) == 0:
                return False

            assert 'leaderless_partitions' in hov['unhealthy_reasons']

            return True

        wait_until(two_nodes_down, 30, 2)

        # restart both nodes, cluster should be healthy back again
        self.redpanda.start_node(first_down)
        self.redpanda.start_node(second_down)

        self.wait_until_healthy()
