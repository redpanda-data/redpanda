# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from collections import defaultdict


class RackAwarePlacementTest(RedpandaTest):
    def __init__(self, test_context):
        super(RackAwarePlacementTest,
              self).__init__(test_context=test_context,
                             num_brokers=6,
                             extra_rp_conf={
                                 'enable_rack_awareness': True,
                                 'segment_fallocation_step': 1024,
                             })

    def setUp(self):
        # Delay startup, so that the test case can configure redpanda
        # based on test parameters before starting it.
        pass

    def _get_partition_count(self, topic):
        try:
            m = self.client().describe_topic(topic)
            return len(m.partitions)
        except Exception as err:
            self.logger.info(f"describe_topic error: {err}")
            return 0

    def _create_topic(self, topic):
        """Create topic and wait until all partitions are created.
        """
        self.client().create_topic(topic)

        wait_until(
            lambda: self._get_partition_count(topic.name
                                              ) == topic.partition_count,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=
            f"Not all partitions are created in time, expected {topic.partition_count} partitions, found {self._get_partition_count(topic.name)}"
        )

    def _validate_placement(self, topic, rack_layout, num_replicas):
        """Validate the replica placement. The method uses provided
        rack layout and number of replicas for the partitions. 
        The validation is done by examining existing replica placemnt
        against the rack layout. The validation succedes if every replica
        is placed on a different rack or if there is not enough racks on
        every available rack.
        """
        num_racks = len(set(rack_layout))
        m = self.client().describe_topic(topic.name)
        replicas = defaultdict(list)
        for part in m.partitions:
            for broker in part.replicas:
                rack = rack_layout[broker - 1]  # broker ids are 1-based
                replicas[part.id].append(rack)

        for part_id, racks in replicas.items():
            expected_racks = min(num_racks, num_replicas)
            self.logger.info(
                f"partition {part_id} is replicated to racks {racks}, expected {expected_racks} racks, total racks {num_racks}"
            )
            assert len(set(racks)) == min(num_racks, num_replicas)

    @cluster(num_nodes=6)
    @matrix(rack_layout_str=['ABCDEF', 'xxYYzz', 'ooooFF'],
            num_partitions=[50, 400],
            replication_factor=[3, 5],
            num_topics=[2])
    def test_replica_placement(self, rack_layout_str, num_partitions,
                               replication_factor, num_topics):
        """
        Test replica placement. The test case creates a set of brokers with
        initialized 'rack' parameter in the configuration. The 'rack' property
        is defined by the 'rack_layout' parameter. Then it creates a topic with
        pre-defined number of partitions and replication factor and validates
        the replica placement.

        The validation procedure checks that:
        - either every replica lives on a different rack;
        - or every rack is used to place one or more replicas of the partition
          (if replication factor is larger than number of racks)

        ----
        @param rack_layout_str is a 6-char string with all rack names combined into a signle string.
                               Every character of the string corresponds to one broker. Every
                               rack id has single character.
        @param num_partitions defines number of partitions that needs to be created.
        @param replication_factor defines recplication factor of all partitions.
        """

        rack_layout = [str(i) for i in rack_layout_str]
        assert len(rack_layout) == 6

        for ix, node in enumerate(self.redpanda.nodes):
            extra_node_conf = {
                # We're introducing two racks, small and large.
                # The small rack has only one node and the
                # large one has four nodes.
                'rack': rack_layout[ix],
                # This parameter enables rack awareness
                'enable_rack_awareness': True,
                'segment_fallocation_step': 1024,
            }
            self.redpanda.set_extra_node_conf(node, extra_node_conf)

        self.redpanda.start()

        if num_topics * num_partitions * replication_factor > 3000:
            # Combination of 2 topics, 5 replicas and 400 partitions can't be run
            # reliably on both debug and release due to ducktape limitations.
            self.logger.info(
                f"combination of num_partitions: {num_partitions}, replication_factor: {replication_factor}, and num_topics: {num_topics} is excluded"
            )
            return

        topics = []
        for tix in range(0, num_topics):
            topic = TopicSpec(partition_count=num_partitions,
                              replication_factor=replication_factor)
            topics.append(topic)
            self._create_topic(topic)

        for topic in topics:
            self._validate_placement(topic, rack_layout, replication_factor)
