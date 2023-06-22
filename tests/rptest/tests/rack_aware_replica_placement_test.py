# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import os
from collections import defaultdict
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.clients.default import DefaultClient
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import cleanup_on_early_exit


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

    def _validate_placement(self,
                            topic,
                            rack_layout,
                            num_replicas,
                            ids_mapping={}):
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
                # broker ids are 1-based
                id = ids_mapping[broker] if broker in ids_mapping else broker
                rack = rack_layout[id - 1]
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

        # https://github.com/redpanda-data/redpanda/issues/11276
        if self.debug_mode and num_partitions == 400:
            self.logger.info(
                "Disabling test in debug mode due to slowness/timeouts with large number of partitions."
            )
            cleanup_on_early_exit(self)
            return

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
                'segment_fallocation_step': 4096,
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

    def _partition_count(self):
        # use smaller number of partitions for debug builds
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            return random.randint(20, 40)

        return random.randint(200, 300)

    @cluster(num_nodes=6)
    @matrix(rack_layout=['ABCDEF', 'xxYYzz', 'ooooFF'])
    def test_rack_awareness_after_node_operations(self, rack_layout):
        replication_factor = 3

        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(
                node,
                {
                    "rack": rack_layout[ix],
                    'enable_rack_awareness': True,
                    # make fallocation step small to spare the disk space
                    'segment_fallocation_step': 4096,
                })

        self.redpanda.start()
        self._client = DefaultClient(self.redpanda)

        topic = TopicSpec(partition_count=self._partition_count(),
                          replication_factor=replication_factor)
        self.client().create_topic(topic)
        self._validate_placement(topic, rack_layout, replication_factor)

        # decommission one of the nodes
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        admin.decommission_broker(to_decommission)

        def node_is_removed():
            for n in self.redpanda.nodes:
                if self.redpanda.idx(n) == to_decommission:
                    continue
                brokers = admin.get_brokers(node=n)
                for b in brokers:
                    if b['node_id'] == to_decommission:
                        return False
            return True

        # wait until node is removed and validate rack placement
        wait_until(node_is_removed, 90, 2)

        self._validate_placement(topic, rack_layout, replication_factor)

        # stop the node and add it back with different id
        to_stop = self.redpanda.get_node(to_decommission)
        self.redpanda.stop_node(to_stop)
        self.redpanda.clean_node(node=to_stop, preserve_logs=True)

        new_node_id = 123

        def seed_servers_for(idx):
            seeds = map(
                lambda n: {
                    "address": n.account.hostname,
                    "port": 33145
                }, self.redpanda.nodes)

            return list(
                filter(
                    lambda n: n['address'] != self.redpanda.get_node(idx).
                    account.hostname, seeds))

        # add a node back with different id but the same rack
        # change the seed server list to prevent node from forming new cluster
        self.redpanda.start_node(
            to_stop,
            override_cfg_params={
                "node_id": new_node_id,
                "seed_servers": seed_servers_for(to_decommission),
                "rack": rack_layout[to_decommission - 1],
                'enable_rack_awareness': True,
                # make fallocation step small to spare the disk space
                'segment_fallocation_step': 4096,
            })

        rpk = RpkTool(self.redpanda)

        def new_node_has_partitions():
            for p in rpk.describe_topic(topic.name):
                for r in p.replicas:
                    if r == 123:
                        return True

            return False

        wait_until(new_node_has_partitions, 90, 2)

        def no_partitions_moving():
            for n in self.redpanda.nodes:
                r = admin.list_reconfigurations(node=n)
                if len(r) > 0:
                    return False

            return True

        wait_until(no_partitions_moving, 90, 1)

        self._validate_placement(topic,
                                 rack_layout,
                                 replication_factor,
                                 ids_mapping={new_node_id: to_decommission})

    @cluster(num_nodes=6)
    def test_node_config_update(self):
        """
        * Create a cluster from nodes with no rack id configured
        * edit node configs adding rack ids
        * restart the nodes
        * check that rack aware placement works
        """
        rack_layout = 'AABBCC'
        replication_factor = 3
        num_partitions = 10

        self.redpanda.start()

        self.redpanda.stop()
        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.start_node(
                node, override_cfg_params={"rack": rack_layout[ix]})

        admin = Admin(self.redpanda)

        def rack_ids_updated():
            for n in self.redpanda.nodes:
                if any('rack' not in b for b in admin.get_brokers(n)):
                    return False
            return True

        wait_until(rack_ids_updated,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="node configurations didn't get updated")

        topic = TopicSpec(partition_count=num_partitions,
                          replication_factor=replication_factor)
        self._create_topic(topic)
        self._validate_placement(topic, rack_layout, replication_factor)
