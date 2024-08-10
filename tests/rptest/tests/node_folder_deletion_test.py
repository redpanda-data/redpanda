# Copyright 2020 Redpanda Data, Inc.
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.clients.consumer_offsets_recovery import ConsumerOffsetsRecovery
from rptest.services.admin import Admin
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
from rptest.utils.mode_checks import skip_debug_mode

from rptest.utils.node_operations import NodeDecommissionWaiter


class NodeFolderDeletionTest(PreallocNodesTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, node_prealloc_count=1)

    def setUp(self):
        # Defer startup to test body.
        pass

    @skip_debug_mode
    @cluster(num_nodes=4)
    def test_deleting_node_folder(self):

        # new bootstrap
        self.redpanda.start(auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
        topics = []
        for i in range(5):
            topics.append(TopicSpec(partition_count=16, replication_factor=3))

        self.client().create_topic(topics)
        topic = topics[0]
        msg_size = 1024
        msg_cnt = 400000

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic.name,
                                       msg_size,
                                       msg_cnt,
                                       custom_node=self.preallocated_nodes,
                                       rate_limit_bps=msg_size * 1000)

        producer.start(clean=False)

        wait_until(lambda: producer.produce_status.acked > 100,
                   timeout_sec=60,
                   backoff_sec=0.5)

        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size,
            readers=3,
            nodes=self.preallocated_nodes)

        consumer.start(clean=False)

        wait_until(lambda: producer.produce_status.acked > 100000,
                   timeout_sec=120,
                   backoff_sec=0.5)
        # explicitly skip node 0 as this is a seed server and its id doesn't change
        to_stop = random.choice(self.redpanda.nodes[1:])
        id = self.redpanda.node_id(to_stop)

        # remove node data folder
        self.redpanda.stop_node(to_stop)
        self.redpanda.clean_node(to_stop)
        # start node back up
        self.redpanda.start_node(to_stop,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False)
        # assert that node id has changed
        assert id != self.redpanda.node_id(to_stop, force_refresh=True)
        wait_until(lambda: producer.produce_status.acked > 200000,
                   timeout_sec=120,
                   backoff_sec=0.5)

        admin = Admin(self.redpanda)

        # validate that the node with deleted folder is recognized as offline
        def removed_node_is_reported_offline():
            cluster_health = admin.get_cluster_health_overview()
            return id in cluster_health['nodes_down']

        wait_until(
            removed_node_is_reported_offline,
            timeout_sec=20,
            backoff_sec=0.5,
            err_msg=
            f"Node {id} is expected to be marked as offline as it was replaced by new node"
        )

        # decommission a node that has been cleared
        admin.decommission_broker(id)
        waiter = NodeDecommissionWaiter(self.redpanda,
                                        id,
                                        self.logger,
                                        progress_timeout=60)

        waiter.wait_for_removal()

        wait_until(lambda: producer.produce_status.acked > 300000,
                   timeout_sec=120,
                   backoff_sec=0.5)
