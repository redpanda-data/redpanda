# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.clients.kafka_cat import KafkaCat

from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest


class TxFeatureFlagTest(EndToEndTest):
    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_disabling_transactions_after_they_being_used(self):
        '''
        Validate that transactions can be safely disabled after 
        the feature have been used
        '''
        # start redpanda with tranasactions enabled, we use
        # replication factor 1 for group topic to make
        # it unavailable when one of the nodes is down,
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={
                                "default_topic_replications": 1,
                                "default_topic_partitions": 1,
                                "health_manager_tick_interval": 3600000
                            })

        tx_topic = TopicSpec(name="tx-topic",
                             partition_count=1,
                             replication_factor=3)
        self.client().create_topic(tx_topic)

        # produce some messages to tx_topic

        kcat = KafkaCat(self.redpanda)
        kcat.produce_one(tx_topic.name, msg='test-msg', tx_id='test-tx-id')

        # disable transactions,
        self.redpanda.stop()

        for n in self.redpanda.nodes:
            self.redpanda.start_node(n,
                                     override_cfg_params={
                                         "transactional_id_expiration_ms":
                                         1000,
                                         "default_topic_replications": 3,
                                         "default_topic_partitions": 1
                                     })

        # create topic for test
        tester = TopicSpec(name="tester",
                           partition_count=1,
                           replication_factor=3)
        self.client().create_topic(tester)
        self.topic = tester
        self.start_producer(2, throughput=10000)
        self.start_consumer(1)
        self.await_startup()

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)

        # make sure that all redpanda nodes are up and running
        for n in self.redpanda.nodes:
            assert self.redpanda.redpanda_pid(n) != None
