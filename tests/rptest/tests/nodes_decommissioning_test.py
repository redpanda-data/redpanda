# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import os
import time
import uuid

from rptest.services.cluster import cluster
from ducktape.mark import ok_to_fail
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import await_bucket_creation


class NodesDecommissioningTest(EndToEndTest):
    """
    Basic nodes decommissioning test.
    """
    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_working_node(self):
        self.start_redpanda(num_nodes=4)
        topics = []
        for partition_count in range(1, 5):
            name = f"topic{len(topics)}"
            spec = TopicSpec(name=name,
                             partition_count=partition_count,
                             replication_factor=3)
            topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_records_consumed()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)
        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission['node_id'])

        def node_removed():
            brokers = admin.get_brokers()
            for b in brokers:
                if b['node_id'] == to_decommission['node_id']:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decommissioning_crashed_node(self):

        self.start_redpanda(num_nodes=4)
        topics = []
        for partition_count in range(1, 5):
            name = f"topic{len(topics)}"
            spec = TopicSpec(name=name,
                             partition_count=partition_count,
                             replication_factor=3)
            topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_records_consumed()
        admin = Admin(self.redpanda)

        to_decommission = self.redpanda.nodes[1]
        node_id = 2
        self.redpanda.stop_node(node=to_decommission)
        self.logger.info(f"decommissioning node: {node_id}", )
        admin.decommission_broker(id=node_id)

        def node_removed():
            brokers = admin.get_brokers()
            for b in brokers:
                if b['node_id'] == node_id:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)


class SINodesDecommissioningTest(EndToEndTest):
    """
    Enable SI on a topic and test node decommissioning
    """
    segment_size = 1048576  # 1MB
    topics = []

    def __init__(self, test_context):
        super(SINodesDecommissioningTest,
              self).__init__(test_context=test_context)

        self._si_settings = SISettings(
            log_segment_size=self.segment_size,
            cloud_storage_cache_size=5 * self.segment_size,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5)

        self.redpanda = RedpandaService(context=test_context,
                                        num_brokers=4,
                                        si_settings=self._si_settings)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def setUp(self):
        self.redpanda.start()

        for partition_count in range(1, 5):
            name = f'topic{len(self.topics)}'
            spec = TopicSpec(name=name,
                             partition_count=partition_count,
                             replication_factor=3)
            self.topics.append(spec)

        for spec in self.topics:
            self.kafka_tools.create_topic(spec)
            self.topic = spec.name

    def _wait_on_s3_objects(self):
        def check_s3():
            objects = list(self.redpanda.get_objects_from_si())
            return len(objects) > 0

        wait_until(check_s3,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='Failed to write objects to S3')

    @ok_to_fail  # NoSuchBucket error inconsistently causes failure
    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_working_node_with_si(self):
        await_bucket_creation(self.redpanda,
                              self._si_settings.cloud_storage_bucket)

        self.start_producer(1)
        self._wait_on_s3_objects()
        self.start_consumer(1)
        self.await_records_consumed()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)
        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission['node_id'])

        def node_removed():
            brokers = admin.get_brokers()
            for b in brokers:
                if b['node_id'] == to_decommission['node_id']:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decommissioning_crashed_node_with_si(self):
        await_bucket_creation(self.redpanda,
                              self._si_settings.cloud_storage_bucket)

        self.start_producer(1)
        self._wait_on_s3_objects()
        self.start_consumer(1)
        self.await_records_consumed()
        admin = Admin(self.redpanda)

        to_decommission = self.redpanda.nodes[1]
        node_id = 2
        self.redpanda.stop_node(node=to_decommission)
        self.logger.info(f"decommissioning node: {node_id}", )
        admin.decommission_broker(id=node_id)

        def node_removed():
            brokers = admin.get_brokers()
            for b in brokers:
                if b['node_id'] == node_id:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)