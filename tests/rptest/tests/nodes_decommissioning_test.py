# Copyright 2020 Vectorized, Inc.
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
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST
from rptest.util import Scale
from rptest.services.redpanda import RedpandaService
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.archival.s3_client import S3Client


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
            for replication_factor in (3, 3):
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
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
            for replication_factor in (3, 3):
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
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

    def _setup_remote_io(self, topics, segment_size, bucket_name):
        rpk = RpkTool(self.redpanda)
        for spec in topics:
            rpk.alter_topic_config(spec.name, 'redpanda.remote.write', 'true')
            rpk.alter_topic_config(spec.name, 'redpanda.remote.read', 'true')
            rpk.alter_topic_config(spec.name, 'retention.bytes',
                                   str(segment_size))

        # Verify that we really enabled shadow indexing correctly, such
        # that some objects were written
        def check_si():
            objects = list(self.redpanda.get_objects_from_si())
            return len(objects) > 0

        wait_until(check_si,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='No S3 objects written.')

        objects = list(self.redpanda.get_objects_from_si())
        for o in objects:
            self.logger.info(f"S3 object: {o.Key}, {o.ContentLength}")

        def check_bucket():
            buckets = self.redpanda.list_buckets()
            self.logger.debug(f'buckets: {buckets}')

            if len(buckets) != 1:
                return False

            if buckets[0]['Name'] != bucket_name:
                return False

            return True

        wait_until(check_bucket,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='Failed to create bucket.')


class SINodesDecommissioningTest(EndToEndTest):
    """
    Enable SI on a topic and test node decommissioning
    """
    segment_size = 1048576  # 1MB
    s3_host_name = "minio-s3"
    s3_access_key = "panda-user"
    s3_secret_key = "panda-secret"
    s3_region = "panda-region"
    s3_topic_name = "panda-topic"
    topics = []

    def __init__(self, test_context):
        super(SINodesDecommissioningTest,
              self).__init__(test_context=test_context)

        self.s3_bucket_name = f"panda-bucket-{uuid.uuid1()}"
        self._extra_rp_conf = dict(
            cloud_storage_enabled=True,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
            cloud_storage_access_key=self.s3_access_key,
            cloud_storage_secret_key=self.s3_secret_key,
            cloud_storage_region=self.s3_region,
            cloud_storage_bucket=self.s3_bucket_name,
            cloud_storage_disable_tls=True,
            cloud_storage_api_endpoint=self.s3_host_name,
            cloud_storage_api_endpoint_port=9000,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=self.segment_size,  # 1MB
            cloud_storage_cache_size=5 * self.segment_size,
        )

        self.scale = Scale(test_context)
        self.redpanda = RedpandaService(
            context=test_context,
            num_brokers=4,
            extra_rp_conf=self._extra_rp_conf,
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.s3_client = S3Client(
            region=self.s3_region,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            endpoint=f"http://{self.s3_host_name}:9000",
            logger=self.logger,
        )

    def setUp(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        self.s3_client.create_bucket(self.s3_bucket_name)
        self.redpanda.start()

        for partition_count in range(1, 5):
            for replication_factor in (3, 3):
                name = f'topic{len(self.topics)}'
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                self.topics.append(spec)

        for spec in self.topics:
            self.kafka_tools.create_topic(spec)
            self.topic = spec.name

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_working_node_with_si(self):
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
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
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
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