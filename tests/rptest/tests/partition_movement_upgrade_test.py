# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import threading
from time import sleep
import requests
from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer

from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from ducktape.utils.util import wait_until


class PartitionMovementUpgradeTest(PreallocNodesTest, PartitionMovementMixin):
    def __init__(self, test_context):
        super(PartitionMovementUpgradeTest,
              self).__init__(test_context=test_context,
                             num_brokers=5,
                             node_prealloc_count=1)
        self.installer = self.redpanda._installer
        self._message_size = 128
        self._message_cnt = 30000
        self._stop_move = threading.Event()

    def setUp(self):
        self.old_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        _, self.old_version_str = self.installer.install(
            self.redpanda.nodes, self.old_version)
        super(PartitionMovementUpgradeTest, self).setUp()

    def _start_producer(self, topic_name):
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            self._message_size,
            self._message_cnt,
            custom_node=self.preallocated_nodes)
        self.producer.start(clean=False)

        wait_until(lambda: self.producer.produce_status.acked > 10,
                   timeout_sec=30,
                   backoff_sec=1)

    def _start_consumer(self, topic_name):

        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic_name,
            self._message_size,
            readers=5,
            nodes=self.preallocated_nodes,
            debug_logs=True)
        self.consumer.start(clean=False)

    def verify(self):
        self.producer.wait()

        def finished_consuming():
            self.logger.debug(
                f"verifying, producer acked: {self.producer.produce_status.acked}, "
                f"consumer valid reads: {self.consumer.consumer_status.validator.valid_reads}"
            )
            return self.consumer.consumer_status.validator.valid_reads >= self.producer.produce_status.acked

        # wait for consumers to finish
        wait_until(finished_consuming, 90)

        self.consumer.wait()

    def start_moving_partitions(self, md):
        def move_partitions():
            while not self._stop_move.is_set():
                try:
                    topic, partition = self._random_partition(md)
                    self.logger.info(
                        f"selected partition: {topic}/{partition}")
                    self._do_move_and_verify(topic, partition, 360)
                    # connection errors are expected as we restart nodes for upgrade
                except requests.exceptions.ConnectionError as e:
                    self.redpanda.logger.info(f"Error moving partition: {e}")
                    sleep(1)

        self.move_worker = threading.Thread(name='partition-move-worker',
                                            target=move_partitions)
        self.move_worker.daemon = True
        self.move_worker.start()

    def stop_moving_partitions(self):
        self._stop_move.set()

        self.move_worker.join()

    # Allow unsupported version error log entry for older redpanda versions
    #
    # This log entry may be logged by version up to v22.1.x
    unsupported_api_version_log_entry = re.compile(
        "Error\[applying protocol\] .*Unsupported version \d+ for .*")

    @cluster(num_nodes=6,
             log_allow_list=RESTART_LOG_ALLOW_LIST +
             [unsupported_api_version_log_entry])
    def test_basic_upgrade(self):
        topic = TopicSpec(partition_count=16, replication_factor=3)
        self.client().create_topic(topic)

        self._start_producer(topic.name)
        self._start_consumer(topic.name)

        def topics_ready():
            metadata = self.client().describe_topics()
            return len(metadata) > 1

        wait_until(topics_ready, 10, 0.5)
        metadata = self.client().describe_topics()
        self.start_moving_partitions(metadata)

        first_node = self.redpanda.nodes[0]

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        # Upgrade one node to the head version.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.old_version_str in unique_versions, unique_versions

        # Rollback the partial upgrade and ensure we go back to the original
        # state.
        self.installer.install([first_node], self.old_version)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        # Only once we upgrade the rest of the nodes do we converge on the new
        # version.
        self.installer.install([first_node], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str not in unique_versions, unique_versions

        self.stop_moving_partitions()
        self.verify()
