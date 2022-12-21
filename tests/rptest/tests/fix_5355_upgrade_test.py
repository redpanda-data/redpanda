# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.services.redpanda import RedpandaService

from confluent_kafka import (Producer, KafkaException)
from random import choice
from string import ascii_uppercase


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)


class Fix5355UpgradeTest(RedpandaTest):
    topics = [TopicSpec(name="topic1")]
    """
    Basic test that upgrading software works as expected.
    """
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "log_segment_size": 1048576
        }
        super(Fix5355UpgradeTest, self).__init__(test_context=test_context,
                                                 num_brokers=3,
                                                 extra_rp_conf=extra_rp_conf)
        self.installer = self.redpanda._installer

    def setUp(self):
        # NOTE: `rpk redpanda admin brokers list` requires versions v22.1.x and
        # above.
        self.oldversion, self.oldversion_str = self.installer.install(
            self.redpanda.nodes, (22, 1))
        super(Fix5355UpgradeTest, self).setUp()

    def fill_segment(self):
        payload_1kb = ''.join(choice(ascii_uppercase) for i in range(1024))
        p = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5
        })
        for i in range(0, 2 * 1024):
            p.produce("topic1",
                      key="key1".encode('utf-8'),
                      value=payload_1kb.encode('utf-8'),
                      callback=on_delivery)
        p.flush()

    def check_snapshot_exist(self):
        for node in self.redpanda.nodes:
            cmd = f"find {RedpandaService.DATA_DIR}"
            out_iter = node.account.ssh_capture(cmd)
            has_snapshot = False
            for line in out_iter:
                has_snapshot = has_snapshot or re.match(
                    f"{RedpandaService.DATA_DIR}/kafka/topic1/\\d+_\\d+/tx.snapshot",
                    line)
            assert has_snapshot

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rollback(self):
        """
        the test checks than a mid upgrade rollback isn't broken
        """
        first_node = self.redpanda.nodes[0]

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.oldversion_str in unique_versions, unique_versions

        # Upgrade one node to the head version.
        self.installer.install([first_node], (22, 2))
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.oldversion_str in unique_versions, unique_versions

        self.fill_segment()
        self.check_snapshot_exist()

        # Rollback the partial upgrade and ensure we go back to the original
        # state.
        self.installer.install([first_node], self.oldversion)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.oldversion_str in unique_versions, unique_versions

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        """
        the test checks than upgrade isn't broken
        """
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.oldversion_str in unique_versions, unique_versions

        self.fill_segment()
        self.check_snapshot_exist()

        # Upgrade one node to the head version.
        self.installer.install(self.redpanda.nodes, (22, 2))
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.oldversion_str not in unique_versions, unique_versions
