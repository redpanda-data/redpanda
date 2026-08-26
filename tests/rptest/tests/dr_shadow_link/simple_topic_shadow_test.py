# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase


class SimpleTopicShadowTest(ShadowLinkTestBase):
    """
    Simple test demonstrating basic shadow-link topic replication between two clusters.
    Verifies that topics created on source cluster are replicated to target cluster.
    """

    @cluster(num_nodes=8)  # 6 for clusters + 2 for workload
    def test_basic_shadow_link_setup(self):
        """Test basic shadow-link setup between two clusters."""

        # The base class already sets up two clusters:
        # - self.source_cluster
        # - self.target_cluster

        # Verify source cluster
        source_nodes = self.source_cluster.service.started_nodes()
        self.logger.info(f"Source cluster has {len(source_nodes)} nodes")

        # Verify target cluster
        target_nodes = self.target_cluster.service.started_nodes()
        self.logger.info(f"Target cluster has {len(target_nodes)} nodes")

        # Create topic on source
        topic_name = "dr-test-topic"
        topic_spec = TopicSpec(name=topic_name, partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic_spec)

        # Wait for topic creation
        wait_until(
            lambda: self.topic_exists_in_source(topic_name),
            timeout_sec=10,
            err_msg=f"Topic {topic_name} not created on source",
        )

        self.logger.info(f"Created topic '{topic_name}' on source cluster")

        # Create shadow link
        self.create_link("test-dr-link")

        # Wait for topic to replicate
        wait_until(
            lambda: self.topic_exists_in_target(topic_name),
            timeout_sec=30,
            err_msg=f"Topic {topic_name} not replicated to target",
        )

        self.logger.info(
            f"Topic '{topic_name}' successfully replicated to target cluster"
        )
