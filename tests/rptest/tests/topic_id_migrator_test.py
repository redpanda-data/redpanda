# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.kcl import RawKCL
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, LoggingConfig
from rptest.services.redpanda_installer import wait_for_num_versions
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


class TopicIDUpgradeTest(RedpandaTest):
    TARGET_VERSION = (25, 2)

    def __init__(self, test_ctx, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=7,
            log_config=LoggingConfig(
                "info",
                logger_levels={
                    "cluster": "trace",
                    "features": "trace",
                },
            ),
            **kwargs,
        )
        self.installer = self.redpanda._installer
        self.kcl = RawKCL(self.redpanda)

    def setUp(self):
        # 25.2.x is when topic ids went live, so start with 25.1.x
        self.old_version = self.redpanda._installer.highest_from_prior_feature_version(
            self.TARGET_VERSION
        )
        self.installer.install(self.redpanda.nodes, self.old_version)
        super(TopicIDUpgradeTest, self).setUp()

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_topic_id_migration(self):
        n_topics = 500

        # There are a large number of controller records created when the topics are created
        self.redpanda.set_expected_controller_records(n_topics * 2 + 1000)

        # Create the topics to be migrated
        topics = [
            {
                "name": f"test_topic_{i}",
                "partition_count": 1,
                "replication_factor": 1,
            }
            for i in range(n_topics)
        ]
        res = self.kcl.create_topics(6, topics=topics)
        self.redpanda.logger.info(f"Response: {res}")
        assert len(res) == n_topics, f"Unexpected response length: {len(res)}"
        for t in res:
            # The request to kcl may timeout within a non-configurable 5s limit, and may be retried.
            # Allow the ErrorCode "The topic has already been created" (36)
            assert t["ErrorCode"] in {0, 36}, f"Failed to create topic: {t}"

        # Update all nodes to newest version
        self.installer.install(self.redpanda.nodes, self.TARGET_VERSION)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        _ = wait_for_num_versions(self.redpanda, 1)

        # Verify that the old topics have been migrated
        def topic_migrated():
            return self.redpanda.search_log_any(
                f"Assigning topic id .* to topic {{kafka/test_topic_{n_topics - 1}}}"
            ) and self.redpanda.search_log_any(
                "Successfully assigned a UUID to all existing topics"
            )

        wait_until_result(
            topic_migrated,
            timeout_sec=30,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="Timeout waiting for topics to be migrated",
        )
