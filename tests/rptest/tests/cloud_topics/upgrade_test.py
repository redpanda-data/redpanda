# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    CLOUD_TOPICS_CONFIG_STR,
    PREV_VERSION_LOG_ALLOW_LIST,
    RESTART_LOG_ALLOW_LIST,
    SISettings,
)
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.redpanda_test import RedpandaTest


class CloudTopicsUpgradeTest(RedpandaTest):
    """
    Verify that cloud topics topic creation is rejected when the cluster
    is not fully upgraded to v26.1.1, and succeeds after the upgrade
    completes.
    """

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        extra_rp_conf = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "enable_cluster_metadata_upload_loop": False,
        }

        super().__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )

        self.installer = self.redpanda._installer
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    # cloud_topics feature is gated at v26.1, so start on v25.3 (pre-feature)
    OLD_VERSION = (25, 3)
    NEW_VERSION = RedpandaInstaller.next_major_version(OLD_VERSION)

    def setUp(self):
        self.installer.install(self.redpanda.nodes, self.OLD_VERSION)
        self.redpanda.start()

    def _try_create_cloud_topic(self, topic_name: str) -> bool:
        """Attempt to create a cloud-mode topic. Returns True on success."""
        try:
            self.rpk.create_topic(
                topic=topic_name,
                partitions=1,
                replicas=3,
                config={
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
                    "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                },
            )
            return True
        except RpkException as e:
            self.logger.info(f"Topic creation failed as expected: {e}")
            return False

    def _cloud_topics_feature_active(self):
        features = self.admin.get_features()
        features_map = {f["name"]: f for f in features["features"]}
        feat = features_map.get("cloud_topics")
        return feat is not None and feat["state"] == "active"

    def _wait_for_feature_active(self, feature_name: str) -> None:
        def check():
            features = self.admin.get_features()
            features_map = {f["name"]: f for f in features["features"]}
            feat = features_map.get(feature_name)
            return feat is not None and feat["state"] == "active"

        wait_until(
            check,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Feature {feature_name} did not become active",
        )

    @cluster(
        num_nodes=3,
        log_allow_list=RESTART_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST,
    )
    def test_cloud_topic_create_rejected_during_upgrade(self):
        """
        Start a cluster on v25.3.x, upgrade all nodes to HEAD, and confirm
        that cloud topic creation is rejected while the cloud_topics feature
        has not yet activated. After the feature activates, topic creation
        should succeed.
        """
        initial_version = self.admin.get_features()["cluster_version"]
        self.logger.info(f"Starting upgrade test: initial_version={initial_version}")

        # Upgrade all nodes to HEAD one at a time.
        self.installer.install(self.redpanda.nodes, self.NEW_VERSION)
        for node in self.redpanda.nodes:
            self.redpanda.restart_nodes([node])

        # All nodes are now on HEAD. The cloud_topics feature should not be
        # active immediately — it requires the cluster version to bump via
        # health monitor ticks first.
        if not self._cloud_topics_feature_active():
            self.logger.info(
                "cloud_topics feature not yet active, confirming topic "
                "creation is rejected"
            )
            assert not self._try_create_cloud_topic("should-fail-pre-activation"), (
                "Cloud topic creation should fail before cloud_topics feature is active"
            )
        else:
            self.logger.info(
                "cloud_topics feature already active (race), skipping "
                "pre-activation check"
            )

        # Wait for the cloud_topics feature to become active.
        self._wait_for_feature_active("cloud_topics")

        # The cloud_topics_enabled config was set in extra_rp_conf, but
        # v25.3.x doesn't know this property so it was never propagated to
        # the cluster config store. Explicitly set it now that all nodes
        # are on a version that recognizes the property. This is a
        # restart-required property, so restart after setting it.
        self.redpanda.set_cluster_config(
            {CLOUD_TOPICS_CONFIG_STR: True}, expect_restart=True
        )
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Now cloud topic creation should succeed.
        assert self._try_create_cloud_topic("should-succeed-after-upgrade"), (
            "Cloud topic creation should succeed after full upgrade"
        )

        # Verify the topic has the expected storage mode.
        topic_configs = self.rpk.describe_topic_configs("should-succeed-after-upgrade")
        storage_mode = topic_configs.get("redpanda.storage.mode", None)
        assert storage_mode is not None, "storage mode config not found"
        assert storage_mode[0] == "cloud", (
            f"Expected storage mode 'cloud', got '{storage_mode[0]}'"
        )
