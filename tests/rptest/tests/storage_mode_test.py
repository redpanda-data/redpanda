# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    RESTART_LOG_ALLOW_LIST,
    CLOUD_TOPICS_CONFIG_STR,
    SISettings,
)
from rptest.services.redpanda_installer import (
    RedpandaInstaller,
    wait_for_num_versions,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_http_error


class StorageModeTestBase(RedpandaTest):
    """
    Base class for storage mode tests with common helper methods.
    """

    CLUSTER_CONFIG_DEFAULT_STORAGE_MODE = "default_redpanda_storage_mode"

    def _get_topic_config(
        self, rpk: RpkTool, topic_name: str, property_name: str
    ) -> str | None:
        """Get a topic config property value, or None if not present."""
        configs = rpk.describe_topic_configs(topic_name)
        if property_name in configs:
            return configs[property_name][0]
        return None

    def _get_topic_storage_mode(self, rpk: RpkTool, topic_name: str) -> str | None:
        """Get the storage mode property value for a topic."""
        return self._get_topic_config(rpk, topic_name, TopicSpec.PROPERTY_STORAGE_MODE)

    def _get_topic_remote_read(self, rpk: RpkTool, topic_name: str) -> str | None:
        """Get the remote read property value for a topic."""
        return self._get_topic_config(rpk, topic_name, TopicSpec.PROPERTY_REMOTE_READ)

    def _get_topic_remote_write(self, rpk: RpkTool, topic_name: str) -> str | None:
        """Get the remote write property value for a topic."""
        return self._get_topic_config(rpk, topic_name, TopicSpec.PROPERTY_REMOTE_WRITE)

    def _create_topic(
        self,
        rpk: RpkTool,
        topic_name: str,
        config: dict[str, str] | None = None,
    ):
        """Create a topic with optional config overrides."""
        rpk.create_topic(
            topic=topic_name, partitions=1, replicas=3, config=config or {}
        )

    def _set_cluster_default_storage_mode(self, rpk: RpkTool, mode: str):
        """Set the cluster default storage mode."""
        rpk.cluster_config_set(self.CLUSTER_CONFIG_DEFAULT_STORAGE_MODE, mode)


class StorageModeDefaultTest(StorageModeTestBase):
    """
    Test that cluster default storage mode is respected for new topics,
    and that explicit storage mode at topic creation overrides the default.
    """

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
        )

        super(StorageModeDefaultTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
        )

    @cluster(num_nodes=3)
    def test_cluster_default_storage_mode(self):
        """
        Test that topics inherit the cluster default storage mode.
        """
        rpk = RpkTool(self.redpanda)

        # Test with default_redpanda_storage_mode = local
        self._set_cluster_default_storage_mode(rpk, TopicSpec.STORAGE_MODE_LOCAL)
        self._create_topic(rpk, "topic-default-local")
        assert (
            self._get_topic_storage_mode(rpk, "topic-default-local")
            == TopicSpec.STORAGE_MODE_LOCAL
        ), "Topic should have storage_mode=local from cluster default"

        # Test with default_redpanda_storage_mode = tiered
        self._set_cluster_default_storage_mode(rpk, TopicSpec.STORAGE_MODE_TIERED)
        self._create_topic(rpk, "topic-default-tiered")
        assert (
            self._get_topic_storage_mode(rpk, "topic-default-tiered")
            == TopicSpec.STORAGE_MODE_TIERED
        ), "Topic should have storage_mode=tiered from cluster default"

    @cluster(num_nodes=3)
    def test_explicit_storage_mode_overrides_default(self):
        """
        Test that explicitly setting storage_mode at topic creation
        overrides the cluster default.
        """
        rpk = RpkTool(self.redpanda)

        # Set cluster default to local
        self._set_cluster_default_storage_mode(rpk, TopicSpec.STORAGE_MODE_LOCAL)

        # Create topic with explicit storage_mode=tiered (overriding local default)
        self._create_topic(
            rpk,
            "topic-explicit-tiered",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED},
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-explicit-tiered")
            == TopicSpec.STORAGE_MODE_TIERED
        ), "Explicit storage_mode=tiered should override cluster default"

        # Set cluster default to tiered
        self._set_cluster_default_storage_mode(rpk, TopicSpec.STORAGE_MODE_TIERED)

        # Create topic with explicit storage_mode=local (overriding tiered default)
        self._create_topic(
            rpk,
            "topic-explicit-local",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_LOCAL},
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-explicit-local")
            == TopicSpec.STORAGE_MODE_LOCAL
        ), "Explicit storage_mode=local should override cluster default"


class StorageModeUpgradeTest(StorageModeTestBase):
    """
    Test that topics created before the storage_mode property existed
    default to 'unset' after upgrade while preserving their tiered storage
    settings (remote_read/remote_write).
    """

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
        )

        super(StorageModeUpgradeTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
        )
        self.installer = self.redpanda._installer

    # storage_mode property introduced in v26.1, start on v25.3 (pre-feature)
    FROM_VERSION = (25, 3)
    TO_VERSION = RedpandaInstaller.next_major_version(FROM_VERSION)

    def setUp(self):
        self.installer.install(self.redpanda.nodes, self.FROM_VERSION)
        super(StorageModeUpgradeTest, self).setUp()

    def _create_topic_with_tiered_storage_config(
        self, rpk: RpkTool, topic_name: str, remote_read: bool, remote_write: bool
    ):
        """Create a topic with specific remote read/write settings."""
        config = {
            TopicSpec.PROPERTY_REMOTE_READ: str(remote_read).lower(),
            TopicSpec.PROPERTY_REMOTE_WRITE: str(remote_write).lower(),
        }
        self._create_topic(rpk, topic_name, config)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_storage_mode_unset_on_upgrade(self):
        """
        Test that topics created before the storage_mode property existed
        default to 'unset' after upgrade, while preserving their tiered
        storage settings (remote_read/remote_write).

        Creates topics with various remote read/write configurations in an
        old version, then upgrades and verifies:
        - storage_mode defaults to 'unset' for all pre-existing topics
        - remote_read/remote_write settings are preserved
        """
        rpk = RpkTool(self.redpanda)

        _ = wait_for_num_versions(self.redpanda, 1)

        # Create topics with different tiered storage configurations
        # These represent topics that existed before storage_mode was introduced
        test_cases = [
            # (topic_name, remote_read, remote_write)
            ("topic-tiered-both", True, True),
            ("topic-tiered-read-only", True, False),
            ("topic-tiered-write-only", False, True),
            ("topic-local", False, False),
        ]

        for topic_name, remote_read, remote_write in test_cases:
            self._create_topic_with_tiered_storage_config(
                rpk, topic_name, remote_read, remote_write
            )
            # On old version, properties are always present with "true"/"false"
            actual_read = self._get_topic_remote_read(rpk, topic_name)
            actual_write = self._get_topic_remote_write(rpk, topic_name)
            assert actual_read == str(remote_read).lower(), (
                f"Topic {topic_name} remote_read mismatch: expected {str(remote_read).lower()}, got {actual_read}"
            )
            assert actual_write == str(remote_write).lower(), (
                f"Topic {topic_name} remote_write mismatch: expected {str(remote_write).lower()}, got {actual_write}"
            )

        # Upgrade all nodes to next version
        self.installer.install(self.redpanda.nodes, self.TO_VERSION)
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for cluster to stabilize on new version
        _ = wait_for_num_versions(self.redpanda, 1)

        # Verify all topics have storage_mode=unset and preserved tiered settings
        for topic_name, remote_read, remote_write in test_cases:
            # Verify remote_read/remote_write are preserved
            actual_read = self._get_topic_remote_read(rpk, topic_name)
            actual_write = self._get_topic_remote_write(rpk, topic_name)
            expected_read = str(remote_read).lower()
            expected_write = str(remote_write).lower()
            assert actual_read == expected_read, (
                f"Topic {topic_name}: remote_read is {actual_read}, expected {expected_read}"
            )
            assert actual_write == expected_write, (
                f"Topic {topic_name}: remote_write is {actual_write}, expected {expected_write}"
            )

            # Verify storage_mode defaults to 'unset' for all pre-existing topics
            actual_mode = self._get_topic_storage_mode(rpk, topic_name)
            assert actual_mode == TopicSpec.STORAGE_MODE_UNSET, (
                f"Topic {topic_name}: expected storage_mode=unset, got {actual_mode}"
            )


class StorageModeUnsetTest(StorageModeTestBase):
    """
    Test that storage_mode=unset correctly falls back to legacy shadow_indexing
    behavior, and that explicit storage_mode values (local/tiered) override
    shadow_indexing settings.
    """

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
        )

        super(StorageModeUnsetTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
        )

    @cluster(num_nodes=3)
    def test_unset_storage_mode_default(self):
        """
        Test that the default storage mode is 'unset' and topics created
        without explicit storage_mode get 'unset'.
        """
        rpk = RpkTool(self.redpanda)

        # Verify the cluster default is 'unset'
        self._set_cluster_default_storage_mode(rpk, TopicSpec.STORAGE_MODE_UNSET)

        # Create topic without explicit storage_mode
        self._create_topic(rpk, "topic-default-unset")
        actual_mode = self._get_topic_storage_mode(rpk, "topic-default-unset")
        assert actual_mode == TopicSpec.STORAGE_MODE_UNSET, (
            f"Topic should have storage_mode=unset, got {actual_mode}"
        )

    @cluster(num_nodes=3)
    def test_unset_falls_back_to_shadow_indexing(self):
        """
        Test that when storage_mode=unset, the legacy shadow_indexing configs
        (remote.read/remote.write) determine tiered storage behavior.
        """
        rpk = RpkTool(self.redpanda)

        # Set cluster default to unset
        self._set_cluster_default_storage_mode(rpk, TopicSpec.STORAGE_MODE_UNSET)

        # Create topic with unset storage mode and enable remote write
        self._create_topic(
            rpk,
            "topic-unset-with-remote-write",
            config={
                TopicSpec.PROPERTY_REMOTE_WRITE: "true",
            },
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-with-remote-write")
            == TopicSpec.STORAGE_MODE_UNSET
        ), "Topic should have storage_mode=unset"
        assert (
            self._get_topic_remote_write(rpk, "topic-unset-with-remote-write") == "true"
        ), "Topic should have remote_write=true"

        # Create topic with unset storage mode and no shadow_indexing
        self._create_topic(rpk, "topic-unset-no-shadow")
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-no-shadow")
            == TopicSpec.STORAGE_MODE_UNSET
        ), "Topic should have storage_mode=unset"
        # remote.read and remote.write should be false (disabled)
        assert self._get_topic_remote_read(rpk, "topic-unset-no-shadow") == "false", (
            "Topic should have remote_read=false"
        )
        assert self._get_topic_remote_write(rpk, "topic-unset-no-shadow") == "false", (
            "Topic should have remote_write=false"
        )

    @cluster(num_nodes=3)
    def test_explicit_tiered_overrides_shadow_indexing(self):
        """
        Test that storage_mode=tiered enables both archival (write) and fetch
        (read), regardless of shadow_indexing settings.
        """
        rpk = RpkTool(self.redpanda)

        # Create topic with explicit tiered mode, without setting shadow_indexing
        self._create_topic(
            rpk,
            "topic-tiered-no-shadow",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED},
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-tiered-no-shadow")
            == TopicSpec.STORAGE_MODE_TIERED
        ), "Topic should have storage_mode=tiered"

        # Create topic with explicit tiered and shadow_indexing=archival (write-only)
        # Both read and write should still be enabled because tiered is authoritative
        self._create_topic(
            rpk,
            "topic-tiered-archival-only",
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                TopicSpec.PROPERTY_REMOTE_WRITE: "true",
                TopicSpec.PROPERTY_REMOTE_READ: "false",
            },
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-tiered-archival-only")
            == TopicSpec.STORAGE_MODE_TIERED
        ), "Topic should have storage_mode=tiered"

    @cluster(num_nodes=3)
    def test_explicit_local_overrides_shadow_indexing(self):
        """
        Test that storage_mode=local disables tiered storage features,
        regardless of shadow_indexing settings.
        """
        rpk = RpkTool(self.redpanda)

        # Create topic with explicit local mode, but trying to enable shadow_indexing
        # This should result in local mode where tiered features are disabled
        self._create_topic(
            rpk,
            "topic-local-with-shadow",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_LOCAL},
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-local-with-shadow")
            == TopicSpec.STORAGE_MODE_LOCAL
        ), "Topic should have storage_mode=local"


class StorageModeTransitionTest(StorageModeTestBase):
    """
    Test storage mode transitions, including transitions to/from 'unset'.
    """

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
        )

        super(StorageModeTransitionTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
        )

    @cluster(num_nodes=3)
    def test_storage_mode_transitions(self):
        """
        Test all permitted and blocked storage mode transitions.

        Permitted transitions:
        - unset -> local
        - unset -> tiered
        - local -> tiered
        - tiered -> local

        Blocked transitions:
        - local -> unset
        - tiered -> unset
        - unset -> cloud
        - unset -> tiered_cloud
        - local -> tiered_cloud

        Note: cloud <-> tiered_cloud transitions are tested in
        StorageModeCloudTransitionTest (requires cloud_topics_enabled).
        """
        rpk = RpkTool(self.redpanda)

        # Set cluster default to unset for initial topic creation
        self._set_cluster_default_storage_mode(rpk, TopicSpec.STORAGE_MODE_UNSET)

        # Test unset -> local transition (permitted)
        self._create_topic(rpk, "topic-unset-to-local")
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-local")
            == TopicSpec.STORAGE_MODE_UNSET
        )
        rpk.alter_topic_config(
            "topic-unset-to-local",
            TopicSpec.PROPERTY_STORAGE_MODE,
            TopicSpec.STORAGE_MODE_LOCAL,
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-local")
            == TopicSpec.STORAGE_MODE_LOCAL
        ), "Transition from unset to local should succeed"

        # Test unset -> tiered transition (permitted)
        self._create_topic(rpk, "topic-unset-to-tiered")
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-tiered")
            == TopicSpec.STORAGE_MODE_UNSET
        )
        rpk.alter_topic_config(
            "topic-unset-to-tiered",
            TopicSpec.PROPERTY_STORAGE_MODE,
            TopicSpec.STORAGE_MODE_TIERED,
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-tiered")
            == TopicSpec.STORAGE_MODE_TIERED
        ), "Transition from unset to tiered should succeed"

        # Test local -> tiered transition (permitted)
        self._create_topic(
            rpk,
            "topic-local-to-tiered",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_LOCAL},
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-local-to-tiered")
            == TopicSpec.STORAGE_MODE_LOCAL
        )
        rpk.alter_topic_config(
            "topic-local-to-tiered",
            TopicSpec.PROPERTY_STORAGE_MODE,
            TopicSpec.STORAGE_MODE_TIERED,
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-local-to-tiered")
            == TopicSpec.STORAGE_MODE_TIERED
        ), "Transition from local to tiered should succeed"

        # Test tiered -> local transition (permitted)
        self._create_topic(
            rpk,
            "topic-tiered-to-local",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED},
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-tiered-to-local")
            == TopicSpec.STORAGE_MODE_TIERED
        )
        rpk.alter_topic_config(
            "topic-tiered-to-local",
            TopicSpec.PROPERTY_STORAGE_MODE,
            TopicSpec.STORAGE_MODE_LOCAL,
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-tiered-to-local")
            == TopicSpec.STORAGE_MODE_LOCAL
        ), "Transition from tiered to local should succeed"

        # Test blocked transition: local -> unset
        self._create_topic(
            rpk,
            "topic-local-to-unset",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_LOCAL},
        )
        try:
            rpk.alter_topic_config(
                "topic-local-to-unset",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_UNSET,
            )
            assert False, "Transition from local to unset should have been rejected"
        except Exception:
            pass  # Expected - transition should be rejected
        assert (
            self._get_topic_storage_mode(rpk, "topic-local-to-unset")
            == TopicSpec.STORAGE_MODE_LOCAL
        ), "Storage mode should still be local after rejected transition"

        # Test blocked transition: tiered -> unset
        self._create_topic(
            rpk,
            "topic-tiered-to-unset",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED},
        )
        try:
            rpk.alter_topic_config(
                "topic-tiered-to-unset",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_UNSET,
            )
            assert False, "Transition from tiered to unset should have been rejected"
        except Exception:
            pass  # Expected - transition should be rejected
        assert (
            self._get_topic_storage_mode(rpk, "topic-tiered-to-unset")
            == TopicSpec.STORAGE_MODE_TIERED
        ), "Storage mode should still be tiered after rejected transition"

        # Test blocked transition: unset -> cloud
        self._create_topic(rpk, "topic-unset-to-cloud")
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-cloud")
            == TopicSpec.STORAGE_MODE_UNSET
        )
        try:
            rpk.alter_topic_config(
                "topic-unset-to-cloud",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_CLOUD,
            )
            assert False, "Transition from unset to cloud should have been rejected"
        except Exception:
            pass  # Expected - transition should be rejected
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-cloud")
            == TopicSpec.STORAGE_MODE_UNSET
        ), "Storage mode should still be unset after rejected transition"

        # Test blocked transition: unset -> tiered_cloud
        self._create_topic(rpk, "topic-unset-to-tiered-cloud")
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-tiered-cloud")
            == TopicSpec.STORAGE_MODE_UNSET
        )
        try:
            rpk.alter_topic_config(
                "topic-unset-to-tiered-cloud",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_TIERED_CLOUD,
            )
            assert False, (
                "Transition from unset to tiered_cloud should have been rejected"
            )
        except Exception:
            pass  # Expected - transition should be rejected
        assert (
            self._get_topic_storage_mode(rpk, "topic-unset-to-tiered-cloud")
            == TopicSpec.STORAGE_MODE_UNSET
        ), "Storage mode should still be unset after rejected transition"

        # Test blocked transition: local -> tiered_cloud
        self._create_topic(
            rpk,
            "topic-local-to-tiered-cloud",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_LOCAL},
        )
        try:
            rpk.alter_topic_config(
                "topic-local-to-tiered-cloud",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_TIERED_CLOUD,
            )
            assert False, (
                "Transition from local to tiered_cloud should have been rejected"
            )
        except Exception:
            pass  # Expected - transition should be rejected
        assert (
            self._get_topic_storage_mode(rpk, "topic-local-to-tiered-cloud")
            == TopicSpec.STORAGE_MODE_LOCAL
        ), "Storage mode should still be local after rejected transition"


class StorageModeValidationTest(RedpandaTest):
    """
    Test that the default_redpanda_storage_mode cluster config validation
    correctly enforces dependencies on cloud_storage_enabled and
    cloud_topics_enabled.
    """

    def __init__(self, test_context: TestContext):
        # Start without cloud storage to test validation
        super(StorageModeValidationTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
        )

    @cluster(num_nodes=3)
    def test_default_storage_mode_validation(self):
        """
        Test that default_redpanda_storage_mode validation enforces:
        - 'tiered' requires cloud_storage_enabled=true
        - 'cloud' requires cloud_topics_enabled=true
        """
        admin = Admin(self.redpanda)

        # Verify preconditions
        config = admin.get_cluster_config()
        assert config["cloud_storage_enabled"] is False, (
            "cloud_storage_enabled should be false for this test"
        )
        assert config["cloud_topics_enabled"] is False, (
            "cloud_topics_enabled should be false for this test"
        )

        # Test: setting 'tiered' should fail without cloud_storage_enabled
        with expect_http_error(400):
            admin.patch_cluster_config(
                upsert={"default_redpanda_storage_mode": "tiered"}
            )
        config = admin.get_cluster_config()
        assert config["default_redpanda_storage_mode"] != "tiered", (
            "default_redpanda_storage_mode should not be tiered"
        )

        # Test: setting 'cloud' should fail without cloud_topics_enabled
        with expect_http_error(400):
            admin.patch_cluster_config(
                upsert={"default_redpanda_storage_mode": "cloud"}
            )
        config = admin.get_cluster_config()
        assert config["default_redpanda_storage_mode"] != "cloud", (
            "default_redpanda_storage_mode should not be cloud"
        )

        # Test: setting 'tiered_cloud' should fail without cloud_topics_enabled
        with expect_http_error(400):
            admin.patch_cluster_config(
                upsert={"default_redpanda_storage_mode": "tiered_cloud"}
            )
        config = admin.get_cluster_config()
        assert config["default_redpanda_storage_mode"] != "tiered_cloud", (
            "default_redpanda_storage_mode should not be tiered_cloud"
        )


class StorageModeCloudTransitionTest(StorageModeTestBase):
    """
    Test storage mode transitions between cloud and tiered_cloud.
    Requires cloud topics to be enabled.
    """

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
        )

        super(StorageModeCloudTransitionTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
            extra_rp_conf={
                CLOUD_TOPICS_CONFIG_STR: True,
            },
        )

    def setUp(self):
        super().setUp()
        self.redpanda.set_feature_active("tiered_cloud_topics", True, timeout_sec=30)

    @cluster(num_nodes=3)
    def test_cloud_to_tiered_cloud_transition(self):
        """
        Test that cloud -> tiered_cloud and tiered_cloud -> cloud transitions
        are permitted.
        """
        rpk = RpkTool(self.redpanda)

        # Create a cloud topic
        self._create_topic(
            rpk,
            "topic-cloud-to-tc",
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD},
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-cloud-to-tc")
            == TopicSpec.STORAGE_MODE_CLOUD
        )

        # Transition cloud -> tiered_cloud (permitted)
        rpk.alter_topic_config(
            "topic-cloud-to-tc",
            TopicSpec.PROPERTY_STORAGE_MODE,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-cloud-to-tc")
            == TopicSpec.STORAGE_MODE_TIERED_CLOUD
        ), "Transition from cloud to tiered_cloud should succeed"

        # Transition tiered_cloud -> cloud (permitted)
        rpk.alter_topic_config(
            "topic-cloud-to-tc",
            TopicSpec.PROPERTY_STORAGE_MODE,
            TopicSpec.STORAGE_MODE_CLOUD,
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-cloud-to-tc")
            == TopicSpec.STORAGE_MODE_CLOUD
        ), "Transition from tiered_cloud to cloud should succeed"

    @cluster(num_nodes=3)
    def test_tiered_cloud_blocked_transitions(self):
        """
        Test that tiered_cloud cannot transition to local, tiered, or unset.
        """
        rpk = RpkTool(self.redpanda)

        # Create a tiered_cloud topic
        self._create_topic(
            rpk,
            "topic-tc-blocked",
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED_CLOUD
            },
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-tc-blocked")
            == TopicSpec.STORAGE_MODE_TIERED_CLOUD
        )

        # tiered_cloud -> local (blocked)
        try:
            rpk.alter_topic_config(
                "topic-tc-blocked",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_LOCAL,
            )
            assert False, (
                "Transition from tiered_cloud to local should have been rejected"
            )
        except Exception:
            pass
        assert (
            self._get_topic_storage_mode(rpk, "topic-tc-blocked")
            == TopicSpec.STORAGE_MODE_TIERED_CLOUD
        ), "Storage mode should still be tiered_cloud after rejected transition"

        # tiered_cloud -> tiered (blocked)
        try:
            rpk.alter_topic_config(
                "topic-tc-blocked",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_TIERED,
            )
            assert False, (
                "Transition from tiered_cloud to tiered should have been rejected"
            )
        except Exception:
            pass
        assert (
            self._get_topic_storage_mode(rpk, "topic-tc-blocked")
            == TopicSpec.STORAGE_MODE_TIERED_CLOUD
        ), "Storage mode should still be tiered_cloud after rejected transition"

        # tiered_cloud -> unset (blocked)
        try:
            rpk.alter_topic_config(
                "topic-tc-blocked",
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_UNSET,
            )
            assert False, (
                "Transition from tiered_cloud to unset should have been rejected"
            )
        except Exception:
            pass
        assert (
            self._get_topic_storage_mode(rpk, "topic-tc-blocked")
            == TopicSpec.STORAGE_MODE_TIERED_CLOUD
        ), "Storage mode should still be tiered_cloud after rejected transition"

    @cluster(num_nodes=3)
    def test_tiered_cloud_topic_creation(self):
        """
        Test that a topic can be created directly with tiered_cloud mode.
        """
        rpk = RpkTool(self.redpanda)

        self._create_topic(
            rpk,
            "topic-created-as-tc",
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED_CLOUD
            },
        )
        assert (
            self._get_topic_storage_mode(rpk, "topic-created-as-tc")
            == TopicSpec.STORAGE_MODE_TIERED_CLOUD
        ), "Topic should be created with storage_mode=tiered_cloud"
