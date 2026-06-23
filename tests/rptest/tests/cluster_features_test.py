# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import time

from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.errors import TimeoutError as DucktapeTimeoutError
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.clients.admin.proto.redpanda.core.admin.v2 import features_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception, wait_until_result
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.utils.rpenv import sample_license, sample_license_v1

FEATURE_ALPHA_NAME = "__test_alpha"
FEATURE_BRAVO_NAME = "__test_bravo"
FEATURE_CHARLIE_NAME = "__test_charlie"
TEST_FEATURES_VERSION = 2001


class FeaturesTestBase(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer
        self.head_latest_logical_version = None
        self.head_earliest = None

    def setUp(self):
        super().setUp()
        features = self.admin.get_features()
        self.head_latest_logical_version = features["node_latest_version"]
        self.head_earliest_logical_version = features["node_earliest_version"]

        # Sanity check that the cluster is at a clean initial state where
        # the original == latest == active
        self.logger.info(f"Initial feature state: {json.dumps(features, indent=2)}")
        assert features["node_latest_version"] >= 1
        assert features["node_earliest_version"] >= 1
        assert features["node_latest_version"] == features["original_cluster_version"]
        assert features["node_latest_version"] == features["cluster_version"]
        assert features["node_earliest_version"] <= features["node_latest_version"]

        self.previous_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )

    """
    Test cases defined in this parent class are executed as part
    of subclasses that define node count below.
    """

    def _get_features_map(self, feature_response=None, node=None):
        if feature_response is None:
            feature_response = self.admin.get_features(node=node)
        return dict((f["name"], f) for f in feature_response["features"])

    def _assert_default_features(self):
        """
        Verify that the config GET endpoint serves valid json with
        the expected features and version.
        """

        features_response = self.admin.get_features()
        self.logger.info(f"Features response: {features_response}")

        # This assertion will break each time we increment the value
        # of `latest_version` in the redpanda source.  Update it when
        # that happens.
        initial_version = features_response["cluster_version"]
        assert initial_version == self.head_latest_logical_version, (
            f"Version mismatch: {initial_version} vs {self.head_latest_logical_version}"
        )

        assert self._get_features_map(features_response)["license"]["state"] == "active"

        return features_response

    def _wait_for_version_everywhere(self, target_version):
        """
        Apply a GET check to all nodes, for writes that are expected to
        propagate via controller log
        """

        def check():
            for node in self.redpanda.nodes:
                node_version = self.admin.get_features(node=node)["cluster_version"]
                if node_version != target_version:
                    return False

            return True

        # Version propagation is a little slower than feature write propagation, because
        # it relies on periodic health messages
        wait_until(check, timeout_sec=20, backoff_sec=1)

    def _wait_for_feature_everywhere(self, fn):
        """
        Apply a GET check to all nodes, for writes that are expected to
        propagate via controller log
        """

        def check():
            for node in self.redpanda.nodes:
                feature_map = self._get_features_map(node=node)
                if not fn(feature_map):
                    return False

            return True

        # Controller writes usually propagate in milliseconds, so this is not
        # a particularly long timeout: it's here for when tests run very slow.
        wait_until(check, timeout_sec=10, backoff_sec=0.5)


class FeaturesMultiNodeTest(FeaturesTestBase):
    """
    Multi-node variant of tests is the 'normal' execution path for feature manager.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @cluster(num_nodes=3)
    def test_get_features(self):
        self._assert_default_features()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_explicit_activation(self):
        """
        Using a dummy feature, verify its progression through unavailable->available->active
        """

        # Parameters of the compiled-in test feature
        feature_alpha_version = 2001

        initial_version = self.admin.get_features()["cluster_version"]
        assert initial_version < feature_alpha_version
        # Initially, before setting the magic environment variable, dummy test features
        # should be hidden
        assert FEATURE_ALPHA_NAME not in self._get_features_map().keys()

        self.redpanda.set_environment({"__REDPANDA_TEST_FEATURES": "ON"})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        assert self._get_features_map()[FEATURE_ALPHA_NAME]["state"] == "unavailable"

        # Version is too low, feature should be unavailable
        assert initial_version == self.admin.get_features()["cluster_version"]

        self.redpanda.set_environment(
            {
                "__REDPANDA_TEST_FEATURES": "ON",
                "__REDPANDA_EARLIEST_LOGICAL_VERSION": f"{self.head_latest_logical_version}",
                "__REDPANDA_LATEST_LOGICAL_VERSION": f"{feature_alpha_version}",
            }
        )
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for version to increment: this is a little slow because we wait
        # for health monitor structures to time out in order to propagate the
        # updated version
        self._wait_for_version_everywhere(feature_alpha_version)

        # Feature should become available now that version increased.  It should NOT
        # become active, because it has an explicit_only policy for activation.
        self._wait_for_feature_everywhere(
            lambda fm: fm[FEATURE_ALPHA_NAME]["state"] == "available"
        )

        # Disable the feature, see that it enters the expected state
        self.admin.put_feature(FEATURE_ALPHA_NAME, {"state": "disabled"})
        self._wait_for_feature_everywhere(
            lambda fm: fm[FEATURE_ALPHA_NAME]["state"] == "disabled"
        )

        state = self._get_features_map()[FEATURE_ALPHA_NAME]
        assert state["state"] == "disabled"
        assert state["was_active"] == False

        # Write to admin API to enable the feature
        self.admin.put_feature(FEATURE_ALPHA_NAME, {"state": "active"})

        # This is an async check because propagation of feature_table is async
        self._wait_for_feature_everywhere(
            lambda fm: fm[FEATURE_ALPHA_NAME]["state"] == "active"
        )

        # Disable the feature, see that it enters the expected state
        self.admin.put_feature(FEATURE_ALPHA_NAME, {"state": "disabled"})
        self._wait_for_feature_everywhere(
            lambda fm: fm[FEATURE_ALPHA_NAME]["state"] == "disabled"
        )

        state = self._get_features_map()[FEATURE_ALPHA_NAME]
        assert state["state"] == "disabled"
        assert state["was_active"] == True

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_license_upload_and_query(self):
        """
        Test uploading and retrieval of license
        """
        license = sample_license()
        if license is None:
            self.logger.info("Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return
        expected_license_contents = {
            "expires": 4813252273,
            "format_version": 0,
            "org": "redpanda-testing",
            "type": "enterprise",
            "sha256": "2730125070a934ca1067ed073d7159acc9975dc61015892308aae186f7455daf",
        }

        assert self.admin.put_license(license).status_code == 200

        def check_license(is_matching_license):
            # Wait for all of the nodes to see the next license to ensure that
            # we don't get idempotent 200's when updating the license on a
            # stale node in the face of leadership changes
            licenses = [
                self.admin.get_license(node=node)
                for node in self.redpanda.started_nodes()
            ]
            if any(not is_matching_license(lic) for lic in licenses):
                return False, None
            return (True, licenses[0])

        resp = wait_until_result(
            lambda: check_license(self.admin.is_sample_license),
            timeout_sec=20,
            backoff_sec=1,
        )
        assert resp["license"] is not None
        assert expected_license_contents == resp["license"], resp["license"]

        license_v1 = sample_license_v1()
        if license_v1 is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE_V1_PRODUCTS env var not found"
            )
            return
        expected_license_contents_v1 = {
            "format_version": 1,
            "org": "redpanda-testing",
            "type": "testing_license",
            "products": ["some_prod", "some_other_prod"],
            "expires": 4344165449,
            "sha256": "0937a2d8e4437a63373c1c1cb0f5f62c5cae9366fea1b00467b4c4eaab8ca4cf",
        }

        assert self.admin.put_license(license_v1).status_code == 200

        def is_v1_license(lic):
            if lic is None or "license" not in lic:
                return False
            return lic["license"]["format_version"] == 1

        resp = wait_until_result(
            lambda: check_license(is_v1_license), timeout_sec=20, backoff_sec=1
        )
        assert resp["license"] is not None
        assert expected_license_contents_v1 == resp["license"], resp["license"]

        # Set back to v0 for sanity check
        assert self.admin.put_license(license).status_code == 200

        resp = wait_until_result(
            lambda: check_license(self.admin.is_sample_license),
            timeout_sec=20,
            backoff_sec=1,
        )
        assert resp["license"] is not None
        assert expected_license_contents == resp["license"], resp["license"]


class FeaturesMultiNodeUpgradeTest(FeaturesTestBase):
    """
    Multi-node variant of tests that exercise upgrades from older versions.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    def setUp(self):
        super().setUp()
        # setup an old version as start condition
        self.installer.install(self.redpanda.nodes, self.previous_version)
        self.redpanda.start()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        """
        Verify that on updating to a new logical version, the cluster
        version does not increment until all nodes are up to date.
        """
        initial_version = self.admin.get_features()["cluster_version"]
        assert initial_version < self.head_latest_logical_version, (
            f"downgraded logical version {initial_version}"
        )

        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        _ = wait_for_num_versions(self.redpanda, 2)
        assert initial_version == self.admin.get_features()["cluster_version"]

        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        # Even after waiting a bit, the logical version shouldn't change.
        time.sleep(5)
        assert initial_version == self.admin.get_features()["cluster_version"]

        self.redpanda.restart_nodes([self.redpanda.nodes[2]])

        # Node logical versions are transmitted as part of health messages, so we may
        # have to wait for the next health tick (health_monitor_tick_interval=10s) before
        # the controller leader fetches health from the last restarted peer.
        self._wait_for_version_everywhere(self.head_latest_logical_version)

        # Check that initial version and current version are properly reflected
        # across all nodes.
        def complete():
            for n in self.redpanda.nodes:
                features = self.admin.get_features(node=n)
                if (
                    features["cluster_version"] != self.head_latest_logical_version
                    or features["original_cluster_version"] != initial_version
                ):
                    return False
            return True

        wait_until(complete, timeout_sec=5, backoff_sec=1)

        # Check that initial version is properly remembered past restarts
        self.redpanda.restart_nodes(self.redpanda.nodes)
        assert complete()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rollback(self):
        """
        Verify that on a rollback before updating all nodes, the cluster
        version does not increment.
        """
        initial_version = self.admin.get_features()["cluster_version"]
        assert initial_version < self.head_latest_logical_version, (
            f"downgraded logical version {initial_version}"
        )

        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        _ = wait_for_num_versions(self.redpanda, 2)
        # Even after waiting a bit, the logical version shouldn't change.
        time.sleep(5)
        assert initial_version == self.admin.get_features()["cluster_version"]

        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        time.sleep(5)
        assert initial_version == self.admin.get_features()["cluster_version"]

        self.installer.install(self.redpanda.nodes, self.previous_version)
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        _ = wait_for_num_versions(self.redpanda, 1)
        assert initial_version == self.admin.get_features()["cluster_version"]


class FeaturesSingleNodeTest(FeaturesTestBase):
    """
    A single node variant to make sure feature_manager does its job in the absence
    of any health reports.
    """

    def __init__(self, *args, **kwargs):
        # Skip immediate parent constructor
        super().__init__(*args, num_brokers=1, **kwargs)

    @cluster(num_nodes=1)
    def test_get_features(self):
        self._assert_default_features()


class FeaturesSingleNodeUpgradeTest(FeaturesTestBase):
    """
    Single-node variant of tests that exercise upgrades from older versions.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=1, **kwargs)

    def setUp(self):
        super().setUp()
        self.installer.install(self.redpanda.nodes, self.previous_version)
        self.redpanda.start()

    @cluster(num_nodes=1, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        """
        Verify that on updating to a new logical version, the cluster
        version does not increment until all nodes are up to date.
        """
        initial_version = self.admin.get_features()["cluster_version"]
        assert initial_version < self.head_latest_logical_version, (
            f"downgraded logical version {initial_version}"
        )

        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.installer.install([self.redpanda.nodes[0]], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        wait_until(
            lambda: (
                self.head_latest_logical_version
                == self.admin.get_features()["cluster_version"]
            ),
            timeout_sec=10,
            backoff_sec=1,
        )


OLD_NODE_JOIN_LOG_ALLOW_LIST = [
    # We expect startup failure when an old node joins, so we allow the corresponding
    # error message.
    r"Failure during startup: seastar::abort_requested_exception \(abort requested\)"
]


class FeaturesNodeJoinTest(FeaturesTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=4, **kwargs)

    def setUp(self):
        super().setUp()
        # We will start nodes by hand during test.
        self.redpanda.stop()

    @cluster(num_nodes=4, log_allow_list=OLD_NODE_JOIN_LOG_ALLOW_LIST)
    def test_old_node_join(self):
        """
        Verify that when an old-versioned node tries to join a newer-versioned cluster,
        it is rejected, using real redpanda packages of different versions.
        """

        # Pick a node to roleplay an old version of redpanda
        old_node = self.redpanda.nodes[-1]
        old_version = self.installer.highest_from_prior_feature_version(
            self.installer.HEAD
        )
        self.logger.info(f"Selected old version {old_version}")
        self.installer.install([old_node], old_version)

        # Start first three nodes
        self.redpanda.start(self.redpanda.nodes[0:-1])

        # Explicit clean because it's not included in the default
        # one during start()
        self.redpanda.clean_node(old_node, preserve_current_install=True)

        initial_version = self.admin.get_features()["cluster_version"]
        assert initial_version == self.head_latest_logical_version, (
            f"Version mismatch: {initial_version} vs {self.head_latest_logical_version}"
        )

        try:
            self.redpanda.start_node(old_node)
        except DucktapeTimeoutError:
            pass
        else:
            raise RuntimeError(
                f"Node {old_node} joined cluster, but should have been rejected"
            )

        # Restart it with a sufficiently recent version and join should succeed
        self.installer.install([old_node], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([old_node])

        # Timeout long enough for join retries & health monitor tick (registered
        # requires `is_alive`)
        wait_until(
            lambda: self.redpanda.registered(old_node), timeout_sec=30, backoff_sec=1
        )

    def _test_synthetic_versions(self, joiner_earliest_version, joiner_latest_version):
        """
        Verify that when an bad-versioned node tries to join a cluster,
        it is rejected.  Do this using the same physical version, but with a synthetic
        logical version, to check that the rejection is really the result of a logical
        version check, and not some other incompatibility.
        """

        # Pick a node to run with a synthetic old version
        old_node = self.redpanda.nodes[-1]
        self.logger.info(f"Selected node {old_node.name} to be joiner")

        # Start first three nodes
        self.redpanda.start(self.redpanda.nodes[0:-1])

        # Explicit clean because it's not included in the default
        # one during start()
        self.redpanda.clean_node(old_node, preserve_current_install=True)

        initial_version = self.admin.get_features()["cluster_version"]
        assert initial_version == self.head_latest_logical_version, (
            f"Version mismatch: {initial_version} vs {self.head_latest_logical_version}"
        )

        try:
            self.logger.info(
                f"Starting node {old_node.name} with version {joiner_earliest_version}-{joiner_latest_version}"
            )
            # Set the joining node's version to something bad, it should be forbidden
            # to join the cluster.
            self.redpanda.set_environment(
                {"__REDPANDA_LATEST_LOGICAL_VERSION": joiner_latest_version}
            )
            self.redpanda.set_environment(
                {"__REDPANDA_EARLIEST_LOGICAL_VERSION": joiner_earliest_version}
            )
            self.redpanda.start_node(old_node)
        except DucktapeTimeoutError:
            pass
        else:
            raise RuntimeError(
                f"Node {old_node} joined cluster, but should have been rejected"
            )

        # Restart it with a sufficiently recent version and join should succeed
        self.logger.info(
            f"Starting node {old_node.name} with version {initial_version}"
        )
        self.redpanda.set_environment(
            {"__REDPANDA_LATEST_LOGICAL_VERSION": initial_version}
        )
        self.redpanda.set_environment(
            {"__REDPANDA_EARLIEST_LOGICAL_VERSION": initial_version}
        )
        self.redpanda.restart_nodes([old_node])

        # Timeout long enough for join retries & health monitor tick (registered
        # requires `is_alive`)
        wait_until(
            lambda: self.redpanda.registered(old_node), timeout_sec=30, backoff_sec=1
        )

    @cluster(num_nodes=4, log_allow_list=OLD_NODE_JOIN_LOG_ALLOW_LIST)
    def test_synthetic_old_node_join(self):
        # A node that reports a version range below the current version's earliest logical version
        # This fails the check that joining node's latest version must be >= the cluster active version
        self._test_synthetic_versions(
            self.head_earliest_logical_version - 2, self.head_latest_logical_version - 1
        )

    @cluster(num_nodes=4, log_allow_list=OLD_NODE_JOIN_LOG_ALLOW_LIST)
    def test_synthetic_too_new_node_join(self):
        # A node that reports a version range starting above the current active version.
        # This fails the test that joining nodes must have an earliest version <= the
        # active version.
        self._test_synthetic_versions(
            self.head_latest_logical_version + 1, self.head_latest_logical_version + 2
        )


class FeaturesUpgradeAssertionTest(FeaturesTestBase):
    @cluster(
        num_nodes=3, log_allow_list="Attempted to upgrade from incompatible version"
    )
    def test_upgrade_assertion(self):
        """
        That if we try to upgrade to a version whose earliest_logical_version is ahead
        of the pre-upgrade version, Redpanda refuses to start.
        :return:
        """

        upgrade_node = self.redpanda.nodes[-1]
        self.redpanda.stop_node(upgrade_node)

        self.redpanda.set_environment(
            {"__REDPANDA_LATEST_LOGICAL_VERSION": self.head_latest_logical_version + 2}
        )
        self.redpanda.set_environment(
            {
                "__REDPANDA_EARLIEST_LOGICAL_VERSION": self.head_latest_logical_version
                + 1
            }
        )

        # Startup should fail with an incompatible version
        with expect_exception(DucktapeTimeoutError, lambda _: True):
            self.redpanda.start_node(upgrade_node)

        # Don't assume that the asserted node will have exited promptly: explicitly kill it.
        self.redpanda.stop_node(upgrade_node, forced=True)

        # With the config set to override checks, start should succeed
        self.redpanda.start_node(
            upgrade_node, override_cfg_params={"upgrade_override_checks": True}
        )


class FeaturesUpgradeActivationTest(FeaturesTestBase):
    def setUp(self):
        pass

    @cluster(num_nodes=1)
    @parametrize(upgrade=False)
    @parametrize(upgrade=True)
    def test_new_cluster_only_activation(self, upgrade: bool):
        if upgrade:
            self.redpanda.set_environment(
                {
                    "__REDPANDA_TEST_FEATURES": "ON",
                    "__REDPANDA_LATEST_LOGICAL_VERSION": TEST_FEATURES_VERSION - 1,
                }
            )
        else:
            self.redpanda.set_environment(
                {
                    "__REDPANDA_TEST_FEATURES": "ON",
                    "__REDPANDA_LATEST_LOGICAL_VERSION": TEST_FEATURES_VERSION,
                }
            )

        self.redpanda.start()

        if upgrade:
            for f in [FEATURE_ALPHA_NAME, FEATURE_CHARLIE_NAME, FEATURE_CHARLIE_NAME]:
                # Pre-upgrade, none of the features should be available
                assert self._get_features_map()[f]["state"] == "unavailable"

            self.redpanda.set_environment(
                {
                    "__REDPANDA_TEST_FEATURES": "ON",
                    "__REDPANDA_LATEST_LOGICAL_VERSION": TEST_FEATURES_VERSION,
                }
            )
            self.redpanda.restart_nodes(self.redpanda.nodes)
            self.redpanda.wait_until(
                lambda: (
                    self._get_features_map()[FEATURE_ALPHA_NAME]["state"] == "available"
                ),
                timeout_sec=30,
                backoff_sec=1,
            )
        else:
            # No upgrade: feature should be available from time zero.
            assert self._get_features_map()[FEATURE_ALPHA_NAME]["state"] == "available"

        # Once we are on the test feature version, auto-active features should be on
        assert self._get_features_map()[FEATURE_BRAVO_NAME]["state"] == "active"

        # Once we are on the test feature version, new_clusters_only features' state
        # should depend on whether we upgraded or we were always at this version.
        assert (
            self._get_features_map()[FEATURE_CHARLIE_NAME]["state"] == "available"
            if upgrade
            else "active"
        )

    @cluster(num_nodes=1)
    @parametrize(disable=False)
    @parametrize(disable=True)
    def test_policy_change_in_minor_release(self, disable: bool):
        self.redpanda.set_environment(
            {
                "__REDPANDA_TEST_FEATURES": "ON",
                "__REDPANDA_LATEST_LOGICAL_VERSION": TEST_FEATURES_VERSION,
                "__REDPANDA_TEST_FEATURE_NO_AUTO_ACTIVATE_BRAVO": "true",
            }
        )
        self.logger.info(f"test: env={self.redpanda._environment}")
        self.redpanda.start()

        # The feature's policy is explicit_only, it should only go to available, not active
        assert self._get_features_map()[FEATURE_BRAVO_NAME]["state"] == "available"

        # Ensure that the config manager background loop isn't activating wrongly
        time.sleep(10)
        assert self._get_features_map()[FEATURE_BRAVO_NAME]["state"] == "available"

        # Ensure that restarts don't activate the feature
        self.redpanda.restart_nodes(self.redpanda.nodes)
        time.sleep(10)
        assert self._get_features_map()[FEATURE_BRAVO_NAME]["state"] == "available"

        if disable:
            # Explicitly disable the feature: this should prevent it auto activating
            # after the simulated upgrade
            self.admin.put_feature(FEATURE_BRAVO_NAME, {"state": "disabled"})
            self._wait_for_feature_everywhere(
                lambda fm: fm[FEATURE_BRAVO_NAME]["state"] == "disabled"
            )

        # Simulate upgrading to a .z release that changes the feature's policy to ::always
        self.redpanda.unset_environment(
            ["__REDPANDA_TEST_FEATURE_NO_AUTO_ACTIVATE_BRAVO"]
        )
        self.redpanda.set_environment(
            {
                "__REDPANDA_TEST_FEATURES": "ON",
                "__REDPANDA_LATEST_LOGICAL_VERSION": TEST_FEATURES_VERSION,
            }
        )
        self.redpanda.restart_nodes(self.redpanda.nodes)

        if disable:
            # Because feature was explicitly disabled, it should not auto-activate
            assert self._get_features_map()[FEATURE_BRAVO_NAME]["state"] == "disabled"
            time.sleep(10)
            # ...even after time for some background ticks
            assert self._get_features_map()[FEATURE_BRAVO_NAME]["state"] == "disabled"
        else:
            # Now that the feature's policy is to auto-activate, it should activate
            self._wait_for_feature_everywhere(
                lambda fm: fm[FEATURE_BRAVO_NAME]["state"] == "active"
            )


# Synthetic logical versions used by ManualFinalizationTest to simulate an
# upgrade scenario without requiring real binary versioning.
MANUAL_FINALIZE_OLD_VERSION = 2000
MANUAL_FINALIZE_NEW_VERSION = 2001


class ManualFinalizationTest(FeaturesTestBase):
    """
    Tests for the manual upgrade finalization flow controlled by
    `features_auto_finalization`.

    Uses synthetic logical versions (via `__REDPANDA_LATEST_LOGICAL_VERSION`)
    to simulate an upgrade without an actual binary install/upgrade. The
    cluster boots at MANUAL_FINALIZE_OLD_VERSION, and individual nodes are
    "upgraded" by restarting them with the env var bumped to
    MANUAL_FINALIZE_NEW_VERSION.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)
        self.admin_v2 = AdminV2(self.redpanda)

    def setUp(self):
        # Defer cluster start to test bodies so each test can choose its own
        # initial logical version.
        pass

    def _start_at_old(self):
        """Start the cluster with all nodes reporting the old logical version."""
        self.redpanda.set_environment(
            {
                "__REDPANDA_LATEST_LOGICAL_VERSION": str(MANUAL_FINALIZE_OLD_VERSION),
            }
        )
        self.redpanda.start()
        wait_until(
            lambda: (
                self.admin.get_features()["cluster_version"]
                == MANUAL_FINALIZE_OLD_VERSION
            ),
            timeout_sec=30,
            backoff_sec=1,
        )

    def _restart_at_new(self, nodes):
        """Restart the given nodes with the new logical version env var."""
        self.redpanda.set_environment(
            {
                "__REDPANDA_LATEST_LOGICAL_VERSION": str(MANUAL_FINALIZE_NEW_VERSION),
            }
        )
        self.redpanda.restart_nodes(nodes)

    def _disable_auto_finalization(self):
        self.redpanda.set_cluster_config({"features_auto_finalization": False})

    def _call_with_leader_retry(self, call, timeout_sec=30):
        """Invoke a controller-leader-routed admin v2 call, retrying through
        the transient UNAVAILABLE ("controller has no leader") window that can
        occur during or after node restarts and leadership transfers --
        notably under parallel load, where elections take longer. Unlike the
        v1 Admin client, the v2 connect client does not retry leadership
        itself. Only UNAVAILABLE is retried; other errors (e.g.
        FAILED_PRECONDITION) propagate immediately."""
        deadline = time.time() + timeout_sec
        while True:
            try:
                return call()
            except ConnectError as e:
                if e.code != ConnectErrorCode.UNAVAILABLE or time.time() >= deadline:
                    raise
                time.sleep(1)

    def _finalize(self):
        return self._call_with_leader_retry(
            lambda: self.admin_v2.features().finalize_upgrade(
                features_pb2.FinalizeUpgradeRequest()
            )
        )

    def _get_upgrade_status(self):
        return self._call_with_leader_retry(
            lambda: self.admin_v2.features().get_upgrade_status(
                features_pb2.GetUpgradeStatusRequest()
            )
        )

    def _wait_for_status_state(self, state, timeout_sec=30):
        """Wait until GetUpgradeStatus reports `state`; return that status."""
        wait_until(
            lambda: self._get_upgrade_status().state == state,
            timeout_sec=timeout_sec,
            backoff_sec=1,
        )
        return self._get_upgrade_status()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_auto_finalization_disabled_blocks_advance(self):
        """
        With `features_auto_finalization=false` and a completed rolling
        upgrade, `cluster_version` must not auto-advance. The controller
        leader logs a rate-limited "deferring advance" message.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        self._restart_at_new(self.redpanda.nodes)

        # Give the controller leader plenty of time to observe all nodes at
        # the new version. cluster_version must not advance because
        # auto-finalization is off and no manual request has been issued.
        time.sleep(15)
        assert (
            self.admin.get_features()["cluster_version"] == MANUAL_FINALIZE_OLD_VERSION
        )

        # The observability RPC reports the same situation: a uniform higher
        # version is available, but the cluster is held at the old (downgrade
        # floor) version pending an explicit finalize.
        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )
        assert status.active_version == MANUAL_FINALIZE_OLD_VERSION
        assert status.version_after_finalization == MANUAL_FINALIZE_NEW_VERSION
        assert not status.auto_finalization_enabled

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_manual_request_triggers_advance(self):
        """
        With `features_auto_finalization=false`, an explicit FinalizeUpgrade
        RPC drives `cluster_version` forward to the version reported uniformly
        by all members.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        self._restart_at_new(self.redpanda.nodes)

        # Confirm the cluster is in the deferred state before triggering.
        time.sleep(5)
        assert (
            self.admin.get_features()["cluster_version"] == MANUAL_FINALIZE_OLD_VERSION
        )

        # Uniformly upgraded but deferred: the cluster is ready to finalize.
        ready = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )
        assert ready.version_after_finalization == MANUAL_FINALIZE_NEW_VERSION

        self._finalize()

        self._wait_for_version_everywhere(MANUAL_FINALIZE_NEW_VERSION)

        # Once the advance lands, the status flips to finalized and the active
        # version (the downgrade floor) catches up to the binaries.
        finalized = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_FINALIZED
        )
        assert finalized.active_version == MANUAL_FINALIZE_NEW_VERSION
        assert finalized.version_after_finalization == MANUAL_FINALIZE_NEW_VERSION

    @cluster(num_nodes=3)
    def test_manual_request_rejected_when_auto_enabled(self):
        """
        FinalizeUpgrade is only meaningful when auto-finalization is disabled.
        With the default `features_auto_finalization=true`, the RPC must
        return FAILED_PRECONDITION.
        """
        self.redpanda.start()

        with expect_exception(
            ConnectError,
            lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION,
        ):
            self._finalize()

        # The read-only status RPC is not gated on auto-finalization: it
        # succeeds and reflects that automatic finalization is enabled.
        status = self._get_upgrade_status()
        assert status.state == features_pb2.FINALIZATION_STATE_FINALIZED, status.state
        assert status.auto_finalization_enabled

    @cluster(num_nodes=3)
    def test_manual_request_idempotent_when_no_advance_pending(self):
        """
        FinalizeUpgrade is a no-op (success) when the cluster is already at
        the latest reported version.
        """
        self.redpanda.start()
        self._disable_auto_finalization()

        # Cluster is already at the binary's latest version: nothing pending.
        # The RPC should return success without changing state.
        version_before = self.admin.get_features()["cluster_version"]

        self._finalize()
        # Repeated calls should also succeed.
        self._finalize()

        time.sleep(2)
        assert self.admin.get_features()["cluster_version"] == version_before

        status = self._get_upgrade_status()
        assert status.state == features_pb2.FINALIZATION_STATE_FINALIZED, status.state
        assert status.active_version == version_before
        assert status.version_after_finalization == version_before

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_manual_request_does_not_advance_on_mixed_version(self):
        """
        With nodes reporting different logical versions (a partial
        upgrade), the request is accepted but the background loop's
        precondition check rejects the advance. The cluster stays at
        the old version.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        # Upgrade only two of three nodes — leave the third at OLD_VERSION.
        self._restart_at_new(self.redpanda.nodes[:2])

        # Wait long enough for health reports to propagate so the controller
        # leader's view of node versions is up to date.
        time.sleep(15)

        self._finalize()

        # Give the loop time to attempt and back off.
        time.sleep(15)
        assert (
            self.admin.get_features()["cluster_version"] == MANUAL_FINALIZE_OLD_VERSION
        )

        # The status RPC exposes the mixed versions and keeps
        # version_after_finalization pinned to the (unchanged) active version.
        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_UPGRADE_IN_PROGRESS
        )
        assert status.version_after_finalization == MANUAL_FINALIZE_OLD_VERSION
        assert sorted(m.logical_version for m in status.members) == [
            MANUAL_FINALIZE_OLD_VERSION,
            MANUAL_FINALIZE_NEW_VERSION,
            MANUAL_FINALIZE_NEW_VERSION,
        ]

    @cluster(
        num_nodes=3,
        log_allow_list=RESTART_LOG_ALLOW_LIST + ["node_status_backend.*Failed to send"],
    )
    def test_manual_request_does_not_advance_with_dead_node(self):
        """
        With a member node forcibly stopped, the request is accepted but
        the background loop's liveness check rejects the advance. The
        cluster stays at the old version.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        self._restart_at_new(self.redpanda.nodes)
        time.sleep(5)

        # Pick a node that isn't the controller leader so the controller
        # itself stays available to serve the RPC.
        leader_id = self.admin.await_stable_leader(
            namespace="redpanda", topic="controller", partition=0
        )
        victim = next(
            n for n in self.redpanda.nodes if self.redpanda.node_id(n) != leader_id
        )
        self.redpanda.stop_node(victim, forced=True)

        # Wait for the controller's health monitor to mark the victim
        # not-alive (alive_timeout_ms default is 5s). Without this wait
        # the loop's first attempt could land before the liveness check
        # turns negative, and the advance would proceed.
        time.sleep(15)

        self._finalize()

        # Give the loop time to attempt and back off.
        time.sleep(15)
        assert (
            self.admin.get_features()["cluster_version"] == MANUAL_FINALIZE_OLD_VERSION
        )

        # The status RPC surfaces the liveness gap: every node is on the new
        # version, but the stopped node reports not-alive, so the cluster is
        # not finalizable.
        victim_id = self.redpanda.node_id(victim)
        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_UPGRADE_IN_PROGRESS
        )
        assert status.version_after_finalization == MANUAL_FINALIZE_OLD_VERSION
        members = {m.node_id: m for m in status.members}
        assert not members[victim_id].alive, f"victim {victim_id} should be dead"
        assert all(
            members[self.redpanda.node_id(n)].alive
            for n in self.redpanda.nodes
            if self.redpanda.node_id(n) != victim_id
        )

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_manual_request_lost_on_leader_failover(self):
        """
        The pending request is held only in memory on the controller leader.
        A leader change before the loop tick replicates the advance loses the
        request, and the operator must re-issue against the new leader.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        self._restart_at_new(self.redpanda.nodes)
        time.sleep(5)
        assert (
            self.admin.get_features()["cluster_version"] == MANUAL_FINALIZE_OLD_VERSION
        )

        old_leader = self.admin.await_stable_leader(
            namespace="redpanda", topic="controller", partition=0
        )
        target = next(
            self.redpanda.node_id(n)
            for n in self.redpanda.nodes
            if self.redpanda.node_id(n) != old_leader
        )

        # Issue the RPC and immediately move controller leadership. There is
        # a small window in which the original leader's loop may have
        # replicated the advance before the transfer — if so, the test still
        # passes (the RPC was honored), and the re-issue below is just an
        # idempotent no-op.
        self._finalize()
        self.admin.transfer_leadership_to(
            namespace="redpanda",
            topic="controller",
            partition=0,
            target_id=target,
        )

        new_leader = self.admin.await_stable_leader(
            namespace="redpanda", topic="controller", partition=0
        )
        assert new_leader == target, f"expected leader {target}, got {new_leader}"

        # Give the new leader time to repopulate `_node_versions` from
        # health reports (they were cleared on the leadership change).
        # Without this wait the re-issued RPC could see an empty version map
        # and return `ok` as a no-op without advancing.
        time.sleep(15)

        # Re-issue against the new leader: idempotent if the advance already
        # landed; otherwise this is the call that drives the advance.
        self._finalize()
        self._wait_for_version_everywhere(MANUAL_FINALIZE_NEW_VERSION)

        # The new leader serves the status RPC and reports the finalized state.
        finalized = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_FINALIZED
        )
        assert finalized.active_version == MANUAL_FINALIZE_NEW_VERSION

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_get_upgrade_status_reports_lifecycle(self):
        """
        GetUpgradeStatus reflects the finalization lifecycle: FINALIZED at
        rest, READY_TO_FINALIZE once all nodes report a higher version under
        `features_auto_finalization=false`, then FINALIZED again after an
        explicit FinalizeUpgrade. active_version doubles as the downgrade
        floor: it equals version_after_finalization (no downgrade possible)
        only in the FINALIZED states.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        # At rest: nothing to finalize, no downgrade possible.
        status = self._get_upgrade_status()
        assert status.state == features_pb2.FINALIZATION_STATE_FINALIZED, status.state
        assert status.active_version == MANUAL_FINALIZE_OLD_VERSION
        assert status.version_after_finalization == MANUAL_FINALIZE_OLD_VERSION
        assert not status.auto_finalization_enabled
        assert len(status.members) == len(self.redpanda.nodes)
        assert all(m.version_known and m.alive for m in status.members)
        assert all(
            m.logical_version == MANUAL_FINALIZE_OLD_VERSION for m in status.members
        )
        # release_version is plumbed through from the per-node health report.
        assert all(m.release_version for m in status.members)

        # Roll every node to the new version. Auto-finalization is off, so the
        # active version holds and the cluster becomes READY_TO_FINALIZE.
        self._restart_at_new(self.redpanda.nodes)

        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )
        assert status.active_version == MANUAL_FINALIZE_OLD_VERSION
        assert status.version_after_finalization == MANUAL_FINALIZE_NEW_VERSION
        assert all(
            m.logical_version == MANUAL_FINALIZE_NEW_VERSION for m in status.members
        )

        # Finalize: the active version catches up; no downgrade after this.
        self._finalize()
        self._wait_for_version_everywhere(MANUAL_FINALIZE_NEW_VERSION)

        status = self._wait_for_status_state(features_pb2.FINALIZATION_STATE_FINALIZED)
        assert status.active_version == MANUAL_FINALIZE_NEW_VERSION
        assert status.version_after_finalization == MANUAL_FINALIZE_NEW_VERSION

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_get_upgrade_status_reports_in_progress(self):
        """
        With a partial rolling upgrade (mixed logical versions),
        GetUpgradeStatus reports UPGRADE_IN_PROGRESS and holds
        version_after_finalization at the unchanged active version, while
        the per-member list exposes the version spread.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        # Upgrade only two of three nodes; the third stays at OLD_VERSION.
        self._restart_at_new(self.redpanda.nodes[:2])

        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_UPGRADE_IN_PROGRESS
        )
        assert status.active_version == MANUAL_FINALIZE_OLD_VERSION
        assert status.version_after_finalization == MANUAL_FINALIZE_OLD_VERSION
        assert sorted(m.logical_version for m in status.members) == [
            MANUAL_FINALIZE_OLD_VERSION,
            MANUAL_FINALIZE_NEW_VERSION,
            MANUAL_FINALIZE_NEW_VERSION,
        ]


# The `features_auto_finalization` knob was backported to v26.1.9, so the real
# upgrade below must start from a released patch that already has it. The knob
# lets a cluster opt out of auto-finalization *before* upgrading to a HEAD build
# that ships the gating logic and the admin v2 RPCs.
MANUAL_FINALIZE_MIN_OLD_RELEASE = (26, 1, 9)

# The same knob was also backported to v25.3.15, the oldest release from which a
# multi-hop upgrade (v25.3 -> v26.1 -> HEAD) can carry the opt-out through every
# hop with the flag set only once.
MANUAL_FINALIZE_MIN_OLDEST_RELEASE = (25, 3, 15)

# How long to dwell after the cluster reports READY_TO_FINALIZE before
# re-checking that the active version is still held. Guards against a late,
# incorrect advance on a subsequent gating-loop tick.
MANUAL_FINALIZE_HOLD_DWELL_SEC = 20

# Number of upgrade -> perturb -> downgrade rollback cycles to run before the
# final upgrade is finalized. Each cycle simulates discovering a problem after
# upgrading the binaries and rolling back to the prior release without
# finalizing -- the core capability the unfinalized-upgrade feature provides.
DOWNGRADE_ROLLBACK_CYCLES = 2

# Baseline perturbation: a fixed set of topics, seeded once, then re-read in
# every state to confirm data survives the upgrade/downgrade transitions.
PERTURB_TOPIC_COUNT = 10
PERTURB_TOPIC_PARTITIONS = 5
PERTURB_RECORDS_PER_TOPIC = 100
PERTURB_RECORD_SIZE = 128

# Features the per-step perturbations exercise / deliberately skip, accumulated
# across upgrade steps (today only v26.1 -> v26.2). The coverage guard
# (_assert_feature_coverage) cross-checks these against the live feature table,
# so a newly gated feature cannot be added -- in any major -- without either an
# exercise or an explicit acknowledgement. Add a feature's name here when you
# cover it or knowingly skip it; entries for features no longer gated by the
# current upgrade are harmless extras, so no per-major pruning is needed.
PERTURB_EXERCISED_FEATURES = frozenset({"tiered_cloud_topics"})
PERTURB_ACKNOWLEDGED_FEATURES = frozenset(
    {
        # Cluster-linking features, out of scope for this single-cluster test.
        "shadow_link_sr_api_sync",
        "batch_mirror_topic_status",
        # Iceberg extended-mode topic-config gate; exercising it needs Iceberg
        # topic setup orthogonal to the finalization behavior under test.
        "iceberg_extended_mode_config",
    }
)


class ManualFinalizationUpgradeTest(FeaturesTestBase):
    """
    Real-upgrade counterpart to ManualFinalizationTest.

    ManualFinalizationTest fakes the version bump with
    `__REDPANDA_LATEST_LOGICAL_VERSION`; this test performs an actual binary
    upgrade from the latest released patch of the prior feature line (which must
    include the backported `features_auto_finalization` knob, i.e.
    >= v26.1.9) up to the HEAD build that ships the manual-finalization gating
    logic and the admin v2 RPCs.

    It mirrors three ManualFinalizationTest scenarios end to end:
      - auto-finalization disabled blocks the post-upgrade advance,
      - an explicit FinalizeUpgrade drives the advance, and
      - GetUpgradeStatus reports the finalization lifecycle.

    The old release has neither the gating logic nor the v2 RPCs, so the opt-out
    is set on the old binary (which also exercises the backported knob) and the
    status/finalize RPCs are only invoked once every node is on HEAD.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)
        self.admin_v2 = AdminV2(self.redpanda)
        # Logical versions are discovered from the running binaries at runtime
        # rather than hard-coded as in the synthetic test.
        self.old_logical = None
        self.new_logical = None
        self.old_release = None
        # Topics created by _perturb_common on its first call; persisted across
        # upgrade/downgrade transitions so data survival can be re-checked.
        self._perturb_topics: list[str] = []

    def setUp(self):
        # Defer cluster start: the old release must be installed before the
        # first boot. FeaturesTestBase.setUp assumes a HEAD cluster (it asserts
        # active == latest == original), so we deliberately skip it.
        pass

    def _start_at_old(self):
        """Install and boot the whole cluster on the old released version."""
        old_release = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )
        assert old_release >= MANUAL_FINALIZE_MIN_OLD_RELEASE, (
            f"prior feature version {old_release} predates the "
            f"features_auto_finalization backport "
            f"{MANUAL_FINALIZE_MIN_OLD_RELEASE}; cannot opt out before upgrade"
        )
        self.old_release = old_release
        self.logger.info(f"Starting cluster on old release {old_release}")
        self.installer.install(self.redpanda.nodes, old_release)
        self.redpanda.start()

        self.old_logical = self.admin.get_features()["cluster_version"]
        self.logger.info(
            f"Old release {old_release} reports logical version {self.old_logical}"
        )

    def _restart_at_new(self, nodes):
        """Upgrade `nodes` to the HEAD build and restart them in place."""
        self.installer.install(nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(nodes)
        self._wait_for_cluster_settled()

        self.new_logical = self._node_latest_logical_version(nodes[0])
        self.logger.info(f"HEAD build reports logical version {self.new_logical}")
        assert self.new_logical > self.old_logical, (
            f"expected HEAD logical version {self.new_logical} to exceed the old "
            f"version {self.old_logical}; the upgrade did not raise the version"
        )

    def _node_latest_logical_version(self, node):
        """Read a (possibly just-restarted) node's latest logical version,
        tolerating the brief window before it is serving the admin API."""

        def query():
            return self.admin.get_features(node=node).get("node_latest_version")

        return wait_until_result(
            query,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="node did not report node_latest_version after upgrade",
        )

    def _upgrade_all_to(self, version):
        """Install `version` on every node, restart in place, and return the
        binary's latest logical version."""
        self.installer.install(self.redpanda.nodes, version)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._wait_for_cluster_settled()
        return self._node_latest_logical_version(self.redpanda.nodes[0])

    def _wait_for_cluster_version(self, target, timeout_sec=60):
        """Wait until every node reports cluster_version == target."""

        def check():
            return all(
                self.admin.get_features(node=n)["cluster_version"] == target
                for n in self.redpanda.nodes
            )

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=1)

    def _wait_for_cluster_settled(self, timeout_sec=90):
        """Wait for the cluster to settle after a restart: every broker has
        rejoined and no under-replicated partitions remain. `start_node` only
        waits for per-node readiness (the v1 ready probe), not cluster
        convergence -- so without this a "did not advance" assertion could pass
        merely because the controller has not yet observed the upgrade."""
        self.redpanda.wait_for_membership(first_start=False)
        wait_until(
            self.redpanda.healthy,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="cluster did not become healthy after restart",
        )

    def _downgrade_all_to(self, release):
        """Roll every node back to `release` (an older binary) without
        finalizing, then wait for the cluster to come back healthy. This is the
        rollback the unfinalized-upgrade feature is meant to preserve: because
        the active version was never advanced, the older binary can still run on
        the existing on-disk data."""
        self.installer.install(self.redpanda.nodes, release)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._wait_for_cluster_settled()

    def _perturb(self, phase):
        """Perturb the cluster while it sits in `phase` (e.g.
        "...-upgraded-unfinalized" on the new binary, or "...-downgraded" after a
        rollback). Structured as 1 + N parts: shared baseline work, then one
        function per supported unfinalized-upgrade step. Today N=1 -- the
        v26.1 -> v26.2 step. See the note above the per-step functions for how
        this grows for v26.3 and beyond."""
        self.logger.info(f"perturb: phase={phase}")
        self._perturb_common(phase)
        if "upgraded" in phase:
            # Generic across majors: every feature gated by the upgrade under
            # test must be exercised or acknowledged.
            self._assert_feature_coverage()
        self._perturb_v26_1_to_v26_2(phase)

    def _perturb_common(self, phase):
        """Baseline data-plane perturbation, run in every state. On the first
        call it creates a fixed set of topics and seeds each with records. Every
        call then reads all records back, restarts the whole cluster in place
        (same binary), waits for it to return healthy, and reads the records
        again. The topics persist across the upgrade/downgrade transitions, so
        the read-backs confirm data produced on one binary stays intact in the
        other and survives an in-place restart -- a feature-agnostic integrity
        signal that holds regardless of which features are active."""
        rpk = RpkTool(self.redpanda)
        if not self._perturb_topics:
            kafka = KafkaCliTools(self.redpanda)
            for i in range(PERTURB_TOPIC_COUNT):
                name = f"perturb-{i}"
                rpk.create_topic(name, partitions=PERTURB_TOPIC_PARTITIONS)
                kafka.produce(
                    name, PERTURB_RECORDS_PER_TOPIC, PERTURB_RECORD_SIZE, acks=-1
                )
                self._perturb_topics.append(name)

        self._perturb_verify_data(f"{phase} pre-restart")

        # Restart the whole cluster in place (same binary) and confirm it comes
        # back healthy with the data still readable.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._wait_for_cluster_settled()
        self._perturb_verify_data(f"{phase} post-restart")

    def _perturb_verify_data(self, label):
        """Read every record back from each perturbation topic and assert the
        full count is still present."""
        rpk = RpkTool(self.redpanda)
        for name in self._perturb_topics:
            consumed = rpk.consume(
                name,
                n=PERTURB_RECORDS_PER_TOPIC,
                offset="start",
                quiet=True,
                timeout=30,
            )
            got = sum(1 for line in consumed.splitlines() if line)
            assert got == PERTURB_RECORDS_PER_TOPIC, (
                f"{name}: expected {PERTURB_RECORDS_PER_TOPIC} records intact "
                f"({label}), read {got}"
            )

    # NOTE: growing this beyond the v26.1 -> v26.2 step.
    #
    # When v26.2 is released and v26.3 development begins, two things change:
    #   1. Add a new per-step function, e.g. _perturb_v26_2_to_v26_3, exercising
    #      the features gated by the unfinalized v26.2 -> v26.3 upgrade, and call
    #      it from _perturb alongside the existing one.
    #   2. Extend the harness to perform CHAINED unfinalized upgrades: an
    #      unfinalized upgrade v26.1 -> v26.2, then a further unfinalized upgrade
    #      v26.2 -> v26.3, perturbing (and exercising downgrade) at each step
    #      instead of a single old -> HEAD hop.
    # Keep older step functions and their harness coverage for as long as the
    # support window allows upgrading from those releases; drop a step once its
    # source release leaves the supported upgrade matrix.
    def _perturb_v26_1_to_v26_2(self, phase):
        """Per-step perturbation for the v26.1 -> v26.2 unfinalized upgrade (the
        N=1 part of the 1 + N structure).

        Exercises the code paths gated by the v26.2 feature flags. While the
        upgrade is unfinalized the active version is held at v26.1, so these
        features are still unavailable and their gates are in force -- which is
        what keeps a downgrade safe. Each feature's check is feature-specific
        (see the helpers); the shared harness separately confirms the system
        stays functional after the up/downgrade, and _verify_v26_2_features_working
        confirms the features actually work once finalized.

        Only runs on the v26.2 binary: on the rolled-back v26.1 binary these code
        paths and config values do not exist, so there is nothing to exercise."""
        if "upgraded" not in phase:
            return
        self._exercise_tiered_cloud_topics()
        # The other two v26.2-gated features are not exercised by this
        # single-cluster perturbation: shadow_link_sr_api_sync and
        # batch_mirror_topic_status are both cluster-linking features
        # (batch_mirror_topic_status additionally needs a second cluster).

    def _exercise_tiered_cloud_topics(self):
        """tiered_cloud_topics gate: creating a topic with storage mode
        `tiered_cloud` is refused until the feature is active. Drive that
        validator path while unfinalized and confirm the feature is unavailable
        and the create is rejected. The topic is never created, so exercising
        the gate cannot leave state that would block a downgrade."""
        assert self._feature_state("tiered_cloud_topics") == "unavailable", (
            "tiered_cloud_topics should be unavailable while the upgrade is unfinalized"
        )
        try:
            RpkTool(self.redpanda).create_topic(
                "perturb-tiered-cloud",
                partitions=1,
                config={
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED_CLOUD
                },
            )
        except RpkException as e:
            # Confirm the create failed via the storage-mode gate, not an
            # unrelated rpk/controller error (timeout, UNAVAILABLE, controller
            # not ready) that would otherwise read as a false "gate worked". The
            # HEAD validator rejects with INVALID_CONFIG and this distinctive
            # message; the create only ever runs on the HEAD binary, so the
            # string is stable.
            assert "Invalid storage mode" in str(e), (
                f"tiered_cloud create failed, but not via the storage-mode gate: {e}"
            )
        else:
            raise AssertionError(
                "creating a tiered_cloud topic should be gated while unfinalized"
            )

    def _verify_tiered_cloud_topics_working(self):
        """After finalize the active version has advanced past the feature's
        require_version, so the gate opens: the feature moves from unavailable to
        available (it is explicit_only, so it does not auto-activate) and can
        then be enabled to active -- the simple signal that it now works.
        (Creating an actual tiered_cloud topic additionally needs cloud storage,
        which this test does not configure.)"""
        wait_until(
            lambda: self._feature_state("tiered_cloud_topics") == "available",
            timeout_sec=30,
            backoff_sec=1,
            err_msg="tiered_cloud_topics did not become available after finalize",
        )
        self.admin.put_feature("tiered_cloud_topics", {"state": "active"})
        wait_until(
            lambda: self._feature_state("tiered_cloud_topics") == "active",
            timeout_sec=30,
            backoff_sec=1,
            err_msg="tiered_cloud_topics did not activate after being enabled",
        )

    def _verify_v26_2_features_working(self):
        """After the upgrade is finalized, confirm each v26.2 feature's gate has
        opened and the feature actually works (simple per-feature predicates)."""
        self._verify_tiered_cloud_topics_working()

    def _feature_state(self, name):
        for f in self.admin.get_features()["features"]:
            if f["name"] == name:
                return f["state"]
        return None

    def _assert_feature_coverage(self):
        """Cross-check the perturbation's coverage against the live feature
        table. Major-agnostic: it never names a specific release.

        While the upgrade is unfinalized the active version is held at the old
        release, so the features gated by the upgrade under test are exactly
        those the features endpoint reports `unavailable`: they require the
        not-yet-active version of the binary we upgraded to, and a binary never
        carries features for a major beyond its own, so this is precisely the
        immediate next-major set (features further out cannot exist here).

        Every such feature must be exercised or explicitly acknowledged; fail --
        with an actionable message -- if any is in neither registry, so a newly
        gated feature cannot slip in untested. The registries are cumulative
        across upgrade steps, so a feature no longer gated by the current step is
        a harmless extra and needs no pruning."""
        gated = {
            f["name"]
            for f in self.admin.get_features()["features"]
            if f["state"] == "unavailable"
        }
        uncovered = gated - (PERTURB_EXERCISED_FEATURES | PERTURB_ACKNOWLEDGED_FEATURES)
        assert not uncovered, (
            f"the upgrade under test gates feature(s) {sorted(uncovered)} with no "
            "perturbation coverage: add an exercise in the matching _perturb_* "
            "step, or acknowledge them in PERTURB_ACKNOWLEDGED_FEATURES"
        )

    def _disable_auto_finalization(self):
        self.redpanda.set_cluster_config({"features_auto_finalization": False})

    def _call_with_leader_retry(self, call, timeout_sec=30):
        """Retry a controller-leader-routed admin v2 call through the transient
        UNAVAILABLE window after restarts/leadership changes. See the identical
        helper in ManualFinalizationTest for the rationale."""
        deadline = time.time() + timeout_sec
        while True:
            try:
                return call()
            except ConnectError as e:
                if e.code != ConnectErrorCode.UNAVAILABLE or time.time() >= deadline:
                    raise
                time.sleep(1)

    def _finalize(self):
        return self._call_with_leader_retry(
            lambda: self.admin_v2.features().finalize_upgrade(
                features_pb2.FinalizeUpgradeRequest()
            )
        )

    def _get_upgrade_status(self):
        return self._call_with_leader_retry(
            lambda: self.admin_v2.features().get_upgrade_status(
                features_pb2.GetUpgradeStatusRequest()
            )
        )

    def _wait_for_status_state(self, state, timeout_sec=30):
        """Wait until GetUpgradeStatus reports `state`; return that status."""
        wait_until(
            lambda: self._get_upgrade_status().state == state,
            timeout_sec=timeout_sec,
            backoff_sec=1,
        )
        return self._get_upgrade_status()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_auto_finalization_disabled_blocks_advance(self):
        """
        Real-upgrade analogue of
        ManualFinalizationTest.test_auto_finalization_disabled_blocks_advance.

        With `features_auto_finalization=false` set on the old release before
        the upgrade, a completed rolling upgrade to HEAD must not auto-advance
        cluster_version. The opt-out, written on the old binary, has to survive
        the version boundary and be honored by the HEAD gating logic.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        self._restart_at_new(self.redpanda.nodes)

        # Reaching READY_TO_FINALIZE is positive proof that the controller
        # observed every node at the new version and chose to defer the advance,
        # rather than a fixed sleep that could pass before the gating loop has
        # even run. The active version is held at the old (downgrade floor)
        # version, with the uniform higher version available to finalize.
        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )
        assert self.admin.get_features()["cluster_version"] == self.old_logical
        assert status.active_version == self.old_logical
        assert status.version_after_finalization == self.new_logical
        assert not status.auto_finalization_enabled

        # Dwell and re-check: the version must stay held across subsequent
        # gating-loop ticks, not advance late.
        time.sleep(MANUAL_FINALIZE_HOLD_DWELL_SEC)
        assert self.admin.get_features()["cluster_version"] == self.old_logical
        assert (
            self._get_upgrade_status().state
            == features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_get_upgrade_status_reports_lifecycle(self):
        """
        Real-upgrade analogue of two merged ManualFinalizationTest scenarios:
        test_get_upgrade_status_reports_lifecycle and
        test_manual_request_triggers_advance. A real upgrade is expensive, and
        walking the lifecycle already exercises the manual-triggered advance, so
        a single upgrade covers both: the v2 status reporting and the advance
        that an explicit FinalizeUpgrade drives.

        The old release has no v2 RPCs, so (unlike the synthetic test) the
        FINALIZED-at-rest state cannot be observed before the upgrade; the
        lifecycle is observed from READY_TO_FINALIZE (post-upgrade) through
        FINALIZED, checked against both the v2 status and the v1 features API.
        """
        self._start_at_old()
        self._disable_auto_finalization()

        # Roll every node to HEAD. Auto-finalization is off, so the active
        # version holds and the cluster becomes READY_TO_FINALIZE.
        self._restart_at_new(self.redpanda.nodes)

        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )
        assert status.active_version == self.old_logical
        assert status.version_after_finalization == self.new_logical
        # The v1 features API agrees with the v2 status: the advance is deferred,
        # so cluster_version is still held at the old (downgrade-floor) version.
        assert self.admin.get_features()["cluster_version"] == self.old_logical
        assert len(status.members) == len(self.redpanda.nodes)
        assert all(m.version_known and m.alive for m in status.members)
        assert all(m.logical_version == self.new_logical for m in status.members)
        # release_version is plumbed through from the per-node health report.
        assert all(m.release_version for m in status.members)

        # Finalize: the active version catches up; no downgrade after this.
        self._finalize()
        self._wait_for_version_everywhere(self.new_logical)

        status = self._wait_for_status_state(features_pb2.FINALIZATION_STATE_FINALIZED)
        assert status.active_version == self.new_logical
        assert status.version_after_finalization == self.new_logical

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_config_passes_through_multi_hop_upgrade(self):
        """
        Lock in that `features_auto_finalization`, set once on the oldest
        release, survives a multi-hop upgrade (v25.3.x -> v26.1.x -> HEAD) and
        is still honored by the final binary's gating logic.

        The knob was backported to both v25.3.15 and v26.1.9, so every
        intermediate binary recognizes the property and carries it through the
        controller log unchanged. Only HEAD consumes the knob, so the
        deferred-advance behavior appears only after the final hop -- the
        intermediate v26.1 hop advances the active version normally.
        """
        oldest, _ = self.installer.latest_for_line((25, 3))
        assert oldest >= MANUAL_FINALIZE_MIN_OLDEST_RELEASE, (
            f"latest v25.3 patch {oldest} predates the features_auto_finalization "
            f"backport {MANUAL_FINALIZE_MIN_OLDEST_RELEASE}"
        )
        mid, _ = self.installer.latest_for_line((26, 1))
        assert mid >= MANUAL_FINALIZE_MIN_OLD_RELEASE, (
            f"latest v26.1 patch {mid} predates the backport "
            f"{MANUAL_FINALIZE_MIN_OLD_RELEASE}"
        )

        # Hop 0: boot the oldest release and opt out of auto-finalization once.
        # Setting the flag here is a convenience (set-it-once-early), NOT a
        # guarantee of multi-hop downgradability: a multi-hop upgrade does not
        # preserve the ability to downgrade across every hop. Only the gated hop
        # (to HEAD) keeps its downgrade open -- see the assertions below.
        self.logger.info(f"Booting oldest release {oldest}")
        self.installer.install(self.redpanda.nodes, oldest)
        self.redpanda.start()
        self._disable_auto_finalization()

        # Hop 1: upgrade to the latest v26.1 patch. v26.1 has the knob but not
        # the gating logic, so it auto-advances the active version to its own
        # latest; the flag rides along in the controller log untouched.
        self.logger.info(f"Upgrading to intermediate release {mid}")
        mid_logical = self._upgrade_all_to(mid)
        self._wait_for_cluster_version(mid_logical)
        held_version = mid_logical

        # The v26.1 hop was NOT gated, so the active version -- which doubles as
        # the downgrade floor -- has advanced to the v26.1 logical version.
        # Downgrade back to v25.3 is no longer possible from here: the flag set
        # on v25.3 did not (and cannot) protect this ungated hop.
        assert self.admin.get_features()["cluster_version"] == held_version

        # Hop 2: upgrade to HEAD, which DOES consume the knob. Because the flag
        # survived both hops, the final advance must be deferred.
        self.logger.info("Upgrading to HEAD")
        head_logical = self._upgrade_all_to(RedpandaInstaller.HEAD)
        assert head_logical > held_version, (
            f"HEAD logical {head_logical} should exceed the held version {held_version}"
        )

        # READY_TO_FINALIZE proves HEAD observed the completed upgrade and
        # deferred the advance (stronger than a fixed sleep that could pass
        # before the gating loop runs).
        status = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
        )
        # The active version (downgrade floor) is held at the v26.1 version, so
        # downgrade from v26.2 back to v26.1 IS preserved: this gated hop is the
        # only one whose downgrade the flag keeps open.
        assert self.admin.get_features()["cluster_version"] == held_version
        assert status.active_version == held_version
        assert status.version_after_finalization == head_logical
        # The decisive passthrough check: HEAD sees auto-finalization as
        # disabled even though the flag was set only once, on the oldest release.
        assert not status.auto_finalization_enabled

        # Dwell and re-check: the version must stay held across subsequent
        # gating-loop ticks, not advance late.
        time.sleep(MANUAL_FINALIZE_HOLD_DWELL_SEC)
        assert self.admin.get_features()["cluster_version"] == held_version

        # The explicit finalize still drives the advance to the head version.
        self._finalize()
        self._wait_for_version_everywhere(head_logical)
        finalized = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_FINALIZED
        )
        assert finalized.active_version == head_logical
        assert finalized.version_after_finalization == head_logical

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_downgrade_before_finalize(self):
        """
        Exercise the core promise of the unfinalized-upgrade feature: after
        upgrading the binaries with auto-finalization disabled, the cluster runs
        unfinalized (giving up post-finalization features) and can be downgraded
        back to the prior release if a problem is found -- repeatedly -- before
        the upgrade is eventually finalized.

        In each state the cluster is perturbed (see _perturb): a fixed set of
        topics is seeded once and re-read after every transition -- and after an
        in-place restart -- to confirm data survives, and on the upgraded
        (unfinalized) binary the v26.2 feature gates are exercised and checked
        for coverage.
        """
        self._start_at_old()
        self._disable_auto_finalization()
        old_release = self.old_release
        old_logical = self.old_logical

        for cycle in range(DOWNGRADE_ROLLBACK_CYCLES):
            self.logger.info(f"rollback cycle {cycle}: upgrade -> perturb -> downgrade")

            # Upgrade every binary to HEAD. With auto-finalization off the active
            # version is held at the old version (READY_TO_FINALIZE), which is
            # what keeps the downgrade available.
            self._restart_at_new(self.redpanda.nodes)
            status = self._wait_for_status_state(
                features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE
            )
            assert self.admin.get_features()["cluster_version"] == old_logical
            assert not status.auto_finalization_enabled

            self._perturb(f"cycle{cycle}-upgraded-unfinalized")

            # Roll back to the old release WITHOUT finalizing, simulating that an
            # issue was found. The cluster must return healthy on the old binary
            # with the active version unchanged.
            self._downgrade_all_to(old_release)
            assert self.admin.get_features()["cluster_version"] == old_logical
            assert all(
                self.admin.get_features(node=n)["node_latest_version"] == old_logical
                for n in self.redpanda.nodes
            ), "not all nodes report the old binary's logical version after downgrade"

            self._perturb(f"cycle{cycle}-downgraded")

        # Final attempt: upgrade once more, and this time complete it by
        # finalizing. The active version advances to the head version.
        self.logger.info("final attempt: upgrade -> finalize")
        self._restart_at_new(self.redpanda.nodes)
        self._wait_for_status_state(features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE)
        self._finalize()
        self._wait_for_version_everywhere(self.new_logical)
        finalized = self._wait_for_status_state(
            features_pb2.FINALIZATION_STATE_FINALIZED
        )
        assert finalized.active_version == self.new_logical
        assert finalized.version_after_finalization == self.new_logical

        # With the upgrade finalized, the v26.2 feature gates have opened:
        # confirm the features actually work.
        self._verify_v26_2_features_working()

    @cluster(
        num_nodes=3,
        log_allow_list=RESTART_LOG_ALLOW_LIST
        + [
            # The old binary aborts via vassert when it detects the cluster
            # advanced past the version it supports. Allow the rejection message
            # plus the generic crash artifacts the intentional abort emits.
            "Incompatible downgrade detected",
            "assert - Backtrace:",
            "Recorded crash reason to crash file",
        ],
    )
    def test_downgrade_rejected_after_finalize(self):
        """
        The flip side of test_downgrade_before_finalize: once the upgrade is
        finalized the active version has advanced to the new version, so the old
        binary can no longer run on the (now finalized) data. Rolling a node back
        must fail at startup with the "Incompatible downgrade detected" guard --
        this is what makes finalization the point of no return.
        """
        self._start_at_old()
        self._disable_auto_finalization()
        old_release = self.old_release

        # Upgrade and this time finalize, advancing the active version past what
        # the old binary supports.
        self._restart_at_new(self.redpanda.nodes)
        self._wait_for_status_state(features_pb2.FINALIZATION_STATE_READY_TO_FINALIZE)
        self._finalize()
        self._wait_for_version_everywhere(self.new_logical)
        self._wait_for_status_state(features_pb2.FINALIZATION_STATE_FINALIZED)

        # Attempt to roll a single node back to the old release. The old binary
        # reads the persisted feature table, sees the cluster advanced past the
        # version it supports, and aborts startup rather than corrupt state.
        victim = self.redpanda.nodes[-1]
        self.redpanda.stop_node(victim)
        self.installer.install([victim], old_release)
        self.redpanda.start_node(victim, expect_fail=True)
        assert self.redpanda.search_log_node(
            victim, "Incompatible downgrade detected"
        ), "old binary should refuse to start after finalization"

        # The data is intact and only the too-old binary was rejected: restoring
        # HEAD lets the node rejoin healthy, leaving a clean cluster for teardown.
        self.redpanda.stop_node(victim, forced=True)
        self.installer.install([victim], RedpandaInstaller.HEAD)
        self.redpanda.start_node(victim)


class ManualFinalizationLicenseTest(RedpandaTest):
    """
    Verifies that disabling `features_auto_finalization` is rejected by the
    cluster config validator when no Enterprise license is in effect.
    """

    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            environment={"__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE": "1"},
        )
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    def test_finalization_requires_enterprise_license(self):
        try:
            self.admin.patch_cluster_config(
                upsert={"features_auto_finalization": False}
            )
        except HTTPError as e:
            # Enterprise restrictions are rejected as 400 with the property
            # name in the response body (validator path), or 403 if the
            # license/auth path catches it first. Accept either.
            assert e.response.status_code in (400, 403), (
                f"Expected 400 or 403, got {e.response.status_code}"
            )
            if e.response.status_code == 400:
                body = e.response.json()
                assert "features_auto_finalization" in body, (
                    f"Expected validator to reject features_auto_finalization, "
                    f"got: {body}"
                )
        else:
            raise RuntimeError(
                "Expected patch_cluster_config to fail without an enterprise license"
            )


class FeatureManagerDecommissionRegressionTest(FeaturesTestBase):
    """
    Regression test for the bug where feature_manager fails to advance
    cluster_version after the last version-blocking node is fully
    decommissioned.

    Mirrors a production-observed scenario: a cluster of v_old nodes is
    upgraded by adding v_high nodes that join because their earliest
    tolerates v_old, then the v_old nodes are decommissioned.
    cluster_version must auto-advance to v_high once the v_old nodes
    leave the members table.

    The bug: feature_manager's background loop is parked on
    _update_wait after the last health-report tick. Health reports
    cease once a node is fully removed, and no leader change is
    guaranteed, so without an explicit wake-up from members_table
    changes the loop never re-evaluates and cluster_version stays
    pinned at v_old indefinitely.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=5, **kwargs)

    def setUp(self):
        super().setUp()
        # We will bootstrap by hand with controlled per-node logical
        # versions. Stop the cluster that the base setUp started and
        # wipe persistent state so the next start is a fresh
        # bootstrap at the synthetic v_old below.
        self.redpanda.stop()
        for node in self.redpanda.nodes:
            self.redpanda.clean_node(node, preserve_current_install=True)

    @cluster(num_nodes=5)
    def test_decommission_advances_cluster_version(self):
        v_high = self.head_latest_logical_version
        v_old = v_high - 1
        assert v_old >= 1, f"v_high={v_high} too low to synthesize a v_old below it"

        old_nodes = self.redpanda.nodes[0:2]
        new_nodes = self.redpanda.nodes[2:5]

        # Step 1: bootstrap a fresh 2-node cluster reporting v_old.
        self.redpanda.set_environment(
            {
                "__REDPANDA_LATEST_LOGICAL_VERSION": f"{v_old}",
                "__REDPANDA_EARLIEST_LOGICAL_VERSION": f"{v_old}",
            }
        )
        self.redpanda.start(old_nodes)
        wait_until(
            lambda: self.admin.get_features(node=old_nodes[0])["cluster_version"]
            == v_old,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"cluster_version did not reach v_old={v_old} after bootstrap",
        )

        # Step 2: add 3 new nodes reporting v_high. Their earliest
        # tolerates v_old so they join the existing cluster.
        self.redpanda.set_environment(
            {
                "__REDPANDA_LATEST_LOGICAL_VERSION": f"{v_high}",
                "__REDPANDA_EARLIEST_LOGICAL_VERSION": f"{v_old}",
            }
        )
        for node in new_nodes:
            self.redpanda.start_node(node)

        # Wait for all five brokers to be present and alive from the
        # leader's perspective. is_alive is computed by the leader's
        # health monitor from received reports, and feature_manager's
        # node callback fires off the same reports — so when this
        # predicate holds, feature_manager has observed v_high from
        # every new node and has had its chance to attempt an advance.
        def cluster_at_steady_state():
            brokers = self.admin.get_brokers(node=new_nodes[0])
            return len(brokers) == 5 and all(b.get("is_alive", False) for b in brokers)

        wait_until(
            cluster_at_steady_state,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="cluster did not reach a 5-broker alive steady state",
        )

        # With v_old members still present, the correct decision is
        # to hold cluster_version at v_old. Assert that's where we
        # are before triggering the decommission.
        assert self.admin.get_features(node=new_nodes[0])["cluster_version"] == v_old, (
            "cluster_version advanced before v_old nodes were decommissioned"
        )

        # Step 3: decommission both v_old nodes via a v_high node,
        # sequentially. raft0 only has two voters in this bootstrap
        # cluster, so the first decommission must complete (replicas
        # rebalanced onto v_high nodes) before the second is safe.
        # Tell the waiter to exclude *both* v_old node ids from
        # progress queries: while one is being removed the other is
        # still a member but is the only remaining v_old node and is
        # busy with raft0/partition rebalancing, so picking it for
        # status queries causes transient 503s in the waiter loop.
        old_ids = [self.redpanda.node_id(n) for n in old_nodes]
        for old_id in old_ids:
            self.admin.decommission_broker(old_id, node=new_nodes[0])
            waiter = NodeDecommissionWaiter(
                self.redpanda,
                old_id,
                self.logger,
                progress_timeout=60,
                decommissioned_node_ids=old_ids,
            )
            waiter.wait_for_removal()

        # Step 4: with both v_old nodes gone, cluster_version must
        # advance to v_high. Pre-fix, feature_manager's loop is parked
        # on _update_wait and no event source wakes it on member
        # removal, so the advance never lands and this times out.
        wait_until(
            lambda: self.admin.get_features(node=new_nodes[0])["cluster_version"]
            == v_high,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=(
                f"cluster_version stuck at v_old={v_old} after decommissioning "
                f"all v_old nodes; expected v_high={v_high}. "
                "feature_manager likely failed to wake on member removal."
            ),
        )
