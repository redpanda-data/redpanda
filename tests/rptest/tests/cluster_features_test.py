# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from rptest.utils.rpenv import sample_license
from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions

from ducktape.errors import TimeoutError as DucktapeTimeoutError
from ducktape.utils.util import wait_until
from rptest.util import wait_until_result
from ducktape.mark import ok_to_fail


class FeaturesTestBase(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer

    def setUp(self):
        super().setUp()
        self.head_logical_version = self.admin.get_features(
        )["cluster_version"]
        self.previous_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)

    """
    Test cases defined in this parent class are executed as part
    of subclasses that define node count below.
    """

    def _get_features_map(self, feature_response=None, node=None):
        if feature_response is None:
            feature_response = self.admin.get_features(node=node)
        return dict((f['name'], f) for f in feature_response['features'])

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
        assert initial_version == self.head_logical_version, \
            f"Version mismatch: {initial_version} vs {self.head_logical_version}"

        assert self._get_features_map(
            features_response)['central_config']['state'] == 'active'

        return features_response

    def _wait_for_version_everywhere(self, target_version):
        """
        Apply a GET check to all nodes, for writes that are expected to
        propagate via controller log
        """
        def check():
            for node in self.redpanda.nodes:
                node_version = self.admin.get_features(
                    node=node)['cluster_version']
                if node_version != target_version:
                    return False

            return True

        # Version propagation is a little slower than feature write propagation, because
        # it relies on periodic health messages
        wait_until(check, timeout_sec=20, backoff_sec=1)


class FeaturesMultiNodeTest(FeaturesTestBase):
    """
    Multi-node variant of tests is the 'normal' execution path for feature manager.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @cluster(num_nodes=3)
    def test_get_features(self):
        self._assert_default_features()

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

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_explicit_activation(self):
        """
        Using a dummy feature, verify its progression through unavailable->available->active
        """

        # Parameters of the compiled-in test feature
        feature_alpha_version = 2001
        feature_alpha_name = "__test_alpha"

        initial_version = self.admin.get_features()['cluster_version']
        assert (initial_version < feature_alpha_version)
        # Initially, before setting the magic environment variable, dummy test features
        # should be hidden
        assert feature_alpha_name not in self._get_features_map().keys()

        self.redpanda.set_environment({'__REDPANDA_TEST_FEATURES': "ON"})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        assert self._get_features_map(
        )[feature_alpha_name]['state'] == 'unavailable'

        # Version is too low, feature should be unavailable
        assert initial_version == self.admin.get_features()['cluster_version']

        self.redpanda.set_environment({
            '__REDPANDA_TEST_FEATURES':
            "ON",
            '__REDPANDA_LOGICAL_VERSION':
            f'{feature_alpha_version}'
        })
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for version to increment: this is a little slow because we wait
        # for health monitor structures to time out in order to propagate the
        # updated version
        self._wait_for_version_everywhere(feature_alpha_version)

        # Feature should become available now that version increased.  It should NOT
        # become active, because it has an explicit_only policy for activation.
        self._wait_for_feature_everywhere(
            lambda fm: fm[feature_alpha_name]['state'] == 'available')

        # Disable the feature, see that it enters the expected state
        self.admin.put_feature(feature_alpha_name, {"state": "disabled"})
        self._wait_for_feature_everywhere(
            lambda fm: fm[feature_alpha_name]['state'] == 'disabled')

        state = self._get_features_map()[feature_alpha_name]
        assert state['state'] == 'disabled'
        assert state['was_active'] == False

        # Write to admin API to enable the feature
        self.admin.put_feature(feature_alpha_name, {"state": "active"})

        # This is an async check because propagation of feature_table is async
        self._wait_for_feature_everywhere(
            lambda fm: fm[feature_alpha_name]['state'] == 'active')

        # Disable the feature, see that it enters the expected state
        self.admin.put_feature(feature_alpha_name, {"state": "disabled"})
        self._wait_for_feature_everywhere(
            lambda fm: fm[feature_alpha_name]['state'] == 'disabled')

        state = self._get_features_map()[feature_alpha_name]
        assert state['state'] == 'disabled'
        assert state['was_active'] == True

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_license_upload_and_query(self):
        """
        Test uploading and retrieval of license
        """
        license = sample_license()
        if license is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return
        expected_license_contents = {
            'expires':
            4813252273,
            'format_version':
            0,
            'org':
            'redpanda-testing',
            'type':
            'enterprise',
            'sha256':
            '2730125070a934ca1067ed073d7159acc9975dc61015892308aae186f7455daf'
        }

        assert self.admin.put_license(license).status_code == 200

        def obtain_license():
            lic = self.admin.get_license()
            return (lic is not None and lic['loaded'] is True, lic)

        resp = wait_until_result(obtain_license, timeout_sec=5, backoff_sec=1)
        assert resp['license'] is not None
        assert expected_license_contents == resp['license'], resp['license']


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

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/8662
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        """
        Verify that on updating to a new logical version, the cluster
        version does not increment until all nodes are up to date.
        """
        initial_version = self.admin.get_features()['cluster_version']
        assert initial_version < self.head_logical_version, \
            f"downgraded logical version {initial_version}"

        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        _ = wait_for_num_versions(self.redpanda, 2)
        assert initial_version == self.admin.get_features()['cluster_version']

        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        # Even after waiting a bit, the logical version shouldn't change.
        time.sleep(5)
        assert initial_version == self.admin.get_features()['cluster_version']

        self.redpanda.restart_nodes([self.redpanda.nodes[2]])

        # Node logical versions are transmitted as part of health messages, so we may
        # have to wait for the next health tick (health_monitor_tick_interval=10s) before
        # the controller leader fetches health from the last restarted peer.
        self._wait_for_version_everywhere(self.head_logical_version)

        # Check that initial version and current version are properly reflected
        # across all nodes.
        def complete():
            for n in self.redpanda.nodes:
                features = self.admin.get_features(node=n)
                if features['cluster_version'] != self.head_logical_version \
                        or features['original_cluster_version'] != initial_version:
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
        initial_version = self.admin.get_features()['cluster_version']
        assert initial_version < self.head_logical_version, \
            f"downgraded logical version {initial_version}"

        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        _ = wait_for_num_versions(self.redpanda, 2)
        # Even after waiting a bit, the logical version shouldn't change.
        time.sleep(5)
        assert initial_version == self.admin.get_features()['cluster_version']

        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        time.sleep(5)
        assert initial_version == self.admin.get_features()['cluster_version']

        self.installer.install(self.redpanda.nodes, self.previous_version)
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        _ = wait_for_num_versions(self.redpanda, 1)
        assert initial_version == self.admin.get_features()['cluster_version']


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
        initial_version = self.admin.get_features()['cluster_version']
        assert initial_version < self.head_logical_version, \
            f"downgraded logical version {initial_version}"

        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.installer.install([self.redpanda.nodes[0]],
                               RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        wait_until(lambda: self.head_logical_version == self.admin.
                   get_features()['cluster_version'],
                   timeout_sec=5,
                   backoff_sec=1)


OLD_NODE_JOIN_LOG_ALLOW_LIST = [
    # We expect startup failure when an old node joins, so we allow the corresponding
    # error message.
    r'Failure during startup: seastar::abort_requested_exception \(abort requested\)'
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
        it is rejected.
        """

        # Pick a node to roleplay an old version of redpanda
        old_node = self.redpanda.nodes[-1]
        self.installer.install([old_node], (22, 1))

        # Start first three nodes
        self.redpanda.start(self.redpanda.nodes[0:-1])

        # Explicit clean because it's not included in the default
        # one during start()
        self.redpanda.clean_node(old_node, preserve_current_install=True)

        initial_version = self.admin.get_features()['cluster_version']
        assert initial_version == self.head_logical_version, \
            f"Version mismatch: {initial_version} vs {self.head_logical_version}"

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
        wait_until(lambda: self.redpanda.registered(old_node),
                   timeout_sec=30,
                   backoff_sec=1)
