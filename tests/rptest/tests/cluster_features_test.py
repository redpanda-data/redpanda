# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from ducktape.errors import TimeoutError as DucktapeTimeoutError

from ducktape.utils.util import wait_until


class FeaturesTestBase(RedpandaTest):
    """
    Test cases defined in this parent class are executed as part
    of subclasses that define node count below.
    """
    def _get_features_map(self, feature_response=None):
        if feature_response is None:
            feature_response = self.admin.get_features()
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
        assert features_response['cluster_version'] == 3

        assert self._get_features_map(
            features_response)['central_config']['state'] == 'active'

        return features_response


class FeaturesMultiNodeTest(FeaturesTestBase):
    """
    Multi-node variant of tests is the 'normal' execution path for feature manager.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    def test_get_features(self):
        self._assert_default_features()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        """
        Verify that on updating to a new logical version, the cluster
        version does not increment until all nodes are up to date.
        """
        initial_version = self.admin.get_features()['cluster_version']

        new_version = initial_version + 1
        self.logger.info(
            f"Simulating upgrade from version {initial_version} to version {new_version}"
        )

        # Modified environment variables apply to processes restarted from this point onwards
        self.redpanda.set_environment(
            {'__REDPANDA_LOGICAL_VERSION': f'{new_version}'})

        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        time.sleep(5)  # Give it a chance to update
        assert initial_version == self.admin.get_features()['cluster_version']

        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        time.sleep(5)  # Give it a chance to update
        assert initial_version == self.admin.get_features()['cluster_version']

        self.redpanda.restart_nodes([self.redpanda.nodes[2]])
        wait_until(lambda: initial_version == self.admin.get_features()[
            'cluster_version'],
                   timeout_sec=5,
                   backoff_sec=1)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rollback(self):
        """
        Verify that on a rollback before updating all nodes, the cluster
        version does not increment.
        """
        initial_version = self.admin.get_features()['cluster_version']

        new_version = initial_version + 1
        self.logger.info(
            f"Simulating upgrade from version {initial_version} to version {new_version}"
        )

        # Modified environment variables apply to processes restarted from this point onwards
        self.redpanda.set_environment(
            {'__REDPANDA_LOGICAL_VERSION': f'{new_version}'})

        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        time.sleep(5)  # Give it a chance to update
        assert initial_version == self.admin.get_features()['cluster_version']

        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        time.sleep(5)  # Give it a chance to update
        assert initial_version == self.admin.get_features()['cluster_version']

        self.logger.info(f"Simulating rollback to version {initial_version}")
        self.redpanda.set_environment(
            {'__REDPANDA_LOGICAL_VERSION': f'{initial_version}'})

        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        self.redpanda.restart_nodes([self.redpanda.nodes[1]])
        time.sleep(5)  # Give it a chance to update
        assert initial_version == self.admin.get_features()['cluster_version']

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
        wait_until(lambda: feature_alpha_version == self.admin.get_features()[
            'cluster_version'],
                   timeout_sec=15,
                   backoff_sec=1)

        # Feature should become available now that version increased.  It should NOT
        # become active, because it has an explicit_only policy for activation.
        assert self._get_features_map(
        )[feature_alpha_name]['state'] == 'available'

        # Disable the feature, see that it enters the expected state
        self.admin.put_feature(feature_alpha_name, {"state": "disabled"})
        wait_until(lambda: self._get_features_map()[feature_alpha_name][
            'state'] == 'disabled',
                   timeout_sec=5,
                   backoff_sec=1)
        state = self._get_features_map()[feature_alpha_name]
        assert state['state'] == 'disabled'
        assert state['was_active'] == False

        # Write to admin API to enable the feature
        self.admin.put_feature(feature_alpha_name, {"state": "active"})

        # This is an async check because propagation of feature_table is async
        wait_until(lambda: self._get_features_map()[feature_alpha_name][
            'state'] == 'active',
                   timeout_sec=5,
                   backoff_sec=1)

        # Disable the feature, see that it enters the expected state
        self.admin.put_feature(feature_alpha_name, {"state": "disabled"})
        wait_until(lambda: self._get_features_map()[feature_alpha_name][
            'state'] == 'disabled',
                   timeout_sec=5,
                   backoff_sec=1)
        state = self._get_features_map()[feature_alpha_name]
        assert state['state'] == 'disabled'
        assert state['was_active'] == True


class FeaturesSingleNodeTest(FeaturesTestBase):
    """
    A single node variant to make sure feature_manager does its job in the absence
    of any health reports.
    """
    def __init__(self, *args, **kwargs):
        # Skip immediate parent constructor
        super().__init__(*args, num_brokers=1, **kwargs)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    def test_get_features(self):
        self._assert_default_features()

    @cluster(num_nodes=1, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        """
        Verify that on updating to a new logical version, the cluster
        version does not increment until all nodes are up to date.
        """
        initial_version = self.admin.get_features()['cluster_version']

        new_version = initial_version + 1
        self.logger.info(
            f"Simulating upgrade from version {initial_version} to version {new_version}"
        )

        # Modified environment variables apply to processes restarted from this point onwards
        self.redpanda.set_environment(
            {'__REDPANDA_LOGICAL_VERSION': f'{new_version}'})

        # Restart nodes one by one.  Version shouldn't increment until all three are done.
        self.redpanda.restart_nodes([self.redpanda.nodes[0]])
        wait_until(lambda: new_version == self.admin.get_features()[
            'cluster_version'],
                   timeout_sec=5,
                   backoff_sec=1)


class FeaturesNodeJoinTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=4, **kwargs)

        self.admin = Admin(self.redpanda)

    def setUp(self):
        # We will start nodes by hand during test.
        pass

    @cluster(num_nodes=4)
    def test_old_node_join(self):
        """
        Verify that when an old-versioned node tries to join a newer-versioned cluster,
        it is rejected.
        """

        initial_version = 768

        self.redpanda.set_environment(
            {'__REDPANDA_LOGICAL_VERSION': f"{initial_version}"})

        # Start first three nodes
        self.redpanda.start(self.redpanda.nodes[0:-1])

        assert initial_version == self.admin.get_features()['cluster_version']

        # Bring up the fourth node reporting an old logical version
        old_node = self.redpanda.nodes[-1]
        old_version = initial_version - 1
        self.redpanda.set_environment(
            {'__REDPANDA_LOGICAL_VERSION': f"{old_version}"})

        try:
            self.redpanda.start_node(old_node)
        except DucktapeTimeoutError:
            pass
        else:
            raise RuntimeError(
                f"Node {old_node} joined cluster, but should have been rejected"
            )

        # Restart it with a sufficiently recent version and join should succeed
        self.redpanda.set_environment(
            {'__REDPANDA_LOGICAL_VERSION': f"{initial_version}"})
        self.redpanda.restart_nodes([old_node])
        wait_until(lambda: self.redpanda.registered(old_node),
                   timeout_sec=10,
                   backoff_sec=1)
