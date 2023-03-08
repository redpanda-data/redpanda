# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from typing import Sequence

from ducktape.tests.test import Test
from rptest.services.redpanda import RedpandaService, CloudStorageType
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.default import DefaultClient
from rptest.util import Scale
from rptest.clients.types import TopicSpec
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.clients.rpk import RpkTool


class RedpandaTest(Test):
    """
    Base class for tests that use the Redpanda service.
    """

    # List of topics to be created automatically when the cluster starts. Each
    # topic is defined by an instance of a TopicSpec.
    topics: Sequence[TopicSpec] = []

    def __init__(self,
                 test_context,
                 num_brokers=None,
                 extra_rp_conf=dict(),
                 si_settings=None,
                 **kwargs):
        """
        Any trailing keyword arguments are passed through to the
        RedpandaService constructor.
        """
        super(RedpandaTest, self).__init__(test_context)
        self.scale = Scale(test_context)
        self.si_settings = si_settings

        if num_brokers is None:
            # Default to a 3 node cluster if sufficient nodes are available, else
            # a single node cluster.  This is just a default: tests are welcome
            # to override constructor to pass an explicit size.  This logic makes
            # it convenient to mix 3 node and 1 node cases in the same class, by
            # just modifying the @cluster node count per test.
            if test_context.cluster.available().size() >= 3:
                num_brokers = 3
            else:
                num_brokers = 1

        self.redpanda = RedpandaService(test_context,
                                        num_brokers,
                                        extra_rp_conf=extra_rp_conf,
                                        si_settings=self.si_settings,
                                        **kwargs)
        self._client = DefaultClient(self.redpanda)

    def early_exit_hook(self):
        """
        Hook for `skip_debug_mode` decorator
        """
        self.redpanda.set_skip_if_no_redpanda_log(True)

    @property
    def topic(self):
        """
        Return the name of the auto-created initial topic. Accessing this
        property requires exactly one initial topic be configured.
        """
        assert len(self.topics) == 1
        return self.topics[0].name

    @property
    def debug_mode(self):
        """
        Useful for tests that want to change behaviour when running on
        the much slower debug builds of redpanda, which generally cannot
        keep up with significant quantities of data or partition counts.
        """
        return os.environ.get('BUILD_TYPE', None) == 'debug'

    @property
    def ci_mode(self):
        """
        Useful for tests that want to dynamically degrade/disable on low-resource
        developer environments (e.g. laptops) but apply stricter checks in CI.
        """
        return os.environ.get('CI', None) != 'false'

    @property
    def azure_blob_storage(self):
        return self.si_settings.cloud_storage_type == CloudStorageType.ABS

    @property
    def cloud_storage_client(self):
        return self.redpanda.cloud_storage_client

    def setUp(self):
        self.redpanda.start()
        self._create_initial_topics()

    def client(self):
        return self._client

    def _create_initial_topics(self):
        config = self.redpanda.security_config()
        user = config.get("sasl_plain_username")
        passwd = config.get("sasl_plain_password")
        client = KafkaCliTools(self.redpanda, user=user, passwd=passwd)
        for spec in self.topics:
            self.logger.debug(f"Creating initial topic {spec}")
            client.create_topic(spec)

    def load_version_range(self, initial_version):
        """
        For tests that do upgrades: find all the latest versions from feature branches
        between the initial version and the head version.  Result is a list, inclusive of both initial
        version and head.
        """

        k = 0
        v = RedpandaInstaller.HEAD
        versions = [v]
        while (v[0], v[1]) != initial_version[0:2]:
            k += 1

            v = self.redpanda._installer.highest_from_prior_feature_version(v)
            versions.insert(0, v)

            # Protect against infinite loop if something is wrong with our version finding
            if k > 100:
                raise RuntimeError(
                    f"Failed to hit expected oldest version, v={v}")

        return versions

    def upgrade_through_versions(self, versions, already_running=False):
        """
        Step the cluster through all the versions in `versions`, at each stage
        yielding the version of the cluster.
        """
        def install_next():
            v = versions.pop(0)
            self.logger.info(f"Installing version {v}...")
            self.redpanda._installer.install(self.redpanda.nodes, v)
            return v

        def logical_version_stable(old_logical_version):
            """Assuming all nodes have been updated to a particular version,
            check that the cluster's active version has advanced to match the
            logical version of the node we are talking to.
            """
            for node in self.redpanda.nodes:
                features = self.redpanda._admin.get_features(node=node)

                if 'node_latest_version' in features:
                    # Only Redpanda >= v23.2 has this field
                    if features['cluster_version'] != features[
                            'node_latest_version']:
                        # The cluster logical version has not yet updated
                        return False
                    else:
                        self.logger.debug(
                            f"Accepting node {node.name} active version {features['cluster_version']}, it is equal to highest version"
                        )

                else:
                    # Older feature API just tells us the cluster version, we compare
                    # it to the logical version pre-upgrade
                    if features['cluster_version'] <= old_logical_version:
                        return False
                    else:
                        self.logger.debug(
                            f"Accepting node {node.name} active version {features['cluster_version']}, it is > {old_logical_version}"
                        )

                if any(f['state'] == 'preparing'
                       for f in features['features']):
                    # One or more features is still in preparing state.
                    return False

            return True

        def await_consumer_offsets():
            rpk = RpkTool(self.redpanda)

            def _consumer_offsets_present():
                try:
                    rpk.describe_topic("__consumer_offsets")
                except Exception as e:
                    if "Topic not found" in str(e):
                        return False
                return True

            self.redpanda.wait_until(_consumer_offsets_present,
                                     timeout_sec=90,
                                     backoff_sec=3)

        if already_running:
            old_logical_version = self.redpanda._admin.get_features(
            )['cluster_version']
        else:
            old_logical_version = -1

        # If we are starting from scratch, then install the initial version and yield
        if not already_running:
            current_version = install_next()
            self.logger.info("Installed initial version, calling setUp...")
            RedpandaTest.setUp(self)
            yield current_version

        # Install subsequent versions
        while len(versions) != 0:
            current_version = install_next()

            use_maintenance_mode = current_version[0:2] >= (
                22, 2) if current_version != RedpandaInstaller.HEAD else True
            if not use_maintenance_mode:
                self.logger.info(
                    f"Upgrading to {current_version}, skipping maintenance mode"
                )

            self.redpanda._installer.install(self.redpanda.nodes,
                                             current_version)
            self.redpanda.rolling_restart_nodes(
                self.redpanda.nodes,
                start_timeout=90,
                stop_timeout=90,
                use_maintenance_mode=use_maintenance_mode)

            if current_version[0:2] == (22, 1):
                # Special case: the version in which we adopted the __consumer_offsets topic
                self.logger.info(
                    "Upgraded to 22.1.x, waiting for consumer_offsets...")
                await_consumer_offsets()

            # After doing a rolling restart with the new version, let the cluster's
            # logical version and associated feature flag state stabilize.  This avoids
            # upgrading "too fast" such that the cluster thinks we skipped a version.
            self.redpanda.wait_until(
                lambda: logical_version_stable(old_logical_version),
                timeout_sec=30,
                backoff_sec=1)

            # For use next time around the loop
            old_logical_version = self.redpanda._admin.get_features(
            )['cluster_version']

            # We are fully upgraded, yield to whatever the test wants to do in this version
            yield current_version
