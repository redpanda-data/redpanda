# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from abc import ABC, abstractmethod
import os
from typing import Any, Callable, Mapping, Sequence, cast

from ducktape.tests.test import Test, TestContext
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService, RedpandaServiceCloud, SISettings, make_redpanda_mixed_service, make_redpanda_service, CloudStorageType
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.default import DefaultClient
from rptest.util import Scale
from rptest.utils import mode_checks
from rptest.clients.types import TopicSpec
from rptest.services.redpanda_installer import RedpandaInstaller, RedpandaVersion, RedpandaVersionLine, RedpandaVersionTriple
from rptest.clients.rpk import RpkTool


class RedpandaTestBase(ABC, Test):
    # List of topics to be created automatically when the cluster starts. Each
    # topic is defined by an instance of a TopicSpec.
    #
    # TD: seems like this should be an instance property, not a class property?
    topics: Sequence[TopicSpec] = []

    def __init__(self, test_context: TestContext):
        super().__init__(test_context)

        self.scale = Scale(test_context)

    def setUp(self):
        self.__redpanda.start()
        self._create_initial_topics()

    @abstractmethod
    def client(self) -> DefaultClient:
        pass

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
        return mode_checks.is_debug_mode()

    @property
    def ci_mode(self):
        """
        Useful for tests that want to dynamically degrade/disable on low-resource
        developer environments (e.g. laptops) but apply stricter checks in CI.
        """
        return os.environ.get('CI', None) != 'false'

    def _create_initial_topics(self):
        client = KafkaCliTools(self.__redpanda)
        for spec in self.topics:
            self.logger.debug(f"Creating initial topic {spec}")
            client.create_topic(spec)

    # TODO: TD helper to get a typed redpanda but this can be removed by making this
    # class generic on the type of service
    @property
    def __redpanda(self) -> RedpandaService | RedpandaServiceCloud:
        return getattr(self, 'redpanda')


class RedpandaTest(RedpandaTestBase):
    """
    Base class for tests that use the Redpanda service.
    """
    def __init__(self,
                 test_context: TestContext,
                 num_brokers: int | None = None,
                 extra_rp_conf: dict[str, Any] | None = None,
                 si_settings: SISettings | None = None,
                 **kwargs: Any):
        """
        Any trailing keyword arguments are passed through to the
        RedpandaService constructor.
        """
        super().__init__(test_context)

        self.redpanda = make_redpanda_service(test_context,
                                              num_brokers,
                                              extra_rp_conf=extra_rp_conf,
                                              si_settings=si_settings,
                                              **kwargs)
        self._client = DefaultClient(self.redpanda)

    def early_exit_hook(self):
        """
        Hook for `skip_debug_mode` decorator
        """
        self.redpanda.set_skip_if_no_redpanda_log(True)

    @property
    def si_settings(self):
        """Shortcut to self.redpanda.si_settings"""
        return self.redpanda.si_settings

    @property
    def azure_blob_storage(self):
        return self.si_settings.cloud_storage_type == CloudStorageType.ABS

    @property
    def cloud_storage_client(self):
        return self.redpanda._cloud_storage_client

    def client(self):
        return self._client

    def load_version_range(self, initial_version: RedpandaVersion):
        """
        For tests that do upgrades: find all the latest versions from feature branches
        between the initial version and the head version.  Result is a list, inclusive of both initial
        version and head.
        """

        k = 0
        v = self.redpanda._installer.head_version()
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

    def upgrade_through_versions(
            self,
            versions_in: list[RedpandaVersion],
            already_running: bool = False,
            auto_assign_node_id: bool = False,
            mid_upgrade_check: Callable[[Mapping[Any, RedpandaVersion]],
                                        None] = lambda x: None,
            license_required: bool = False):
        """
        Step the cluster through all the versions in `versions`, at each stage
        yielding the version of the cluster.
        """

        # replace all the instances of RedpandaInstaller.HEAD with the head_version
        versions: list[RedpandaVersionTriple | RedpandaVersionLine] = [
            v if v != RedpandaInstaller.HEAD else
            self.redpanda._installer.head_version() for v in versions_in
        ]

        def install_next():
            v = versions.pop(0)
            self.logger.info(f"Installing version {v}...")
            self.redpanda._installer.install(self.redpanda.nodes, v)
            return v

        def logical_version_stable(old_logical_version: int):
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
            old_logical_version:int =  \
                self.redpanda._admin.get_features()['cluster_version']
        else:
            old_logical_version = -1

        # If we are starting from scratch, then install the initial version and yield
        if not already_running:
            current_version = install_next()
            old_version = current_version
            self.logger.info("Installed initial version, calling setUp...")
            RedpandaTest.setUp(self)
            yield current_version
        else:
            old_version = self.redpanda.get_version_int_tuple(
                self.redpanda.nodes[0])

        # Install subsequent versions
        while len(versions) != 0:
            current_version = install_next()

            use_maintenance_mode = current_version[0:2] >= (22, 2)
            if not use_maintenance_mode:
                self.logger.info(
                    f"Upgrading to {current_version}, skipping maintenance mode"
                )

            # Installing a license is required for version upgrades with enterprise features
            if license_required:
                self.redpanda.install_license()

            # restarts the nodes in two batches, to run mid_upgrade_check with a mixed-version cluster
            rp_nodes: list[Any] = self.redpanda.nodes
            canary_nodes, rest_nodes = \
                    rp_nodes[0:len(rp_nodes) //  2], rp_nodes[len(rp_nodes) // 2:]
            self.redpanda.rolling_restart_nodes(
                canary_nodes,
                start_timeout=90,
                stop_timeout=90,
                use_maintenance_mode=use_maintenance_mode,
                auto_assign_node_id=auto_assign_node_id)
            mid_upgrade_check({n: current_version
                               for n in canary_nodes}
                              | {n: old_version
                                 for n in rest_nodes})
            self.redpanda.rolling_restart_nodes(
                rest_nodes,
                start_timeout=90,
                stop_timeout=90,
                use_maintenance_mode=use_maintenance_mode,
                auto_assign_node_id=auto_assign_node_id)

            if current_version[0:2] == (22, 1):
                # Special case: the version in which we adopted the __consumer_offsets topic
                self.logger.info(
                    "Upgraded to 22.1.x, waiting for consumer_offsets...")
                await_consumer_offsets()

            if current_version[0:2] > old_version[0:2]:
                # Do this operation only when upgrading to a new major version:
                # After doing a rolling restart with the new version, let the cluster's
                # logical version and associated feature flag state stabilize.  This avoids
                # upgrading "too fast" such that the cluster thinks we skipped a version.
                self.redpanda.wait_until(
                    lambda: logical_version_stable(old_logical_version),
                    timeout_sec=30,
                    backoff_sec=1)

            old_version = current_version

            # For use next time around the loop
            old_logical_version = self.redpanda._admin.get_features(
            )['cluster_version']

            # We are fully upgraded, yield to whatever the test wants to do in this version
            yield current_version


class RedpandaMixedTest(RedpandaTestBase):
    """
    Base class for tests which can run either against vanilla infrastructure or
    against the cloud.
    """
    def __init__(self,
                 test_context: TestContext,
                 *,
                 min_brokers: int | None = None):
        """
        :param min_brokers: the minimum number of brokers the test requires. The
        test must be OK with more brokers than the given number (which will occur
        in cloud tests, as the number of brokers is already set at cloud cluster
        creation time). None indicates that the test does not care how many brokers
        are created (there will always be at least 1!).
        """
        super().__init__(test_context=test_context)
        self.redpanda = make_redpanda_mixed_service(test_context,
                                                    min_brokers=min_brokers)

        self._client = DefaultClient(self.redpanda)

    def setup(self):
        super().setup()
        if (cloud := self.as_cloud()):
            wait_until(lambda: cloud.cluster_healthy(),
                       timeout_sec=20,
                       backoff_sec=5,
                       err_msg='cluster unhealthy before start of test')

    def as_cloud(self):
        """A convenience method which returns self.redpanda if it is a RedpandaServiceCloud
        or None otherwise."""
        return self.redpanda if isinstance(self.redpanda,
                                           RedpandaServiceCloud) else None

    def as_noncloud(self):
        """A convenience method which returns self.redpanda if it is a RedpandaService
        or None otherwise."""
        return self.redpanda if isinstance(self.redpanda,
                                           RedpandaService) else None

    def client(self):
        return self._client

    def tearDown(self):
        if (cloud := self.as_cloud()):
            cloud.clean_cluster()
