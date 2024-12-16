# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from ducktape.mark import matrix

from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.admin import Admin
from rptest.services.redpanda import LoggingConfig, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.utils.mode_checks import skip_fips_mode
from rptest.utils.rpenv import sample_license


class LicenseEnforcementTest(RedpandaTest):
    # Disable log checks because it is not very useful when we expect to crash nodes on license upgrades
    LOG_ALLOW_LIST = [re.compile(".*")]

    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=5,
                         log_config=LoggingConfig('info',
                                                  logger_levels={
                                                      'cluster': 'debug',
                                                      'features': 'debug'
                                                  }),
                         **kwargs)

        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        # start the nodes manually
        pass

    @skip_fips_mode
    @cluster(num_nodes=5, log_allow_list=LOG_ALLOW_LIST)
    @matrix(
        clean_node_before_recovery=[False, True],
        clean_node_after_recovery=[False, True],
    )
    def test_license_enforcement(self, clean_node_before_recovery,
                                 clean_node_after_recovery):
        installer = self.redpanda._installer
        prev_version = installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        latest_version = installer.head_version()
        self.logger.info(
            f"Testing with versions: {prev_version=} {latest_version=}")

        self.logger.info(f"Starting all nodes with version: {prev_version}")
        installer.install(self.redpanda.nodes, prev_version)
        self.redpanda.start(nodes=self.redpanda.nodes,
                            omit_seeds_on_idx_one=False)

        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")

        self.logger.info(f"Enabling an enterprise feature")
        self.redpanda.set_cluster_config(
            {"partition_autobalancing_mode": "continuous"})

        self.logger.info(
            "Disabling the trial license to simulate that the license expired")
        self.redpanda.set_environment(
            {'__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE': True})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")

        first_upgraded = self.redpanda.nodes[0]
        self.logger.info(
            f"Upgrading node {first_upgraded} expecting it to crash")
        installer.install([first_upgraded], latest_version)
        self.redpanda.stop_node(first_upgraded)

        if clean_node_before_recovery:
            self.logger.info(f"Cleaning node {first_upgraded}")
            self.redpanda.remove_local_data(first_upgraded)

        self.redpanda.start_node(first_upgraded,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False,
                                 expect_fail=True)

        self.logger.info(
            "Recovering the node by downgrading and installing a license")
        installer.install([first_upgraded], prev_version)
        self.redpanda.start_node(first_upgraded,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False)
        self.redpanda.install_license()

        self.logger.info("Retrying the upgrade after the license is set")
        self.redpanda.stop_node(first_upgraded)

        if clean_node_after_recovery:
            self.logger.info(f"Cleaning node {first_upgraded}")
            self.redpanda.remove_local_data(first_upgraded)

        installer.install([first_upgraded], latest_version)
        self.redpanda.start_node(first_upgraded,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False)

    @skip_fips_mode
    @cluster(num_nodes=5, log_allow_list=LOG_ALLOW_LIST)
    @matrix(clean_node_before_upgrade=[False, True])
    def test_escape_hatch_license_variable(self, clean_node_before_upgrade):
        installer = self.redpanda._installer
        prev_version = installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        latest_version = installer.head_version()
        self.logger.info(
            f"Testing with versions: {prev_version=} {latest_version=}")

        self.logger.info(f"Starting all nodes with version: {prev_version}")
        installer.install(self.redpanda.nodes, prev_version)
        self.redpanda.start(nodes=self.redpanda.nodes,
                            omit_seeds_on_idx_one=False)

        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")

        self.logger.info(f"Enabling an enterprise feature")
        self.redpanda.set_cluster_config(
            {"partition_autobalancing_mode": "continuous"})

        self.logger.info(
            "Disabling the trial license to simulate that the license expired")
        self.redpanda.set_environment(
            {'__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE': True})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")

        first_upgraded = self.redpanda.nodes[0]
        first_upgraded_id = self.redpanda.node_id(first_upgraded)
        self.logger.info(
            f"Upgrading node {first_upgraded.name} with an license injected via the environment variable escape hatch expecting it to succeed"
        )
        installer.install([first_upgraded], latest_version)
        self.redpanda.stop_node(first_upgraded)

        if clean_node_before_upgrade:
            self.logger.info(f"Cleaning node {first_upgraded.name}")
            self.redpanda.remove_local_data(first_upgraded)
            Admin(self.redpanda).decommission_broker(first_upgraded_id)

        license = sample_license(assert_exists=True)
        self.redpanda.set_environment(
            {'REDPANDA_FALLBACK_ENTERPRISE_LICENSE': license})

        self.redpanda.start_node(first_upgraded,
                                 auto_assign_node_id=True,
                                 omit_seeds_on_idx_one=False)

        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")

    @cluster(num_nodes=5)
    @matrix(root_driven_bootstrap=[False, True])
    def test_enterprise_cluster_bootstrap(self, root_driven_bootstrap):
        self.logger.info(
            "Bootstrapping a cluster with enterprise features but without a license"
        )
        self.redpanda.add_extra_rp_conf(
            {"partition_autobalancing_mode": "continuous"})

        if root_driven_bootstrap:
            self.logger.info("Using root-driven bootstrap")
            for node in self.redpanda.nodes:
                self.redpanda.set_extra_node_conf(
                    node, {"empty_seed_starts_cluster": True})
            self.redpanda.set_seed_servers([self.redpanda.nodes[0]])
            self.redpanda.start(omit_seeds_on_idx_one=True)
        else:
            self.logger.info("Using non-root-driven bootstrap")
            self.redpanda.set_seed_servers(self.redpanda.nodes)
            self.redpanda.start(omit_seeds_on_idx_one=False)

        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")


class LicenseEnforcementPermittedTopicParams(RedpandaTest):
    """
    Tests that validate that topics properties whose controlling cluster config
    is disabled do not cause any issues in regards to license enforcement.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        pass

    @cluster(num_nodes=3)
    @matrix(enable_cloud_storage=[False, True])
    def test_cloud_storage_topic_params(self, enable_cloud_storage):
        """
        This test verifies that if a license isn't installed and `cloud_storage_enabled`
        is set to `False`, then topics may be created with TS settingss set to true, e.g.
        `redpanda.remote.write`.
        """
        if enable_cloud_storage:
            si_settings = SISettings(self.test_context)
            self.redpanda.set_si_settings(si_settings)

        super().setUp()

        self.redpanda.set_environment(
            {'__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE': True})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")

        try:
            self.rpk.create_topic("test",
                                  config={"redpanda.remote.write": "true"})
            assert not enable_cloud_storage, "Should have failed to create topic with redpanda.remote.write set and cloud_storage_enabled set to True"
        except RpkException as e:
            assert enable_cloud_storage, f"Should not have failed to create topic with redpanda.remote.write set and cloud_storage_enabled set to False: {e}"

    @cluster(num_nodes=3)
    def test_upgrade_with_topic_configs(self):
        """
        This test verifies that if a license isn't installed and `cloud_storage_enabled`
        is set to `False` and topics exist with tiered storage capabilities, the upgrade
        will still succeed
        """
        installer = self.redpanda._installer
        prev_version = installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        latest_version = installer.head_version()
        self.logger.info(
            f"Testing with versions: {prev_version=} {latest_version=}")

        self.logger.info(f"Starting all nodes with version: {prev_version}")
        installer.install(self.redpanda.nodes, prev_version)
        self.redpanda.start(nodes=self.redpanda.nodes,
                            omit_seeds_on_idx_one=False)
        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")
        self.logger.debug(
            "Creating a topic with redpanda.remote.write set to true")
        self.rpk.create_topic("test", config={"redpanda.remote.write": "true"})
        self.logger.info(
            "Disabling the trial license to simulate that the license expired")
        self.redpanda.set_environment(
            {'__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE': True})
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=60,
                                 backoff_sec=1,
                                 err_msg="The cluster hasn't stabilized")

        installer.install(self.redpanda.nodes, latest_version)
        self.redpanda.start(nodes=self.redpanda.nodes,
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
