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
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import LoggingConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda_installer import RedpandaInstaller
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

        first_upgraded = self.redpanda.nodes[0]
        self.logger.info(
            f"Upgrading node {first_upgraded} with an license injected via the environment variable escape hatch expecting it to succeed"
        )
        installer.install([first_upgraded], latest_version)
        self.redpanda.stop_node(first_upgraded)

        if clean_node_before_upgrade:
            self.logger.info(f"Cleaning node {first_upgraded}")
            self.redpanda.remove_local_data(first_upgraded)

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
