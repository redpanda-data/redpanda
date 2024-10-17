# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from time import sleep

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint, MetricSamples, RedpandaServiceBase
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import in_fips_environment


class RedpandaStartupTest(RedpandaTest):
    """
    Tests that Redpanda starts within 10 seconds
    """
    def __init__(self, test_context):
        super(RedpandaStartupTest, self).__init__(test_context=test_context,
                                                  node_ready_timeout_s=10)

    @cluster(num_nodes=3)
    def test_startup(self):
        pass


class RedpandaFIPSStartupTestBase(RedpandaTest):
    @staticmethod
    def fips_mode_to_str(fips_mode: RedpandaServiceBase.FIPSMode) -> str:
        if fips_mode == RedpandaServiceBase.FIPSMode.disabled:
            return "disabled"
        elif fips_mode == RedpandaServiceBase.FIPSMode.enabled:
            return "enabled"
        elif fips_mode == RedpandaServiceBase.FIPSMode.permissive:
            return "permissive"
        else:
            assert False, f"Unknown FIPS Mode; {fips_mode}"

    def __init__(self,
                 test_context,
                 fips_mode: RedpandaServiceBase.FIPSMode = RedpandaServiceBase.
                 FIPSMode.permissive):
        super(RedpandaFIPSStartupTestBase,
              self).__init__(test_context=test_context)

        for node in self.redpanda.nodes:
            self.redpanda.set_extra_node_conf(
                node, {
                    "fips_mode":
                    self.fips_mode_to_str(fips_mode),
                    "openssl_config_file":
                    self.redpanda.get_openssl_config_file_path(),
                    "openssl_module_directory":
                    self.redpanda.get_openssl_modules_directory()
                })


class RedpandaFIPSStartupTest(RedpandaFIPSStartupTestBase):
    """
    Tests that Redpanda can start up in FIPS permissive mode
    """
    def __init__(self, test_context):
        super(RedpandaFIPSStartupTest,
              self).__init__(test_context=test_context)

    @cluster(num_nodes=3)
    def test_startup(self):
        """
        This test will validate that Redpanda can come up in permissive mode
        and that the proper warning messages are printed
        """
        fips_enabled_file = '/proc/sys/crypto/fips_enabled'
        file_does_not_exist_log = f"File '{fips_enabled_file}' does not exist."
        file_not_one_log = f"File '{fips_enabled_file}' not reporting '1'"
        if in_fips_environment():
            # Exception to the test here - if we're in a FIPS environment, no log messages should appear
            self.logger.debug("In FIPS environment, no log expected")
            assert not self.redpanda.search_log_all(file_does_not_exist_log)
            assert not self.redpanda.search_log_all(file_not_one_log)
        elif not os.path.isfile(fips_enabled_file):
            self.logger.debug(
                f"Not in FIPS environment and '{fips_enabled_file}' doesn't exist"
            )
            assert self.redpanda.search_log_all(file_does_not_exist_log)
        else:
            self.logger.debug(
                f"Not in FIPS environment and '{fips_enabled_file}' file exists"
            )
            assert self.redpanda.search_log_all(file_not_one_log)

        def check_fips_mode_metric(
                metrics_name: str, metrics_endpoint: MetricsEndpoint,
                expected_mode: RedpandaServiceBase.FIPSMode):
            metrics = self.redpanda.metrics_sample(
                sample_pattern=metrics_name, metrics_endpoint=metrics_endpoint)
            assert isinstance(
                metrics,
                MetricSamples), f'Failed to get metrics {metrics_name}'
            for n in self.redpanda.nodes:
                samples = [
                    sample for sample in metrics.samples if sample.node == n
                ]
                assert len(
                    samples) == 1, f'Invalid number of samples: {len(samples)}'
                fips_mode = RedpandaServiceBase.FIPSMode(int(samples[0].value))
                assert fips_mode == expected_mode, f'Mismatch in mode: {fips_mode} != {expected_mode}'

        check_fips_mode_metric(
            metrics_name='vectorized_application_fips_mode',
            metrics_endpoint=MetricsEndpoint.METRICS,
            expected_mode=RedpandaServiceBase.FIPSMode.permissive)
        check_fips_mode_metric(
            metrics_name='redpanda_application_fips_mode',
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
            expected_mode=RedpandaServiceBase.FIPSMode.permissive)

    @cluster(num_nodes=3)
    def test_non_homogenous(self):
        """
        This test will validate that the non homogenous FIPS
        metric responds appropriately

        1. Find the lader
        2. On a non-leader node, set to 'disabled'
        3. Restart that node
        4. Wait for count to go to 1
        5. On same node, set back to permissive
        6. Restart node
        7. Wait for count to go back to 0
        """
        admin = Admin(self.redpanda)
        leader = self.redpanda.controller()
        non_leader_nodes = list(set(self.redpanda.nodes).difference({leader}))
        assert len(non_leader_nodes) > 0, "Could not find any non-leader nodes"
        target_node = non_leader_nodes[0]
        self.logger.debug(f'Leader: {leader}')
        self.logger.debug(f'Non-leader: {target_node}')
        NON_FIPS_CONFIG = dict(fips_mode="disabled",
                               openssl_config_file=None,
                               openssl_module_directory=None)
        FIPS_CONFIG = dict(
            fips_mode="permissive",
            openssl_config_file=self.redpanda.get_openssl_config_file_path(),
            openssl_module_directory=self.redpanda.
            get_openssl_modules_directory())

        def get_metrics_value(metrics_name: str,
                              metrics_endpoint: MetricsEndpoint,
                              leader_node: ClusterNode) -> int:
            metrics = self.redpanda.metrics_sample(
                sample_pattern=metrics_name, metrics_endpoint=metrics_endpoint)
            assert isinstance(
                metrics,
                MetricSamples), f'Failed to get metrics for {metrics_name}'

            samples = [
                sample for sample in metrics.samples
                if sample.node == leader_node
            ]
            assert len(
                samples) == 1, f'Invalid number of samples: {len(samples)}'
            return int(samples[0].value)

        def target_node_reports_fips_mode(admin: Admin, node: ClusterNode,
                                          fips_mode: str) -> bool:
            node_id = self.redpanda.node_id(node)
            brokers = admin.get_brokers(node)
            return all([
                b['in_fips_mode'] == fips_mode for b in brokers
                if b['node_id'] == node_id
            ])

        wait_until(
            lambda: get_metrics_value(
                metrics_name='redpanda_cluster_non_homogenous_fips_mode',
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
                leader_node=leader) == 0,
            timeout_sec=5,
            backoff_sec=0.2,
            err_msg="Metrics endpoint never returned 0 for initial check")

        self.logger.debug(f'Restarting node {target_node} in non-FIPS mode')

        self.redpanda.restart_nodes([target_node],
                                    override_cfg_params=NON_FIPS_CONFIG)

        wait_until(lambda: target_node_reports_fips_mode(
            admin, target_node, "disabled"),
                   timeout_sec=5,
                   backoff_sec=0.2,
                   err_msg="Node never reported disabled")

        wait_until(
            lambda: get_metrics_value(
                metrics_name='redpanda_cluster_non_homogenous_fips_mode',
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
                leader_node=leader) == 1,
            timeout_sec=5,
            backoff_sec=0.2,
            err_msg=
            f"Metrics endpoint never returned 1 after bringing up {target_node} in non-FIPS mode"
        )

        self.logger.debug(f'Restarting node {target_node} in FIPS mode')

        self.redpanda.restart_nodes([target_node],
                                    override_cfg_params=FIPS_CONFIG)
        wait_until(lambda: target_node_reports_fips_mode(
            admin, target_node, "permissive"),
                   timeout_sec=5,
                   backoff_sec=0.2,
                   err_msg="Target node never reported permissive")

        wait_until(
            lambda: get_metrics_value(
                metrics_name='redpanda_cluster_non_homogenous_fips_mode',
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
                leader_node=leader) == 0,
            timeout_sec=20,
            backoff_sec=0.2,
            err_msg=
            f"Metrics endpoint never returned 0 after restarting {target_node} in FIPS mode"
        )


class RedpandaFIPSStartupLicenseTest(RedpandaFIPSStartupTestBase):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_context):
        super(RedpandaFIPSStartupLicenseTest,
              self).__init__(test_context=test_context,
                             fips_mode=RedpandaServiceBase.FIPSMode.disabled)

        self.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{self.LICENSE_CHECK_INTERVAL_SEC}'
        })

    def _has_license_nag(self) -> bool:
        return self.redpanda.search_log_any(
            "license is required to use enterprise features")

    def _license_nag_is_set(self) -> bool:
        return self.redpanda.search_log_all(
            f"Overriding default license log annoy interval to: {self.LICENSE_CHECK_INTERVAL_SEC}s"
        )

    @cluster(num_nodes=3)
    def test_fips_license_nag(self):
        wait_until(self._license_nag_is_set,
                   timeout_sec=30,
                   err_msg="Failed to set license nag interval")

        self.logger.debug("Ensuring no license nag")
        sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        assert not self._has_license_nag(
        ), "Should not have license nag yet, FIPS mode not enabled"

        fips_mode = RedpandaServiceBase.FIPSMode.enabled if in_fips_environment(
        ) else RedpandaServiceBase.FIPSMode.permissive

        fips_config = dict(
            fips_mode=self.fips_mode_to_str(fips_mode),
            openssl_config_file=self.redpanda.get_openssl_config_file_path(),
            openssl_module_directory=self.redpanda.
            get_openssl_modules_directory())

        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    override_cfg_params=fips_config)

        wait_until(self._has_license_nag,
                   timeout_sec=30,
                   err_msg="License nag failed to appear")
