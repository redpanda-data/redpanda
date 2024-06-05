# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os

from rptest.services.cluster import cluster
from rptest.services.redpanda import in_fips_environment, MetricsEndpoint, MetricSamples, RedpandaServiceBase
from rptest.tests.redpanda_test import RedpandaTest


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


class RedpandaFIPSStartupTest(RedpandaTest):
    """
    Tests that Redpanda can start up in FIPS permissive mode
    """
    def __init__(self, test_context):
        super(RedpandaFIPSStartupTest,
              self).__init__(test_context=test_context)

        for node in self.redpanda.nodes:
            self.redpanda.set_extra_node_conf(
                node, {
                    "fips_mode":
                    "permissive",
                    "openssl_config_file":
                    self.redpanda.get_openssl_config_file_path(),
                    "openssl_module_directory":
                    self.redpanda.get_openssl_modules_directory()
                })

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
