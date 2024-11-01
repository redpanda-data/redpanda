# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re
import datetime
import tempfile
import zipfile
import json

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, MetricSamples, MetricsEndpoint
from rptest.util import expect_exception, get_cluster_license, get_second_cluster_license
from ducktape.utils.util import wait_until
from rptest.util import wait_until_result
from rptest.utils.mode_checks import in_fips_environment

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException


class RpkClusterTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkClusterTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_cluster_info(self):
        def condition():
            brokers = self._rpk.cluster_info()

            if len(brokers) != len(self.redpanda.nodes):
                return False

            advertised_addrs = self.redpanda.brokers()

            ok = True
            for b in brokers:
                ok = ok and \
                    b.address in advertised_addrs

            return ok

        wait_until(condition,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="No brokers found or output doesn't match")

    @cluster(num_nodes=3)
    def test_get_config(self):
        node = self.redpanda.nodes[0]

        config_output = self._rpk.admin_config_print(node)

        # Check the output is valid json
        parsed = json.loads("".join(config_output))

        # Check the output contains at least one known config property
        assert 'enable_transactions' in parsed

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_node_down(self):
        """
        Test RPK's handling of a degraded cluster.  For requests using the
        internal sendAny helper to issue requests to a random node, they should
        retry on another node if the one they pick is down, rather than failing.
        """

        # When doing a sendAny operation, do it repeatedly to exercise
        # various possible RNG outcomes internally to rpk
        sendany_iterations = 10

        # Try with each node offline, in case RPK's behaviour isn't shuffling
        # properly and prefers one node over others.
        for i, node in enumerate(self.redpanda.nodes):
            self.logger.info(
                f"Stopping node {node.name}/{self.redpanda.idx(node)} ")
            self.redpanda.stop_node(node)

            # A vanilla sendAny command
            self.logger.info(f"Trying simple GETs with node {node.name} down")
            for _ in range(0, sendany_iterations):
                self._rpk.cluster_config_status()

            # A sendToLeader command: should be tolerant of a down node while
            # discovering who the leader is, then should also be retrying
            # enough in the request to the leader to wait for election (we
            # just bounced a node so leader may not be elected yet)
            self.logger.info(
                f"Trying write request with node {node.name} down")
            self._rpk.sasl_create_user(
                f"testuser_{i}", "password",
                self.redpanda.SUPERUSER_CREDENTIALS.algorithm)

            self.logger.info(
                f"Starting node {node.name}/{self.redpanda.idx(node)} ")
            self.redpanda.start_node(node)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_cluster_down(self):
        """
        Test that RPK's timeout logic copes properly with a totally offline cluster: should
        time out and return an error.  This test verifies that more complex retry logic (like
        rpk's internal sendAny iterating over nodes) does not incorrectly retry forever.
        """
        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node)

        try:
            # A vanilla sendAny command
            r = self._rpk.cluster_config_status()
        except RpkException as e:
            self.logger.info(f"Got expected exception: {e}")
            pass
        else:
            assert False, f"Unexpected success: '{r}'"

        try:
            # A sendToLeader command
            r = self._rpk.sasl_create_user(
                "expect_fail", "expect_fail",
                self.redpanda.SUPERUSER_CREDENTIALS.algorithm)
        except RpkException as e:
            self.logger.info(f"Got expected exception: {e}")
            pass
        else:
            assert False, f"Unexpected success: '{r}'"

    def _get_license_expiry(self) -> int:
        METRICS_NAME = "cluster_features_enterprise_license_expiry_sec"

        def get_metrics_value(metrics_endpoint: MetricsEndpoint) -> int:
            metrics = self.redpanda.metrics_sample(
                sample_pattern=METRICS_NAME, metrics_endpoint=metrics_endpoint)
            assert isinstance(metrics, MetricSamples), \
                    f'Failed to get metrics for {METRICS_NAME}'

            samples = [sample for sample in metrics.samples]
            assert len(samples) == len(self.redpanda.nodes), \
                f'Invalid number of samples: {len(samples)}'
            return int(samples[0].value)

        internal_val = get_metrics_value(MetricsEndpoint.METRICS)
        public_val = get_metrics_value(MetricsEndpoint.PUBLIC_METRICS)
        # generous slop. only a very large difference here would suggest a
        # logic error on the backend (e.g. approaching a calendar day).
        threshold_s = 30
        assert abs(
            internal_val - public_val
        ) <= threshold_s, f"Mismatch: abs({internal_val} - {public_val}) > {threshold_s}"
        return internal_val

    @cluster(num_nodes=3)
    def test_upload_and_query_cluster_license_rpk(self):
        """
        Test uploading and retrieval of license via rpk
        using --path option
        """
        license = get_cluster_license()
        if license is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        evaluation_period = 2592000  # 30 days
        wait_until(
            lambda: self._get_license_expiry() <= evaluation_period,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg=
            "Without a license we should observe the built in evaluation period trial license"
        )

        with tempfile.NamedTemporaryFile() as tf:
            tf.write(bytes(license, 'UTF-8'))
            tf.seek(0)
            output = self._rpk.license_set(tf.name)
            assert "Successfully uploaded license" in output

        wait_until(
            lambda: self._get_license_expiry() > evaluation_period,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="The expiry metric should be positive with a valid license"
        )

        expected_features = ['fips'] if in_fips_environment() else []
        expected_license = {
            'expires': "Jul 11 2122",
            'organization': 'redpanda-testing',
            'type': 'enterprise',
            'checksum_sha256':
            '2730125070a934ca1067ed073d7159acc9975dc61015892308aae186f7455daf',
            'expires_unix': 4813252273,
            'license_expired': False,
            'license_status': 'valid',
            'license_violation': False,
            'enterprise_features_in_use': expected_features,
        }

        def compare_license():
            license = self._rpk.license_info()
            self.redpanda.logger.debug(f"License response: {license}")
            if license is None or license == "{}":
                return False
            return json.loads(license) == expected_license

        wait_until(
            compare_license,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="unable to retrieve and compare license information")

        # Assert that a second put takes license
        license = get_second_cluster_license()
        output = self._rpk.license_set(None, license)
        assert "Successfully uploaded license" in output

        def obtain_new_license():
            lic = self._rpk.license_info()
            if lic is None or lic == "{}":
                return False
            result = json.loads(lic)
            return result['organization'] == 'redpanda-testing-2'

        wait_until(obtain_new_license,
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True,
                   err_msg="unable to retrieve new license information")

    @cluster(num_nodes=3)
    def test_upload_cluster_license_rpk(self):
        """
        Test uploading of license via rpk
        using inline license option
        """
        license = get_cluster_license()
        if license is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        output = self._rpk.license_set("", license)
        assert "Successfully uploaded license" in output

    @cluster(num_nodes=3)
    def test_upload_cluster_license_error(self):
        license = get_cluster_license()
        if license is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        with expect_exception(RpkException,
                              lambda e: "License is malformed" in str(e)):
            with tempfile.NamedTemporaryFile() as tf:
                tf.write(bytes(license + 'r', 'UTF-8'))
                tf.seek(0)

                self._rpk.license_set(tf.name)

    @cluster(num_nodes=3)
    def test_expired_evaluation_period(self):
        """
        Test that once the eval period license expires, it is not observable
        """
        self.logger.debug("Ensure the eval period license is expired")
        self.redpanda.set_environment(
            dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE='1'))
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            use_maintenance_mode=False)
        self.redpanda.wait_until(
            self.redpanda.healthy,
            timeout_sec=15,
            backoff_sec=1,
            err_msg="The cluster failed to stabilize after restart")

        wait_until(
            lambda: self._get_license_expiry() == -1,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg=
            "After the trial period expired, the license metric should show no license (-1)"
        )

        resp = self._rpk.license_info()
        self.logger.debug(f"Response: {resp}")
        resp_json = json.loads(resp)
        assert resp_json['license_status'] == 'not_present', \
                f"Expected: not_present. Got: {resp_json['license_status']}"
