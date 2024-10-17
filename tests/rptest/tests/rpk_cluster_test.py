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

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.rpk_remote import RpkRemoteTool


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
    def test_debug_bundle(self):
        # The main RpkTool helper runs rpk on the test runner machine -- debug
        # commands are run on redpanda nodes.
        root_name = "bundle"
        bundle_name = "bundle" + ".zip"
        working_dir = "/tmp"
        file_path = os.path.join(working_dir, bundle_name)
        node = self.redpanda.nodes[0]

        rpk_remote = RpkRemoteTool(self.redpanda, node)
        output = rpk_remote.debug_bundle(file_path)
        lines = output.split("\n")

        # On error, rpk bundle returns 0 but writes error description to stdout
        output_file = None
        error_lines = []
        any_errs = False
        for l in lines:
            self.logger.info(l)
            if l.strip().startswith("* "):
                error_lines.append(l)
            elif 'errors occurred' in l:
                any_errs = True
            else:
                m = re.match("^Debug bundle saved to '(.+)'$", l)
                if m:
                    output_file = m.group(1)

        # Avoid false passes if our error line scraping gets broken
        # by a format change.
        if any_errs:
            assert error_lines

        filtered_errors = []
        for l in error_lines:
            if "dmidecode" in l:
                # dmidecode doesn't work in ducktape containers, ignore
                # errors about it.
                continue
            if re.match(r".* error querying .*\.ntp\..* i\/o timeout", l):
                self.logger.error(f"Non-fatal transitory NTP error: {l}")
            else:
                self.logger.error(f"Bad output line: {l}")
                filtered_errors.append(l)

        assert not filtered_errors
        assert output_file == file_path

        node.account.copy_from(output_file, working_dir)

        zf = zipfile.ZipFile(output_file)
        files = zf.namelist()
        assert f'{root_name}/redpanda.yaml' in files
        assert f'{root_name}/redpanda.log' in files

        # At least the first controller log is being saved:
        assert f'{root_name}/controller-logs/redpanda/controller/0_0/0-1-v1.log' in files

        # Cluster admin API calls:
        assert f'{root_name}/admin/brokers.json' in files
        assert f'{root_name}/admin/cluster_config.json' in files
        assert f'{root_name}/admin/health_overview.json' in files

        # Per-node admin API calls:
        for n in self.redpanda.started_nodes():
            # rpk will save 2 snapsots per metrics endpoint:
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t0_metrics.txt' in files
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t1_metrics.txt' in files
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t0_public_metrics.txt' in files
            assert f'{root_name}/metrics/{n.account.hostname}-9644/t1_public_metrics.txt' in files
            # and 1 cluster_view and node_config per node:
            assert f'{root_name}/admin/cluster_view_{n.account.hostname}-9644.json' in files
            assert f'{root_name}/admin/node_config_{n.account.hostname}-9644.json' in files
            assert f'{root_name}/admin/cpu_profile_{n.account.hostname}-9644.json' in files

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

        wait_until(lambda: self._get_license_expiry() == -1,
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True,
                   err_msg="Unset license should return a -1 expiry")

        with tempfile.NamedTemporaryFile() as tf:
            tf.write(bytes(license, 'UTF-8'))
            tf.seek(0)
            output = self._rpk.license_set(tf.name)
            assert "Successfully uploaded license" in output

        def obtain_license():
            lic = self._rpk.license_info()
            return (lic != "{}", lic)

        rp_license = wait_until_result(
            obtain_license,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="unable to retrieve license information")

        wait_until(
            lambda: self._get_license_expiry() > 0,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="The expiry metric should be positive with a valid license"
        )

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
            'enterprise_features_in_use': [],
        }
        result = json.loads(rp_license)
        assert expected_license == result, result

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
