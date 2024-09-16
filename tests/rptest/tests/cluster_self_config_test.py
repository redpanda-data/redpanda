# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import re

from ducktape.mark import parametrize, matrix

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings, get_cloud_storage_type
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.utils import LogSearchLocal
from rptest.utils.mode_checks import skip_fips_mode


class ClusterSelfConfigTest(EndToEndTest):
    def __init__(self, ctx):
        self.ctx = ctx
        super().__init__(ctx)

    def str_in_logs(self, node, s):
        return any(s in log.strip()
                   for log in self.log_searcher._capture_log(node, s))

    def self_config_start_in_logs(self, node):
        client_self_configuration_start_string = 'Client requires self configuration step'
        return self.str_in_logs(node, client_self_configuration_start_string)

    def self_config_default_in_logs(self, node):
        client_self_configuration_default_string = 'Could not self-configure S3 Client'
        return self.str_in_logs(node, client_self_configuration_default_string)

    def self_config_result_from_logs(self, node):
        client_self_configuration_complete_string = 'Client self configuration completed with result'
        for log in self.log_searcher._capture_log(
                node, client_self_configuration_complete_string):
            m = re.search(
                client_self_configuration_complete_string + r' (\{.*\})',
                log.strip())
            if m:
                return m.group(1)
        return None

    @cluster(num_nodes=1)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_s3_self_config(self, cloud_storage_type):
        """
        Verify that cloud_storage_url_style self configuration occurs for the s3_client
        when it is not specified. There aren't any endpoints for testing this, so
        it will be manually checked for from the logs.
        """
        si_settings = SISettings(
            self.ctx,
            # Force self configuration through setting cloud_storage_url_style to None.
            cloud_storage_url_style=None)

        self.start_redpanda(si_settings=si_settings)
        admin = Admin(self.redpanda)
        self.log_searcher = LogSearchLocal(self.ctx, [], self.redpanda.logger,
                                           self.redpanda.STDOUT_STDERR_CAPTURE)

        config = admin.get_cluster_config()

        # Even after self-configuring, the cloud_storage_url_style setting will
        # still be left unset at the cluster config level.
        assert config['cloud_storage_url_style'] is None

        for node in self.redpanda.nodes:
            # Assert that self configuration started.
            assert self.self_config_start_in_logs(node)

            # Assert that self configuration returned a result.
            self_config_result = self.self_config_result_from_logs(node)

            # Currently, virtual_host will succeed in all cases with MinIO.
            self_config_expected_results = [
                '{s3_self_configuration_result: {s3_url_style: virtual_host}}',
                '{s3_self_configuration_result: {s3_url_style: path}}'
            ]

            assert self_config_result and self_config_result in self_config_expected_results

    # OCI only supports path-style requests, fips mode will always fail.
    @skip_fips_mode
    @cluster(num_nodes=1)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_s3_oracle_self_config(self, cloud_storage_type):
        """
        Verify that the cloud_storage_url_style self-configuration for OCI
        backend always results in path-style.
        """
        oracle_api_endpoint = 'mynamespace.compat.objectstorage.us-phoenix-1.oraclecloud.com'
        si_settings = SISettings(
            self.ctx,
            # Force self configuration through setting cloud_storage_url_style to None.
            cloud_storage_url_style=None,
            # Set Oracle endpoint to expected format.
            # https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi_topic-Amazon_S3_Compatibility_API_Support.htm#s3-api-support
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            cloud_storage_api_endpoint=oracle_api_endpoint,
            # Bypass bucket creation, cleanup, and scrubbing, as we won't actually be
            # able to access the endpoint (Self configuration will use the endpoint
            # to set path-style without issuing a request).
            bypass_bucket_creation=True,
            use_bucket_cleanup_policy=False,
            skip_end_of_test_scrubbing=True)
        extra_rp_conf = {
            'cloud_storage_enable_scrubbing': False,
            'cloud_storage_disable_upload_loop_for_tests': True,
            'enable_cluster_metadata_upload_loop': False
        }

        self.start_redpanda(extra_rp_conf=extra_rp_conf,
                            si_settings=si_settings)
        admin = Admin(self.redpanda)
        self.log_searcher = LogSearchLocal(self.ctx, [], self.redpanda.logger,
                                           self.redpanda.STDOUT_STDERR_CAPTURE)

        config = admin.get_cluster_config()

        # Make sure that the endpoint was overridden.
        assert config['cloud_storage_api_endpoint'] == oracle_api_endpoint

        # Even after self-configuring, the cloud_storage_url_style setting will
        # still be left unset at the cluster config level.
        assert config['cloud_storage_url_style'] is None

        for node in self.redpanda.nodes:
            # Assert that self configuration started.
            assert self.self_config_start_in_logs(node)

            # Assert that self configuration returned a result.
            self_config_result = self.self_config_result_from_logs(node)

            # Oracle only supports path-style requests, self-configuration will always succeed.
            self_config_expected_results = [
                '{s3_self_configuration_result: {s3_url_style: path}}'
            ]

            assert self_config_result and self_config_result in self_config_expected_results
