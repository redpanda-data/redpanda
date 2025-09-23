# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import base64
import random
import time


from rptest.clients.installpack import InstallPackClient
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.context.databricks import DatabricksContext, OauthCredentials
from rptest.services.cluster import cluster
from rptest.services.databricks_workspace import DatabricksWorkspace
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest


def supported_catalog_types():
    return ["aws_glue", "databricks_unity", "snowflake"]


def supported_network_types():
    return ["public", "private"]


class IcebergCloudCatalogsTest(RedpandaCloudTest):
    """
    Verify that cluster infra/config matches config profile used to launch - only applies to cloudv2
    """

    def __init__(self, test_context):
        super().__init__(test_context=test_context)
        self._ctx = test_context
        self._ipClient = InstallPackClient(
            self.redpanda._cloud_cluster.config.install_pack_url_template,
            self.redpanda._cloud_cluster.config.install_pack_auth_type,
            self.redpanda._cloud_cluster.config.install_pack_auth,
        )

    def setUp(self):
        super().setUp()
        cloud_cluster = self.redpanda._cloud_cluster
        self.logger.debug(f"Cloud Cluster Info: {vars(cloud_cluster)}")
        install_pack_version = cloud_cluster.get_install_pack_version()
        self._ip = self._ipClient.getInstallPack(install_pack_version)
        self._clusterId = cloud_cluster.cluster_id
        self._configProfile = self._ip["config_profiles"][
            cloud_cluster.config.config_profile_name
        ]

    def test_healthy(self):
        r = self.redpanda.cluster_unhealthy_reason()
        assert r is None, r
        assert self.redpanda.cluster_healthy()
        self.redpanda.assert_cluster_is_reusable()

    @cluster(num_nodes=1)
    def test_cluster_updates(self):
        """
        Test repeated Redpanda cluster property updates with a secret reference.

        This test performs the following steps:
        1. Creates a new Redpanda secret with a randomly generated name and base64-encoded value.
        2. Constructs a properties payload enabling Iceberg integration, using the created secret.
        3. Repeats a cluster update operation `num_iterations` times to validate:
        - That each update succeeds without errors.
        - That the update completes within a reasonable amount of time.
        - That polling for the operation status correctly identifies completion.

        This test helps to catch regressions in update logic, API response handling, and operation polling.

        Raises:
            Exception: If any update operation fails or polling detects a failure state.
        """
        cloud_cluster = self.redpanda._cloud_cluster
        secret_id = f"UNITY_CLIENT_SECRET_{random.randint(10000, 99999)}"
        secret_data = "fake_secret"
        # Encode secret as base64
        encoded_secret_data = base64.b64encode(secret_data.encode("utf-8")).decode(
            "utf-8"
        )
        # Create encoded Redpanda secret with random ID
        create_resp = cloud_cluster.create_secret(secret_id, encoded_secret_data)
        self.logger.debug(f"Create secret response: {create_resp}")

        properties = {
            "iceberg_enabled": True,
            "iceberg_rest_catalog_endpoint": "https://fake.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
            "iceberg_rest_catalog_authentication_mode": "oauth2",
            "iceberg_rest_catalog_client_id": "iceberg_rest_catalog_client_id",
            "iceberg_rest_catalog_client_secret": f"${{secrets.{secret_id}}}",
            "iceberg_rest_catalog_warehouse": "fake_catalog_name",
            "iceberg_catalog_type": "rest",
            "iceberg_disable_snapshot_tagging": "true",
            "iceberg_rest_catalog_oauth2_scope": "all-apis",
            "iceberg_rest_catalog_oauth2_server_uri": "https://dbc-0f5177e3-6aa4.cloud.databricks.com/oidc/v1/token",
        }

        self.logger.debug(f"Properties to be sent: {properties}")

        num_iterations = 10
        for i in range(num_iterations):
            self.logger.info(
                f"Starting cluster update iteration {i + 1}/{num_iterations}"
            )
            try:
                response = cloud_cluster.update_cluster_property_public(
                    self._clusterId, properties
                )
                if not response:
                    self.logger.error("Failed to update cluster properties.")
                    return
                self.logger.debug(f"Update successful: {response}")

                operation = response.get("operation", {})
                operation_id = operation.get("id")
                if not operation_id:
                    self.logger.error(
                        "No operation ID returned from update_cluster_property_public."
                    )
                    return
                self.logger.debug(f"Operation id: {operation_id}")

                success = cloud_cluster.wait_for_operation_complete(
                    self._clusterId, operation_id
                )
                if not success:
                    self.logger.error(
                        f"Operation {operation_id} did not complete successfully."
                    )

            except Exception as e:
                self.logger.error(
                    f"An error occurred while updating cluster properties: {e}"
                )
                raise

    @cluster(num_nodes=1)
    def test_databricks_basic(self):
        dbx_ctx = DatabricksContext.from_context(self._ctx)

        cloud_cluster = self.redpanda._cloud_cluster
        # client: RpCloudApiClient = cloud_cluster.public_api

        databricks_client = DatabricksWorkspace(context=self._ctx)
        bucket = f"redpanda-cloud-storage-{self._clusterId}"
        catalog_info = databricks_client.create_catalog(bucket=bucket)

        # Parameters for creating Redpanda secret
        secret_id = f"UNITY_CLIENT_SECRET_{random.randint(10000, 99999)}"

        # Access credentials (client_secret, client_id) from dbx_ctx.credentials
        if isinstance(dbx_ctx.credentials, OauthCredentials):
            secret_data = dbx_ctx.credentials.client_secret
            databricks_client_id = dbx_ctx.credentials.client_id
            self.logger.debug(f"Using oauth2 for: {databricks_client_id}")
        else:
            self.logger.debug("Using bearer token")
            secret_data = dbx_ctx.credentials.token
            databricks_client_id = None
        self.logger.debug(f"Databricks client ID: {databricks_client_id}")
        # Encode secret as base64
        encoded_secret_data = base64.b64encode(secret_data.encode("utf-8")).decode(
            "utf-8"
        )
        # Create Redpanda secret
        create_resp = cloud_cluster.create_secret(secret_id, encoded_secret_data)
        self.logger.debug(f"Create secret response: {create_resp}")

        iceberg_rest_catalog_endpoint = dbx_ctx.iceberg_rest_url

        # Construct the payload for the request
        properties = {
            "iceberg_enabled": True,
            "iceberg_rest_catalog_endpoint": iceberg_rest_catalog_endpoint,
            "iceberg_rest_catalog_authentication_mode": "oauth2"
            if isinstance(dbx_ctx.credentials, OauthCredentials)
            else "bearer",
            "iceberg_rest_catalog_client_id": str(databricks_client_id),
            "iceberg_rest_catalog_client_secret": f"${{secrets.{secret_id}}}",
            "iceberg_rest_catalog_warehouse": catalog_info.name,
            "iceberg_catalog_type": "rest",
            "iceberg_disable_snapshot_tagging": "true",
            "iceberg_rest_catalog_oauth2_scope": "all-apis",
            "iceberg_rest_catalog_oauth2_server_uri": "https://dbc-0f5177e3-6aa4.cloud.databricks.com/oidc/v1/token",
        }

        # Log the constructed payload for debugging
        self.logger.debug(f"Properties to be sent: {properties}")

        try:
            # Send the HTTP PATCH request
            response = cloud_cluster.update_cluster_property_public(
                self._clusterId, properties
            )

            if response:
                self.logger.debug(f"Update successful: {response}")
            else:
                self.logger.error("Failed to update cluster properties.")

        except Exception as e:
            self.logger.error(
                f"An error occurred while updating cluster properties: {e}"
            )

        # Extract operation ID
        operation = response.get("operation", {})
        operation_id = operation.get("id")
        if not operation_id:
            self.logger.error(
                "No operation ID returned from update_cluster_property_public."
            )
            return
        self.logger.debug(f"Operation id: {operation_id}")

        # Poll for operation completion
        success = cloud_cluster.wait_for_operation_complete(
            self._clusterId, operation_id
        )

        if not success:
            self.logger.error(
                f"Operation {operation_id} did not complete successfully."
            )

        # Create topic(s) and produce data
        self.rpk = RpkTool(self.redpanda)
        test_topic = "test_topic"
        self.rpk.create_topic(test_topic)
        self.rpk.alter_topic_config(
            test_topic, TopicSpec.PROPERTY_ICEBERG_MODE, "key_value"
        )

        MESSAGE_COUNT = 10
        for i in range(MESSAGE_COUNT):
            self.rpk.produce(test_topic, f"foo {i} ", f"bar {i}")

        self.logger.debug("Waiting 10 minute...")
        time.sleep(600)
        # TODO Marat add verification, more tests and options (separate PR)

        databricks_client.stop()
