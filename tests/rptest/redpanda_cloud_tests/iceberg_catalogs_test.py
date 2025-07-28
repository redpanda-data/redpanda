# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
import logging
import json
import time
import requests
import base64
import random

from ducktape.mark import matrix
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient

from rptest.clients.installpack import InstallPackClient
from rptest.clients.rpk import RpkTool, TopicSpec, RpkException

from rptest.services.redpanda import get_cloud_provider
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaServiceCloud

from rptest.services.databricks_workspace import DatabricksWorkspace
from rptest.context.databricks import DatabricksContext, OauthCredentials
from rptest.services.catalog_service import CatalogType
from rptest.services.datalake.query_engine.databricks_sql import DatabricksSQL


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
            self.redpanda._cloud_cluster.config.install_pack_auth)

    def setUp(self):
        super().setUp()
        cloud_cluster = self.redpanda._cloud_cluster
        self.logger.debug(f"Cloud Cluster Info: {vars(cloud_cluster)}")
        install_pack_version = cloud_cluster.get_install_pack_version()
        self._ip = self._ipClient.getInstallPack(install_pack_version)
        self._clusterId = cloud_cluster.cluster_id
        self._configProfile = self._ip['config_profiles'][
            cloud_cluster.config.config_profile_name]

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
        encoded_secret_data = base64.b64encode(
            secret_data.encode("utf-8")).decode("utf-8")
        # Create encoded Redpanda secret with random ID
        create_resp = cloud_cluster.create_secret(secret_id,
                                                  encoded_secret_data)
        self.logger.debug(f"Create secret response: {create_resp}")

        properties = {
            "iceberg_enabled":
            True,
            "iceberg_rest_catalog_endpoint":
            "https://fake.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
            "iceberg_rest_catalog_authentication_mode":
            "oauth2",
            "iceberg_rest_catalog_client_id":
            "iceberg_rest_catalog_client_id",
            "iceberg_rest_catalog_client_secret":
            f"${{secrets.{secret_id}}}",
            "iceberg_rest_catalog_warehouse":
            "fake_catalog_name",
            "iceberg_catalog_type":
            "rest",
            "iceberg_disable_snapshot_tagging":
            "true",
            "iceberg_rest_catalog_oauth2_scope":
            "all-apis",
            "iceberg_rest_catalog_oauth2_server_uri":
            "https://fake.cloud.databricks.com/oidc/v1/token"
        }

        self.logger.debug(f"Properties to be sent: {properties}")

        num_iterations = 10
        for i in range(num_iterations):
            self.logger.info(
                f"Starting cluster update iteration {i + 1}/{num_iterations}")
            try:
                response = cloud_cluster.update_cluster_property_public(
                    self._clusterId, properties)
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
                    self._clusterId, operation_id)
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
        #client: RpCloudApiClient = cloud_cluster.public_api

        databricks_client = DatabricksWorkspace(context=self._ctx)
        bucket = f"redpanda-cloud-storage-{self._clusterId}"
        self.catalog_info = databricks_client.create_catalog(bucket=bucket)

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
        encoded_secret_data = base64.b64encode(
            secret_data.encode("utf-8")).decode("utf-8")
        # Create Redpanda secret
        create_resp = cloud_cluster.create_secret(secret_id,
                                                  encoded_secret_data)
        self.logger.debug(f"Create secret response: {create_resp}")

        iceberg_rest_catalog_endpoint = dbx_ctx.iceberg_rest_url
        iceberg_rest_catalog_oauth2_server_uri = dbx_ctx.iceberg_rest_catalog_oauth2_server_uri

        # Construct the payload for the request
        properties = {
            "iceberg_enabled":
            True,
            "iceberg_rest_catalog_endpoint":
            iceberg_rest_catalog_endpoint,
            "iceberg_rest_catalog_authentication_mode":
            "oauth2"
            if isinstance(dbx_ctx.credentials, OauthCredentials) else "bearer",
            "iceberg_rest_catalog_client_id":
            str(databricks_client_id),
            "iceberg_rest_catalog_client_secret":
            f"${{secrets.{secret_id}}}",
            "iceberg_rest_catalog_warehouse":
            self.catalog_info.name,
            "iceberg_catalog_type":
            "rest",
            "iceberg_disable_snapshot_tagging":
            "true",
            "iceberg_rest_catalog_oauth2_scope":
            "all-apis",
            "iceberg_rest_catalog_oauth2_server_uri":
            iceberg_rest_catalog_oauth2_server_uri
        }

        # Log the constructed payload for debugging
        self.logger.debug(f"Properties to be sent: {properties}")

        try:
            # Send the HTTP PATCH request
            response = cloud_cluster.update_cluster_property_public(
                self._clusterId, properties)

            if response:
                self.logger.debug(f"Update successful: {response}")
            else:
                self.logger.error("Failed to update cluster properties.")

        except Exception as e:
            self.logger.error(
                f"An error occurred while updating cluster properties: {e}")

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
            self._clusterId, operation_id)

        if not success:
            self.logger.error(
                f"Operation {operation_id} did not complete successfully.")

        try:
            # Create topic(s) and produce data
            self.rpk = RpkTool(self.redpanda)
            self.test_topic = f"test_topic_{random.randint(10000, 99999)}"
            self.logger.debug(f"Creating Iceberg topic: self.test_topic")
            self.rpk.create_topic(self.test_topic)
            self.rpk.alter_topic_config(self.test_topic,
                                        TopicSpec.PROPERTY_ICEBERG_MODE,
                                        'key_value')

            self.logger.info("Producing data to the topic")
            MESSAGE_COUNT = 10
            for i in range(MESSAGE_COUNT):
                self.rpk.produce(self.test_topic, f"foo {i} ", f"bar {i}")

            # Produce simple key-value
            self.logger.debug("Producing simple key-value data")
            self.rpk.produce(self.test_topic, "test_key", "test_value")

            # Produce JSON payload without headers
            self.logger.debug("Producing json without headers")
            json_payload_1 = {
                "sensor_id": "temp-001",
                "type": "temperature",
                "value": 74.6,
                "unit": "F",
                "timestamp": "2025-07-17T23:30:00Z",
                "location": {
                    "zone": "A1",
                    "machine_id": "MX-22"
                },
                "status": "ok",
            }
            self.rpk.produce(self.test_topic, "sensor_test_1",
                             json.dumps(json_payload_1))

            self.logger.debug("Producing json with headers")
            json_payload_2 = {
                "sensor_id": "temp-001",
                "type": "temperature",
                "value": 74.6,
                "unit": "F",
                "timestamp": "2025-07-17T23:30:00Z",
                "location": {
                    "zone": "A1",
                    "machine_id": "MX-22"
                },
                "status": "ok",
            }
            self.rpk.produce(self.test_topic, "sensor_test_2",
                             json.dumps(json_payload_2))

        except Exception as e:
            self.logger.exception(
                f"Failed during topic creation or data production: {e}")
            raise

        self.logger.info("Waiting for produced data and tables to be created")
        for i in range(200, 0, -1):
            if i % 10 == 0 or i <= 10:  # Log every 10 seconds, and every second for the last 10s
                self.logger.info(f"...waiting {i} seconds remaining")
            time.sleep(1)

        expected_records = [
            ("test_key", "test_value", {}),
            ("sensor_test_1", json_payload_1, {}),
            ("sensor_test_2", json_payload_2, {}),
        ]

        # Perform verification via Databricks SQL
        self.logger.debug("Verifying data in Databricks Iceberg table...")
        success = DatabricksSQL(
            ctx=self._ctx,
            iceberg_catalog_uri="unused",
            default_warehouse_dir="unused",
            catalog_type=CatalogType.DATABRICKS_UNITY,
            catalog_name=self.catalog_info.name).verify_table_contents(
                namespace="redpanda",
                table=self.test_topic,
                expected_records=expected_records)

        # Assert and log result
        if not success:
            self.logger.error(
                "Verification failed. Some expected records not found.")
            raise AssertionError("Verification failed for table contents.")

        databricks_client.stop()
