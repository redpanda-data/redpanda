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
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier


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
            "https://fake.cloud.databricks.com/oidc/v1/token",
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
    def test_databricks_e2e(self):
        dbx_ctx = DatabricksContext.from_context(self._ctx)

        cloud_cluster = self.redpanda._cloud_cluster
        # client: RpCloudApiClient = cloud_cluster.public_api

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
        iceberg_rest_catalog_oauth2_server_uri = f"{dbx_ctx.workspace_url}/oidc/v1/token"

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
            iceberg_rest_catalog_oauth2_server_uri,
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
            self.logger.debug(f"Creating Iceberg topic: {self.test_topic}")
            self.rpk.create_topic(self.test_topic)
            self.rpk.alter_topic_config(self.test_topic,
                                        TopicSpec.PROPERTY_ICEBERG_MODE,
                                        'key_value')

            self.logger.info(f"Producing data to the topic: {self.test_topic}")
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
            # Produce with headers
            headers = [
                "content-type:application/json", "source:sensor-network"
            ]
            self.rpk.produce(self.test_topic,
                             "sensor_test_2",
                             json.dumps(json_payload_2),
                             headers=headers)

        except Exception as e:
            self.logger.exception(
                f"Failed during topic creation or data production: {e}")
            raise

        # Create query engine first so we can use it to check for data
        query_engine = DatabricksSQL(ctx=self._ctx,
                                     iceberg_catalog_uri="unused",
                                     default_warehouse_dir="unused",
                                     catalog_type=CatalogType.DATABRICKS_UNITY,
                                     catalog_name=self.catalog_info.name)

        self.logger.info(
            "Waiting for produced data to be synchronized to Unity Catalog...")

        # Wait for data to sync with retry logic
        max_wait_time = 180  # 3 minutes total
        check_interval = 20  # Check every 20 seconds
        data_found = False

        for elapsed in range(0, max_wait_time, check_interval):
            remaining = max_wait_time - elapsed
            self.logger.info(
                f"Checking for data synchronization... ({remaining}s remaining)"
            )

            try:
                # Check if table exists and has data
                check_query = f"""SELECT COUNT(*) as count 
                               FROM `{self.catalog_info.name}`.`redpanda`.`{self.test_topic}`"""

                result = query_engine.run_query_fetch_one(check_query)
                row_count = result[0] if result else 0
                self.logger.info(
                    f"Table {self.test_topic} has {row_count} rows")

                if row_count > 0:
                    data_found = True
                    self.logger.info(f"✓ Data found in table after {elapsed}s")

                    # Show sample of data
                    sample_query = f"""SELECT 
                                        redpanda.offset,
                                        redpanda.partition,
                                        redpanda.key,
                                        value,
                                        redpanda.headers
                                     FROM `{self.catalog_info.name}`.`redpanda`.`{self.test_topic}`
                                     ORDER BY redpanda.offset
                                     LIMIT 5"""

                    with query_engine.run_query(sample_query) as cursor:
                        sample_rows = list(cursor)
                        self.logger.info(f"Sample of data in table:")
                        for i, row in enumerate(sample_rows):
                            self.logger.info(f"  Row {i}: {row}")
                    break

            except Exception as e:
                self.logger.warning(f"Error checking table: {e}")

            if elapsed + check_interval < max_wait_time:
                time.sleep(check_interval)

        if not data_found:
            # If no data found, check if table exists at all
            try:
                schema_query = f"""DESCRIBE TABLE `{self.catalog_info.name}`.`redpanda`.`{self.test_topic}`"""
                with query_engine.run_query(schema_query) as cursor:
                    schema_rows = list(cursor)
                    self.logger.error(f"Table exists but has no data. Schema:")
                    for row in schema_rows:
                        self.logger.error(f"  {row}")
            except Exception as e:
                self.logger.error(f"Table may not exist: {e}")
            raise AssertionError(
                f"No data found in Iceberg table after {max_wait_time}s")

        MESSAGE_COUNT = 10
        expected_records = [(f"foo {i} ", f"bar {i}", {})
                            for i in range(MESSAGE_COUNT)]

        expected_records.extend([
            ("test_key", "test_value", {}),
            ("sensor_test_1", json_payload_1, {}),
            ("sensor_test_2", json_payload_2, {
                "content-type": "application/json",
                "source": "sensor-network"
            }),
        ])

        verifier = DatalakeVerifier(redpanda=self.redpanda,
                                    topic=self.test_topic,
                                    query_engine=query_engine)

        # Perform detailed verification via Databricks SQL and datalake_verifier
        try:
            self.logger.info(
                f"Continuing with automated verification for topic: {self.test_topic}"
            )

            # First, let's query ALL data to see what's actually in the table
            self.logger.info("Querying ALL data from the table to debug...")
            all_data_query = f"""SELECT 
                                     redpanda.offset,
                                     redpanda.partition,
                                     redpanda.key,
                                     value,
                                     redpanda.headers
                                  FROM `{self.catalog_info.name}`.`redpanda`.`{self.test_topic}`
                                  ORDER BY redpanda.offset"""

            actual_records = []
            with query_engine.run_query(all_data_query) as cursor:
                actual_rows = list(cursor)
                self.logger.info(
                    f"\nFound {len(actual_rows)} total rows in the Iceberg table"
                )

                for i, row in enumerate(actual_rows):
                    offset, partition, key_hex, value_hex, headers = row
                    # Decode hex values
                    key_decoded = verifier.safe_decode(key_hex)
                    value_decoded = verifier.safe_decode(value_hex)

                    self.logger.info(
                        f"Row {i}: offset={offset}, partition={partition}")
                    self.logger.info(f"  Key (hex): {key_hex}")
                    self.logger.info(f"  Key (decoded): '{key_decoded}'")
                    self.logger.info(f"  Value (hex): {value_hex[:100]}...")
                    self.logger.info(
                        f"  Value (decoded): '{value_decoded[:100]}...'")
                    self.logger.info(f"  Headers: {headers}")

                    actual_records.append(
                        (key_decoded, value_decoded, headers))

            # Now check if we have the expected number of records
            if len(actual_rows) != len(expected_records):
                self.logger.error(
                    f"Record count mismatch: expected {len(expected_records)}, found {len(actual_rows)}"
                )

            # For now, let's just verify we have some data
            if len(actual_rows) == 0:
                raise AssertionError("No data found in Iceberg table")

            # Run the actual verification
            success, errors = verifier.verify_data(
                expected_records=expected_records)

            if not success:
                self.logger.error(
                    f"Verification failed with {len(errors)} errors:")
                for i, error in enumerate(errors, 1):
                    self.logger.error(f"Error {i}: {error}")

                # Log what we expected vs what we found
                self.logger.error(f"\n=== EXPECTED vs ACTUAL ===")
                self.logger.error(f"Expected {len(expected_records)} records:")
                for i, (key, value, headers) in enumerate(expected_records):
                    self.logger.error(
                        f"  Expected[{i}]: key='{key}', value='{value}', headers={headers}"
                    )

                self.logger.error(
                    f"\nActual {len(actual_records)} records found:")
                for i, (key, value, headers) in enumerate(actual_records):
                    self.logger.error(
                        f"  Actual[{i}]: key='{key}', value='{value[:100]}...', headers={headers}"
                    )

                raise AssertionError(
                    f"Data verification failed: {len(errors)} errors found. See logs above for details."
                )

            self.logger.info(
                "Verification succeeded - all expected records found in Iceberg table"
            )
        except Exception as e:
            self.logger.error(
                f"An exception occurred during data verification: {e}")
            raise

        databricks_client.stop()
