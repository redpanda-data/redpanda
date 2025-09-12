# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import logging
import time
from typing import Any

import requests
from ducktape.mark import matrix

from rptest.clients.installpack import InstallPackClient
from rptest.clients.rpk import RpkException, RpkTool, TopicSpec
from rptest.context.databricks import DatabricksContext, OauthCredentials
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.databricks_workspace import DatabricksWorkspace
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient
from rptest.services.redpanda import RedpandaServiceCloud, get_cloud_provider
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest

# from rptest.tests.datalake.datalake_services import DatalakeServices
# from rptest.tests.datalake.query_engine_base import QueryEngineType
# from rptest.tests.datalake.utils import supported_storage_types
# from rptest.tests.redpanda_test import RedpandaTest
# from rptest.utils.mode_checks import cleanup_on_early_exit


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
    def test_databricks_basic(self):
        dbx_ctx = DatabricksContext.from_context(self._ctx)

        cloud_cluster = self.redpanda._cloud_cluster
        client: RpCloudApiClient = cloud_cluster.public_api

        databricks_client = DatabricksWorkspace(context=self._ctx)
        bucket = f"redpanda-cloud-storage-{self._clusterId}"
        catalog_info = databricks_client.create_catalog(bucket=bucket)
        properties = {"iceberg_enabled": True}
        enable_resp = cloud_cluster.update_cluster_property_public(
            self._clusterId, properties
        )
        self.logger.debug(f"Enable iceberg response: {enable_resp}")

        # Parameters for creating Redpanda secret
        secret_id = "UNITY_CLIENT_SECRET"

        # secret_data = dbx_ctx.client_secret
        # Access credentials (client_secret, client_id) from dbx_ctx.credentials
        if isinstance(dbx_ctx.credentials, OauthCredentials):
            secret_data = dbx_ctx.credentials.client_secret
            databricks_client_id = dbx_ctx.credentials.client_id
        else:
            secret_data = dbx_ctx.credentials.token
            databricks_client_id = None

        create_resp = cloud_cluster.create_secret(secret_id, secret_data)
        self.logger.debug(f"Create secret response: {create_resp}")

        iceberg_rest_catalog_endpoint = dbx_ctx.iceberg_rest_url
        iceberg_rest_catalog_warehouse = dbx_ctx.sql_warehouse_path

        # Construct the payload for the request
        properties = {
            "iceberg_rest_catalog_endpoint": iceberg_rest_catalog_endpoint,
            "iceberg_rest_catalog_authentication_mode": "oauth2"
            if isinstance(dbx_ctx.credentials, OauthCredentials)
            else "bearer",
            "iceberg_rest_catalog_client_id": databricks_client_id,
            "iceberg_rest_catalog_client_secret": "${secrets.UNITY_CLIENT_SECRET}",
            "iceberg_rest_catalog_warehouse": catalog_info.name,
            "iceberg_catalog_type": "rest",
        }

        # Log the constructed payload for debugging
        self.logger.debug(f"Properties to be sent: {properties}")

        try:
            # Send the HTTP PATCH request
            response = client.update_cluster_property_public(
                cluster_id=cloud_cluster.current.cluster_id, properties=properties
            )

            if response:
                self.logger.debug(f"Update successful: {response.json()}")
            else:
                self.logger.error("Failed to update cluster properties.")

        except Exception as e:
            self.logger.error(
                f"An error occurred while updating cluster properties: {e}"
            )

        self.rpk = RpkTool(self.redpanda)
        test_topic = "test_topic"
        self.rpk.create_topic(test_topic)
        self.rpk.alter_topic_config(
            test_topic, TopicSpec.PROPERTY_ICEBERG_MODE, "key_value"
        )

        MESSAGE_COUNT = 100
        for i in range(MESSAGE_COUNT):
            self.rpk.produce(test_topic, f"foo {i} ", f"bar {i}")

        self.logger.debug("Waiting 1 minute...")
        time.sleep(60)
        # TODO Marat add verification, more tests and options (separate PR)

        databricks_client.stop()
