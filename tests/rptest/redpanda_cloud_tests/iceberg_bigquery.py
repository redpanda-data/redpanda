# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from google.api_core.exceptions import BadRequest, Conflict
from google.cloud import bigquery

from rptest.clients.installpack import InstallPackClient
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.redpanda import get_cloud_provider
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest


class CloudIcebergBigquery(RedpandaCloudTest):
    """
    Verify cluster infra/config match config profile used to launch - only applies to cloudv2
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

    def test_cloud_iceberg_bigquery(self):
        self.rpk = RpkTool(self.redpanda)

        config = [{"name": "iceberg_enabled", "value": "true"}]
        self.redpanda.set_cluster_config_overrides(self._clusterId, config)

        cloud_provider = get_cloud_provider()

        self.logger.debug(f"Cloud provider is {cloud_provider}")

        storage_uri_prefix = "gs"
        if cloud_provider == "aws":
            storage_uri_prefix = "s3"

        self.logger.debug(f"storage_uri_prefix is {storage_uri_prefix}")

        test_topic = "test_topic"
        self.rpk.create_topic(test_topic)
        self.rpk.alter_topic_config(
            test_topic, TopicSpec.PROPERTY_ICEBERG_MODE, "key_value"
        )

        MESSAGE_COUNT = 20
        for i in range(MESSAGE_COUNT):
            self.rpk.produce(test_topic, f"foo {i} ", f"bar {i}")

        self.logger.debug("Waiting 1 minute...")
        time.sleep(60)

        ## BigQuery
        PROJECT = "devprod-cicd-infra"
        REGION = "us-west1"

        DATASET_ID = f"Dataset_{storage_uri_prefix}_{self._clusterId}"
        TABLE_ID = f"{DATASET_ID}.{storage_uri_prefix}_iceberg_{test_topic}"
        STORAGE_URI = f"{storage_uri_prefix}://redpanda-cloud-storage-{self._clusterId}/redpanda-iceberg-catalog/redpanda/{test_topic}/metadata/v1.metadata.json"

        bq_client = bigquery.Client()

        def run_query(client, sql_query, description):
            """
            Executes a BigQuery SQL query and logs the results.

            :param client: BigQuery client
            :param sql_query: SQL query to execute
            :param description: Description of the query for logging
            """
            self.logger.debug(f"\nRunning query: {description}")
            self.logger.debug(f"SQL: {sql_query}")
            try:
                query_job = client.query(sql_query)
                rows = query_job.result()

                results = []
                for row in rows:
                    self.logger.debug(row)
                    results.append(row)

                return results
            except Exception as e:
                self.logger.error(f"Error executing query '{description}': {e}")
                raise

        # Create Dataset
        def create_dataset(client, project, dataset_id, region):
            dataset_ref = bigquery.Dataset(f"{project}.{dataset_id}")
            dataset_ref.location = region

            try:
                client.create_dataset(dataset_ref)  # API request
                self.logger.debug(
                    f"Dataset '{project}:{dataset_id}' successfully created."
                )
            except Conflict:
                self.logger.debug(
                    f"Dataset '{project}:{dataset_id}' already exists. Skipping creation."
                )

        # Create External Table
        def create_external_table(client, project, table_id, storage_uri):
            table_ref = f"{project}.{table_id}"
            table = bigquery.Table(table_ref)

            # Configure the external table to use Iceberg format and source URIs
            external_config = bigquery.ExternalConfig("ICEBERG")
            external_config.source_uris = [storage_uri]
            table.external_data_configuration = external_config

            try:
                client.create_table(table)
                self.logger.debug(f"External table '{table_ref}' successfully created.")
            except Conflict:
                self.logger.debug(
                    f"External table '{table_ref}' already exists. Skipping creation."
                )
            except BadRequest as e:
                self.logger.debug(f"Error creating external table: {e.message}")

        def delete_dataset(client, project, dataset_id):
            dataset_ref = f"{project}.{dataset_id}"
            try:
                client.delete_dataset(
                    dataset_ref, delete_contents=True, not_found_ok=True
                )
                self.logger.debug(f"Dataset '{dataset_ref}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Error deleting dataset '{dataset_ref}': {e}")

        create_dataset(bq_client, PROJECT, DATASET_ID, REGION)
        create_external_table(bq_client, PROJECT, TABLE_ID, STORAGE_URI)

        select_query = f"SELECT * FROM `{PROJECT}.{TABLE_ID}` LIMIT 10;"
        count_query = f"SELECT COUNT(*) AS total_rows FROM `{PROJECT}.{TABLE_ID}`;"

        try:
            # Test SELECT query
            try:
                select_results = run_query(
                    bq_client, select_query, "SELECT query (LIMIT 10)"
                )
                assert select_results, "SELECT query returned no results!"
                self.logger.info(f"SELECT query returned {len(select_results)} rows.")
                # TODO Add assertions for specific data formats
            except Exception as e:
                self.logger.error(f"Failed to execute SELECT query: {e}")
                raise

            # Test COUNT query
            try:
                count_results = run_query(bq_client, count_query, "COUNT query")
                total_rows = count_results[0].total_rows if count_results else 0

                assert total_rows == MESSAGE_COUNT, (
                    f"Expected {MESSAGE_COUNT} rows, but got {total_rows}!"
                )
                self.logger.info(
                    f"COUNT query result: {total_rows} rows, expected {MESSAGE_COUNT}."
                )

            except Exception as e:
                self.logger.error(f"Failed to execute COUNT query: {e}")
                raise

            # Delete all topics
            topics = self.rpk.list_topics()
            for topic in topics:
                self.rpk.delete_topic(topic)

        finally:
            # Always delete the dataset, even if an exception occurred
            delete_dataset(bq_client, PROJECT, DATASET_ID)
