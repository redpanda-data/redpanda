# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.datalake.catalog_service_factory import filesystem_catalog_type
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_http_error


class DatalakeTableNameTest(RedpandaTest):
    def __init__(self, test_context):
        super(DatalakeTableNameTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=SISettings(test_context=test_context),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
        )

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    # For iceberg, should it be an icecube test?
    def _iceberg_dot_replacement_smoke(self, dl, topic, replacement):
        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        if replacement is not None:
            admin.patch_cluster_config(
                upsert={"iceberg_topic_name_dot_replacement": replacement}
            )

        expected_table = (
            topic.replace(".", replacement) if replacement is not None else topic
        )
        dl.create_iceberg_enabled_topic(topic)

        rpk.produce(topic, "key", "value")
        dl.wait_for_translation(topic, table_override=expected_table, msg_count=1)

        spark = dl.spark()
        tables = spark.run_query_fetch_all("SHOW TABLES IN redpanda")
        table_names = [row[1] for row in tables]
        assert expected_table in table_names, (
            f"Table {expected_table} not found. Tables: {table_names}"
        )
        assert replacement is None or topic not in table_names, (
            f"Table should not have dots: {table_names}"
        )

        result = spark.run_query_fetch_all(
            f"SELECT COUNT(*) FROM redpanda.`{expected_table}`"
        )
        assert result[0][0] == 1, f"Expected 1 row, got {result}"

        rpk.delete_topic(topic)
        wait_until(
            lambda: topic not in rpk.list_topics(),
            timeout_sec=30,
            err_msg=f"Topic {topic} was not deleted",
        )
        wait_until(
            lambda: expected_table
            not in spark.run_query_fetch_all("SHOW TABLES IN redpanda"),
            timeout_sec=30,
            err_msg=f"Table {expected_table} was not deleted",
        )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=[filesystem_catalog_type()],
    )
    def test_topic_name_dot_replacement(self, cloud_storage_type, catalog_type):
        """Test that dots in topic names are replaced in Iceberg table names"""

        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            self._iceberg_dot_replacement_smoke(dl, "test.zero", None)
            self._iceberg_dot_replacement_smoke(dl, "test.one", "")
            self._iceberg_dot_replacement_smoke(dl, "test.two", "_")

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=[filesystem_catalog_type()],
    )
    def test_dlq_table_name_dot_replacement(self, cloud_storage_type, catalog_type):
        """Test that dots in topic names are replaced in DLQ table names"""

        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            rpk = RpkTool(self.redpanda)
            admin = Admin(self.redpanda)

            admin.patch_cluster_config(
                upsert={"iceberg_topic_name_dot_replacement": "_"}
            )

            topic = "test.dlq"
            expected_table = topic.replace(".", "_")
            expected_dlq_table = f"{expected_table}~dlq"

            dl.create_iceberg_enabled_topic(
                topic, iceberg_mode="value_schema_id_prefix"
            )
            rpk.produce(topic, "key", "invalid_record_no_schema")
            dl.wait_for_translation(
                topic, table_override=expected_dlq_table, msg_count=1
            )

            spark = dl.spark()
            tables = spark.run_query_fetch_all("SHOW TABLES IN redpanda")
            table_names = [row[1] for row in tables]

            assert expected_dlq_table in table_names, (
                f"DLQ table {expected_dlq_table} not found. Tables: {table_names}"
            )
            dlq_with_dots = f"{topic}~dlq"
            assert dlq_with_dots not in table_names, (
                f"DLQ table should not have dots: {dlq_with_dots} found in {table_names}"
            )

            # Verify we can query the DLQ table
            result = spark.run_query_fetch_all(
                f"SELECT COUNT(*) FROM redpanda.`{expected_dlq_table}`"
            )
            assert result[0][0] == 1, f"Expected 1 row in DLQ table, got {result}"

            rpk.delete_topic(topic)
            wait_until(
                lambda: topic not in rpk.list_topics(),
                timeout_sec=30,
                err_msg=f"Topic {topic} was not deleted",
            )
            wait_until(
                lambda: expected_dlq_table
                not in [
                    row[1]
                    for row in spark.run_query_fetch_all("SHOW TABLES IN redpanda")
                ],
                timeout_sec=30,
                err_msg=f"DLQ table {expected_dlq_table} was not deleted",
            )


class DatalakeTableNameInvalidReplacementTest(RedpandaTest):
    @cluster(num_nodes=1)
    def test_invalid_replacement_validation(self):
        """Test that replacement strings containing dots are rejected"""

        admin = Admin(self.redpanda)

        with expect_http_error(400):
            admin.patch_cluster_config(
                upsert={"iceberg_topic_name_dot_replacement": "dot.dot"}
            )
