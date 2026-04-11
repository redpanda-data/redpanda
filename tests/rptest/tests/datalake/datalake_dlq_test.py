# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from enum import Enum
from typing import Optional

from ducktape.mark import matrix

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.serde_client_utils import SchemaType, SerdeClientType
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    MetricsEndpoint,
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.services.serde_client import SerdeClient
from rptest.tests.datalake.catalog_service_factory import (
    filesystem_catalog_type,
    supported_catalog_types,
)
from rptest.tests.datalake.datalake_e2e_test import (
    SPARK_RP_FIELD_TYPE,
    TRINO_RP_FIELD_TYPE,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


class IcebergInvalidRecordAction(str, Enum):
    DROP = "drop"
    DLQ_TABLE = "dlq_table"

    def __str__(self):
        return self.value


class IcebergInvalidRecordCause(str, Enum):
    FAILED_KAFKA_SCHEMA_RESOLUTION = "failed_kafka_schema_resolution"
    FAILED_DATA_TRANSLATION = "failed_data_translation"
    FAILED_ICEBERG_SCHEMA_RESOLUTION = "failed_iceberg_schema_resolution"

    def __str__(self):
        return self.value


class DatalakeDLQPropertiesTest(RedpandaTest):
    def __init__(self, test_context):
        super(DatalakeDLQPropertiesTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            extra_rp_conf={
                "iceberg_enabled": "true",
            },
            si_settings=SISettings(test_context=test_context),
        )

        self.rpk = RpkTool(self.redpanda)

    def set_cluster_config(self, key: str, value):
        self.rpk.cluster_config_set(key, value)

    def set_topic_properties(self, key: str, value):
        self.rpk.alter_topic_config(self.topic_name, key, value)

    def validate_topic_configs(self, action: IcebergInvalidRecordAction):
        configs = self.rpk.describe_topic_configs(self.topic_name)
        assert configs[TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION][0] == str(
            action
        ), (
            f"Expected {action} but got {configs[TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION]}"
        )

    @cluster(num_nodes=1)
    def test_properties(self):
        action_conf = "iceberg_invalid_record_action"

        self.admin = self.redpanda._admin

        # Topic with custom properties at creation.
        topic = TopicSpec()
        self.topic_name = topic.name
        self.rpk.create_topic(
            topic=topic.name,
            config={
                # Enable iceberg to make the iceberg.invalid.record.action property visible.
                TopicSpec.PROPERTY_ICEBERG_MODE: "key_value",
                TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION: "drop",
            },
        )

        self.validate_topic_configs(IcebergInvalidRecordAction.DROP)

        # New topic with defaults
        topic = TopicSpec()
        self.topic_name = topic.name
        self.rpk.create_topic(
            topic=topic.name,
            config={
                # Enable iceberg to make the iceberg.invalid.record.action property visible.
                TopicSpec.PROPERTY_ICEBERG_MODE: "key_value",
            },
        )

        # Validate cluster defaults
        self.validate_topic_configs(IcebergInvalidRecordAction.DLQ_TABLE)

        # Changing cluster level configs
        self.set_cluster_config(action_conf, IcebergInvalidRecordAction.DROP)
        self.validate_topic_configs(IcebergInvalidRecordAction.DROP)

        # Change topic property
        self.set_topic_properties(
            TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION,
            IcebergInvalidRecordAction.DLQ_TABLE,
        )
        self.validate_topic_configs(IcebergInvalidRecordAction.DLQ_TABLE)

    @cluster(num_nodes=1)
    def test_create_bad_properties(self):
        topic = TopicSpec()

        with expect_exception(
            RpkException,
            lambda e: "Invalid property value." in e.msg and "INVALID_CONFIG" in e.msg,
        ):
            self.rpk.create_topic(
                topic=topic.name,
                config={
                    TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION: "asd",
                },
            )

        # Create the topic with default property and alter to invalid value
        self.rpk.create_topic(topic=topic.name)
        with expect_exception(
            RpkException,
            lambda e: "unable to parse property redpanda.iceberg.invalid.record.action value"
            in e.msg
            and "INVALID_CONFIG" in e.msg,
        ):
            self.rpk.alter_topic_config(
                topic.name, TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION, "asd"
            )


class DatalakeDLQTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeDLQTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def _get_serde_client(
        self,
        schema_type: SchemaType,
        client_type: SerdeClientType,
        topic: str,
        count: int,
        skip_known_types: Optional[bool] = None,
        subject_name_strategy: Optional[str] = None,
        payload_class: Optional[str] = None,
        compression_type: Optional[TopicSpec.CompressionTypes] = None,
    ):
        schema_reg = self.redpanda.schema_reg().split(",", 1)[0]
        sec_cfg = self.redpanda.kafka_client_security().to_dict()

        return SerdeClient(
            self.test_context,
            self.redpanda.brokers(),
            schema_reg,
            schema_type,
            client_type,
            count,
            topic=topic,
            security_config=sec_cfg if sec_cfg else None,
            skip_known_types=skip_known_types,
            subject_name_strategy=subject_name_strategy,
            payload_class=payload_class,
            compression_type=compression_type,
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
    )
    def test_no_dlq_table_for_valid_records(self, cloud_storage_type, query_engine):
        """
        Produce only valid records and verify that no DLQ table is created.
        Testing with a single query engine because this is a common behavior.
        """
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=filesystem_catalog_type(),
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix"
            )

            avro_serde_client = self._get_serde_client(
                SchemaType.AVRO, SerdeClientType.Golang, self.topic_name, 1
            )
            avro_serde_client.start()
            avro_serde_client.wait()
            avro_serde_client.free()

            dl.wait_for_iceberg_table("redpanda", self.topic_name, 30, 5)

            # No DLQ table created.
            assert dl.num_tables() == 1, "Expected only 1 table in catalog"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
    )
    def test_no_dlq_table_for_key_value_mode(self, cloud_storage_type, query_engine):
        """
        Produce records to a topic with `key_value` mode and verify that no
        DLQ table is created. This is because in `key_value` mode all records
        are considered valid.
        Testing with a single query engine because this is a common behavior.
        """
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=filesystem_catalog_type(),
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, iceberg_mode="key_value")

            dl.produce_to_topic(self.topic_name, 1, 1)

            dl.wait_for_iceberg_table("redpanda", self.topic_name, 30, 5)

            # No DLQ table created.
            assert dl.num_tables() == 1, "Expected only 1 table in catalog"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=supported_catalog_types(),
    )
    def test_dlq_table_for_invalid_records(
        self, cloud_storage_type, query_engine, catalog_type
    ):
        """
        Produce records with no schema to `value_schema_id_prefix` mode topic.
        These records will fail translate and should be written to DLQ table.

        Testing with multiple query engines to make sure that DLQ table naming
        is compatible. I.e. the `~` character is accepted.
        """
        dlq_table_name = f"{self.topic_name}~dlq"
        num_records = 10

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix"
            )

            dl.produce_to_topic(self.topic_name, 1, num_records)
            dl.wait_for_translation(
                self.topic_name, num_records, table_override=dlq_table_name
            )

            # Only the DLQ table got created.
            assert dl.num_tables() == 1, "Expected only 1 table in catalog"

            if query_engine == QueryEngineType.TRINO:
                trino = dl.trino()
                trino_expected_out = [
                    TRINO_RP_FIELD_TYPE,
                    ("value", "varbinary", "", ""),
                ]
                trino_describe_out = trino.run_query_fetch_all(
                    f"describe redpanda.{trino.escape_identifier(dlq_table_name)}"
                )
                assert trino_describe_out == trino_expected_out, str(trino_describe_out)
            else:
                spark = dl.spark()
                spark_expected_out = [
                    SPARK_RP_FIELD_TYPE,
                    ("value", "binary", None),
                    ("", "", ""),
                    ("# Partitioning", "", ""),
                    ("Part 0", "hours(redpanda.timestamp)", ""),
                ]
                spark_describe_out = spark.run_query_fetch_all(
                    f"describe redpanda.{spark.escape_identifier(dlq_table_name)}"
                )
                assert spark_describe_out == spark_expected_out, str(spark_describe_out)

            verifier = DatalakeVerifier(
                self.redpanda,
                self.topic_name,
                dl.query_engine(query_engine),
                table_override=dlq_table_name,
            )
            verifier.start()
            verifier.wait()

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        # Lightweight matrix as we only care about custom suffix behavior here.
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[filesystem_catalog_type()],
    )
    def test_dlq_table_for_invalid_records_custom_suffix(
        self, cloud_storage_type, query_engine, catalog_type
    ):
        """
        Test DLQ table with custom suffix configured via
        `iceberg_dlq_table_suffix` Redpanda config.
        """
        dlq_table_suffix = "__panda_dlq"
        num_records = 10

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[query_engine],
        ) as dl:
            self.redpanda.set_cluster_config(
                {"iceberg_dlq_table_suffix": dlq_table_suffix}
            )

            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix"
            )

            dl.produce_to_topic(self.topic_name, 1, num_records)
            dl.wait_for_translation(
                self.topic_name,
                num_records,
                table_override=f"{self.topic_name}{dlq_table_suffix}",
            )

            # No other tables created.
            assert dl.num_tables() == 1, "Expected only 1 table in catalog"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=supported_catalog_types(),
    )
    def test_dlq_table_for_mixed_records(
        self, cloud_storage_type, query_engine, catalog_type
    ):
        """
        Produce a mix of valid and invalid records to a `value_schema_id_prefix`
        mode topic. Valid records should be written to the main table and
        invalid records should be written to the DLQ table.

        It is important to test with both filesystem catalog mode and w/o
        because their behavior in assigning field ids is different and was
        found to cause translation issues.
        """
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix"
            )

            num_valid_per_iter = 7
            num_invalid_per_iter = 5
            num_iter = 3

            for _ in range(num_iter):
                # Produce valid records.
                avro_serde_client = self._get_serde_client(
                    SchemaType.AVRO,
                    SerdeClientType.Golang,
                    self.topic_name,
                    num_valid_per_iter,
                )
                avro_serde_client.start()
                avro_serde_client.wait()
                avro_serde_client.free()

                # Produce invalid records.
                dl.produce_to_topic(self.topic_name, 1, num_invalid_per_iter)

            # Wait for valid records to be written to the table.
            dl.wait_for_translation(
                self.topic_name,
                num_valid_per_iter * num_iter,
            )

            dl.wait_for_translation(
                self.topic_name,
                num_invalid_per_iter * num_iter,
                table_override=f"{self.topic_name}~dlq",
            )

            invalid_schema_res = self._invalid_records_metric_sum(
                self.topic_name,
                IcebergInvalidRecordCause.FAILED_KAFKA_SCHEMA_RESOLUTION,
            )
            assert invalid_schema_res >= num_iter * num_invalid_per_iter, (
                f"Expected {num_iter * num_invalid_per_iter} invalid records due to failed schema resolution but got {invalid_schema_res}"
            )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        use_topic_property=[True, False],
        action=[IcebergInvalidRecordAction.DROP, IcebergInvalidRecordAction.DLQ_TABLE],
    )
    def test_invalid_record_action(
        self, cloud_storage_type, query_engine, use_topic_property, action
    ):
        topic_config = {}
        if use_topic_property:
            # Topic property.
            topic_config[TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION] = str(action)
        else:
            # Cluster default.
            self.redpanda.add_extra_rp_conf(
                {"iceberg_invalid_record_action": str(action)}
            )

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=filesystem_catalog_type(),
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                iceberg_mode="value_schema_id_prefix",
                config=topic_config,
            )

            num_valid_per_iter = 7
            num_invalid_per_iter = 5
            num_iter = 3

            for _ in range(num_iter):
                # Produce valid records.
                avro_serde_client = self._get_serde_client(
                    SchemaType.AVRO,
                    SerdeClientType.Golang,
                    self.topic_name,
                    num_valid_per_iter,
                )
                avro_serde_client.start()
                avro_serde_client.wait()
                avro_serde_client.free()

                # Produce invalid records.
                dl.produce_to_topic(self.topic_name, 1, num_invalid_per_iter)

            # Wait for valid records to be written to the table.
            dl.wait_for_translation(
                self.topic_name,
                num_valid_per_iter * num_iter,
            )

            if action == IcebergInvalidRecordAction.DROP:
                # Only the main table should be created if invalid records are dropped.
                assert dl.num_tables() == 1, (
                    "Expected only 1 table in catalog as invalid records were dropped"
                )
            elif action == IcebergInvalidRecordAction.DLQ_TABLE:
                dl.wait_for_translation(
                    self.topic_name,
                    num_invalid_per_iter * num_iter,
                    table_override=f"{self.topic_name}~dlq",
                )
            else:
                assert False, f"Unhandled action: {action}"

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        use_topic_property=[True, False],
    )
    def test_invalid_record_action_runtime_change(
        self, cloud_storage_type, query_engine, use_topic_property
    ):
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=filesystem_catalog_type(),
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix"
            )

            num_valid_per_iter = 7
            num_invalid_per_iter = 5
            num_iter = 3

            for _ in range(num_iter):
                # Produce valid records.
                avro_serde_client = self._get_serde_client(
                    SchemaType.AVRO,
                    SerdeClientType.Golang,
                    self.topic_name,
                    num_valid_per_iter,
                )
                avro_serde_client.start()
                avro_serde_client.wait()
                avro_serde_client.free()

                # Produce invalid records.
                dl.produce_to_topic(self.topic_name, 1, num_invalid_per_iter)

            # Wait for valid records to be written to the table.
            dl.wait_for_translation(self.topic_name, num_valid_per_iter * num_iter)

            # Wait for invalid records to be written to the DLQ table.
            dl.wait_for_translation(
                self.topic_name,
                num_invalid_per_iter * num_iter,
                table_override=f"{self.topic_name}~dlq",
            )

            rpk = RpkTool(self.redpanda)
            if use_topic_property:
                # Topic property.
                rpk.alter_topic_config(
                    self.topic_name,
                    TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION,
                    str(IcebergInvalidRecordAction.DROP),
                )
            else:
                rpk.cluster_config_set(
                    "iceberg_invalid_record_action",
                    str(IcebergInvalidRecordAction.DROP),
                )

            # Wait a bit for the change to take effect. This is a bit hacky but
            # allows the test to be simpler.
            time.sleep(5)

            # Produce more invalid records and observe that they are drooped
            # but valid records are still written.
            for _ in range(num_iter):
                # Produce valid records.
                avro_serde_client = self._get_serde_client(
                    SchemaType.AVRO,
                    SerdeClientType.Golang,
                    self.topic_name,
                    num_valid_per_iter,
                )
                avro_serde_client.start()
                avro_serde_client.wait()
                avro_serde_client.free()

                # Produce invalid records.
                dl.produce_to_topic(self.topic_name, 1, num_invalid_per_iter)

            # Wait for valid records to be written to the table.
            dl.wait_for_translation(
                self.topic_name,
                2 * num_valid_per_iter * num_iter,
            )

            dlq_count = dl.query_engine(query_engine).count_table(
                "redpanda", f"{self.topic_name}~dlq"
            )
            assert dlq_count == num_iter * num_invalid_per_iter, (
                f"Didn't expect additional records in DLQ table: Expected {num_iter * num_invalid_per_iter} but got {dlq_count}"
            )

    def _invalid_records_metric_sum(
        self, topic: str, cause: IcebergInvalidRecordCause
    ) -> float:
        metric_name = "redpanda_iceberg_translation_invalid_records_total"

        samples = self.redpanda.metrics_sample(
            metric_name, self.redpanda.nodes, MetricsEndpoint.PUBLIC_METRICS
        )
        if not samples:
            return 0

        return sum(
            [
                s.value
                for s in samples.label_filter(
                    {
                        "redpanda_topic": topic,
                        **({"cause": str(cause)} if cause is not None else {}),
                    }
                ).samples
            ]
        )


class DatalakeDLQMultinodeTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeDLQMultinodeTest, self).__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 1000,
            },
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # Redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=6)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_dlq_table_with_multiple_nodes(self, cloud_storage_type, catalog_type):
        """
        Produces invalid records to a `value_schema_id_prefix` mode topic,
        expecting records to be written to the DLQ table.

        This serves as a smoke test for multi-node clusters, e.g. ensuring
        remote dispatch for the DLQ table works correctly.
        """
        dlq_table_name = f"{self.topic_name}~dlq"
        num_records = 10
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix"
            )

            dl.produce_to_topic(self.topic_name, 1, num_records)
            dl.wait_for_translation(
                self.topic_name, num_records, table_override=dlq_table_name
            )
            assert dl.num_tables() == 1, "Expected only 1 table in catalog"
