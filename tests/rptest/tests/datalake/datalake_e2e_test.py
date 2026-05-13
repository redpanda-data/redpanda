# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import datetime
import json
import random
import re
import time
from random import randint
from typing import Any, Callable

from confluent_kafka import Producer, avro
from confluent_kafka.avro import AvroProducer
from ducktape.mark import ignore, matrix
from ducktape.utils.util import wait_until
from google import protobuf
from google.protobuf import json_format as pb_json_format
from google.protobuf import message_factory
from google.protobuf import text_format as pb_text_format

from rptest.clients.admin import v2 as admin_v2
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import (
    CloudStorageType,
    MetricsEndpoint,
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.tests.datalake.catalog_service_factory import (
    supported_catalog_types,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode

FieldTuple = tuple[str | None, ...]

TRINO_RP_FIELD_TYPE: FieldTuple = (
    "redpanda",
    "row(partition integer, offset bigint, timestamp timestamp(6) with time zone, headers array(row(key varchar, value varbinary)), key varbinary, timestamp_type integer)",
    "",
    "",
)

SPARK_RP_FIELD_TYPE: FieldTuple = (
    "redpanda",
    "struct<partition:int,offset:bigint,timestamp:timestamp,headers:array<struct<key:string,value:binary>>,key:binary,timestamp_type:int>",
    None,
)


class AvroSchema:
    def __init__(
        self,
        schema_str: str,
        record_generator: Callable[[Any], Any],
        expected_trino: list[FieldTuple],
        expected_spark: list[FieldTuple],
    ):
        self.schema_str: str = schema_str
        self.record_generator: Callable[[Any], Any] = record_generator
        self.expected_trino: list[FieldTuple] = [TRINO_RP_FIELD_TYPE] + expected_trino
        self.expected_spark: list[FieldTuple] = [SPARK_RP_FIELD_TYPE] + expected_spark

    def generate_record(self, t):
        return self.record_generator(t)


avro_schema_str = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}}
    ]
}
"""

avro_schema_with_enum_str = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "suit", "type": {"name": "suit", "type": "enum", "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}}
    ]
}
"""

avro_schema_with_array_str = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "arr", "type": {"type": "array", "items": "long"}}
    ]
}
"""

avro_schema_with_map_str = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "kv", "type": {"type": "map", "values": "long"}}
    ]
}
"""

avro_schema_with_union_str = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "str_or_long", "type": ["string", "long"]}
    ]
}
"""

avro_schema_with_null_union_str = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "optional_long", "type": ["null", "long"]}
    ]
}
"""

AVRO_SCHEMA_TEST_CASES = {
    "primitive": AvroSchema(
        schema_str="""{"type": "long", "name": "a_number"}""",
        record_generator=lambda t: int(t),
        expected_trino=[
            ("root", "bigint", "", ""),
        ],
        expected_spark=[
            ("root", "bigint", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
    "basic": AvroSchema(
        schema_str=avro_schema_str,
        record_generator=lambda t: {"number": int(t), "timestamp_us": int(t * 1000000)},
        expected_trino=[
            ("number", "bigint", "", ""),
            ("timestamp_us", "timestamp(6) with time zone", "", ""),
        ],
        expected_spark=[
            ("number", "bigint", None),
            ("timestamp_us", "timestamp", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
    "enum": AvroSchema(
        schema_str=avro_schema_with_enum_str,
        record_generator=lambda t: {
            "number": int(t),
            "timestamp_us": int(t * 1000000),
            "suit": random.choice(["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]),
        },
        expected_trino=[
            ("number", "bigint", "", ""),
            ("timestamp_us", "timestamp(6) with time zone", "", ""),
            ("suit", "varchar", "", ""),
        ],
        expected_spark=[
            ("number", "bigint", None),
            ("timestamp_us", "timestamp", None),
            ("suit", "string", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
    "array": AvroSchema(
        schema_str=avro_schema_with_array_str,
        record_generator=lambda t: {
            "number": int(t),
            "timestamp_us": int(t * 1000000),
            "arr": [int(t) + i for i in range(random.randint(0, 10))],
        },
        expected_trino=[
            ("number", "bigint", "", ""),
            ("timestamp_us", "timestamp(6) with time zone", "", ""),
            ("arr", "array(bigint)", "", ""),
        ],
        expected_spark=[
            ("number", "bigint", None),
            ("timestamp_us", "timestamp", None),
            ("arr", "array<bigint>", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
    "map": AvroSchema(
        schema_str=avro_schema_with_map_str,
        record_generator=lambda t: {
            "number": int(t),
            "timestamp_us": int(t * 1000000),
            "kv": {str(t): int(t)},
        },
        expected_trino=[
            ("number", "bigint", "", ""),
            ("timestamp_us", "timestamp(6) with time zone", "", ""),
            ("kv", "map(varchar, bigint)", "", ""),
        ],
        expected_spark=[
            ("number", "bigint", None),
            ("timestamp_us", "timestamp", None),
            ("kv", "map<string,bigint>", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
    "union": AvroSchema(
        schema_str=avro_schema_with_union_str,
        record_generator=lambda t: {
            "number": int(t),
            "timestamp_us": int(t * 1000000),
            "str_or_long": random.choice([str(t), int(t)]),
        },
        expected_trino=[
            ("number", "bigint", "", ""),
            ("timestamp_us", "timestamp(6) with time zone", "", ""),
            ("str_or_long", "row(string varchar, long bigint)", "", ""),
        ],
        expected_spark=[
            ("number", "bigint", None),
            ("timestamp_us", "timestamp", None),
            ("str_or_long", "struct<string:string,long:bigint>", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
    "null_union": AvroSchema(
        schema_str=avro_schema_with_null_union_str,
        record_generator=lambda t: {
            "number": int(t),
            "timestamp_us": int(t * 1000000),
            "optional_long": random.choice([None, int(t)]),
        },
        expected_trino=[
            ("number", "bigint", "", ""),
            ("timestamp_us", "timestamp(6) with time zone", "", ""),
            ("optional_long", "bigint", "", ""),
        ],
        expected_spark=[
            ("number", "bigint", None),
            ("timestamp_us", "timestamp", None),
            ("optional_long", "bigint", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
}


class JsonSchemaTestCase:
    def __init__(
        self,
        schema_str: str,
        record_generator: Callable[[Any], Any],
        expected_spark: list[FieldTuple] | None,
        skip_encoding: bool = False,
        dlq_cause: str | None = None,
    ):
        """
        :param skip_encoding: If True, the record generator will return a JSON string
            that needs to be produced as-is. If False, the record generator will return
            a dict that needs to be converted to JSON before producing.
        """
        self.schema_str: str = schema_str
        self.record_generator: Callable[[Any], Any] = record_generator
        self.expected_spark: list[FieldTuple] | None = (
            [SPARK_RP_FIELD_TYPE] + expected_spark
            if expected_spark is not None
            else None
        )
        self.dlq_cause: str | None = dlq_cause

        self._skip_encoding: bool = skip_encoding

    def generate_record(self, t):
        if self._skip_encoding:
            return self.record_generator(t)
        else:
            return json.dumps(self.record_generator(t))


JSON_SCHEMA_TEST_CASES = {
    "basic": JsonSchemaTestCase(
        schema_str="""{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "number": {"type": "integer"},
                "timestamp_us": {"type": "integer"}
            },
            "required": ["number", "timestamp_us"]
        }""",
        record_generator=lambda t: {"number": int(t), "timestamp_us": int(t * 1000000)},
        expected_spark=[
            ("number", "bigint", None),
            ("timestamp_us", "bigint", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
    "array_items": JsonSchemaTestCase(
        schema_str="""{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "numbers": {
                    "type": "array",
                    "items": {"type": ["null", "integer"]}
                }
            },
            "required": ["numbers"]
        }""",
        record_generator=lambda t: {
            "numbers": [
                random.choice([None, int(t) + i]) for i in range(randint(0, 10))
            ]
        },
        expected_spark=[
            ("numbers", "array<bigint>", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ],
    ),
}

JSON_SCHEMA_DLQ_TEST_CASES = {
    "bad_schema_dialect": JsonSchemaTestCase(
        schema_str="""{
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "number": {"type": "integer"}
            }
        }""",
        record_generator=lambda t: {
            "number": int(t),
        },
        expected_spark=None,
        # This cause is slightly misleading.
        dlq_cause="failed_kafka_schema_resolution",
    ),
    "mismatched_types": JsonSchemaTestCase(
        schema_str="""{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "number": {"type": "integer"}
            }
        }""",
        record_generator=lambda t: {
            "number": str(t),
        },
        expected_spark=None,
        dlq_cause="failed_data_translation",
    ),
    "bad_input": JsonSchemaTestCase(
        schema_str="""{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "number": {"type": "integer"}
            }
        }""",
        # Incomplete JSON on purpose to trigger a parsing failure.
        record_generator=lambda t: f"""{{"number": {int(t)}, """,
        expected_spark=None,
        skip_encoding=True,
        dlq_cause="failed_data_translation",
    ),
}


class DatalakeE2ETests(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeE2ETests, self).__init__(
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

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=supported_catalog_types(),
    )
    def test_e2e_basic(self, cloud_storage_type, query_engine, catalog_type):
        # Create a topic
        # Produce some events
        # Ensure they end up in datalake
        count = 100
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=supported_catalog_types(),
    )
    def test_avro_schema(self, cloud_storage_type, query_engine, catalog_type):
        count = 100

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            for test_case, schema in AVRO_SCHEMA_TEST_CASES.items():
                self.redpanda.logger.debug(f"Running avro schema test case {test_case}")
                test_case_topic_name = f"{test_case}_test_case"
                table_name = f"redpanda.{test_case_topic_name}"
                dl.create_iceberg_enabled_topic(
                    test_case_topic_name, iceberg_mode="value_schema_id_prefix"
                )
                raw_schema = avro.loads(schema.schema_str)
                producer = AvroProducer(
                    {
                        "bootstrap.servers": self.redpanda.brokers(),
                        "schema.registry.url": self.redpanda.schema_reg().split(",")[0],
                    },
                    default_value_schema=raw_schema,
                )
                for _ in range(count):
                    t = time.time()
                    record = schema.generate_record(t)
                    producer.produce(topic=test_case_topic_name, value=record)
                producer.flush()
                dl.wait_for_translation(test_case_topic_name, msg_count=count)

                if query_engine == QueryEngineType.TRINO:
                    trino = dl.trino()
                    trino_expected_out = schema.expected_trino
                    trino_describe_out = trino.run_query_fetch_all(
                        f"describe {table_name}"
                    )
                    assert trino_describe_out == trino_expected_out, str(
                        trino_describe_out
                    )
                else:
                    spark = dl.spark()
                    spark_expected_out = schema.expected_spark
                    spark_describe_out = spark.run_query_fetch_all(
                        f"describe {table_name}"
                    )
                    assert spark_describe_out == spark_expected_out, str(
                        spark_describe_out
                    )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_avro_map_values(self, cloud_storage_type, query_engine, catalog_type):
        """Verify avro map<string, long> values round-trip end-to-end."""
        count = 100
        topic = "avro_map_test_case"
        table = f"redpanda.{topic}"
        schema_str = """
        {
            "type": "record",
            "namespace": "com.redpanda.examples.avro",
            "name": "MapTest",
            "fields": [
                {"name": "kv", "type": {"type": "map", "values": "long"}}
            ]
        }
        """

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            dl.create_iceberg_enabled_topic(
                topic, iceberg_mode="value_schema_id_prefix"
            )
            producer = AvroProducer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "schema.registry.url": self.redpanda.schema_reg().split(",")[0],
                },
                default_value_schema=avro.loads(schema_str),
            )
            for i in range(count):
                producer.produce(topic=topic, value={"kv": {"k": i}})
            producer.flush()
            dl.wait_for_translation(topic, msg_count=count)

            engine = dl.trino() if query_engine == QueryEngineType.TRINO else dl.spark()
            rows = engine.run_query_fetch_all(
                f"select sum(element_at(kv, 'k')) from {table}"
            )
            expected = sum(range(count))
            assert rows[0][0] == expected, f"expected sum={expected}, got {rows}"

    # Note: nothing unique about this test so run it with single catalog/query engine.
    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_json_schema(self, cloud_storage_type, query_engine, catalog_type):
        count = 100

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            for tc_name, tc_data in JSON_SCHEMA_TEST_CASES.items():
                self.logger.info(f"Running JSON schema test case {tc_name}")
                test_case_topic_name = f"{tc_name}_test_case"
                table_name = f"redpanda.{test_case_topic_name}"
                dl.create_iceberg_enabled_topic(
                    test_case_topic_name, iceberg_mode="value_schema_latest"
                )

                self.logger.info(f"Creating schema for topic {test_case_topic_name}")
                rpk = RpkTool(self.redpanda)
                rpk.create_schema_from_str(
                    subject=f"{test_case_topic_name}-value",
                    schema=tc_data.schema_str,
                    schema_suffix="json",
                )

                self.logger.info(f"Producing records for topic {test_case_topic_name}")
                producer = Producer({"bootstrap.servers": self.redpanda.brokers()})
                for i in range(count):
                    t = time.time()
                    producer.produce(
                        topic=test_case_topic_name, value=tc_data.generate_record(t)
                    )
                producer.flush()

                self.logger.info(
                    f"Waiting for translation for topic {test_case_topic_name}"
                )
                dl.wait_for_translation(test_case_topic_name, msg_count=count)

                spark = dl.spark()
                spark_expected_out = tc_data.expected_spark
                spark_describe_out = spark.run_query_fetch_all(f"describe {table_name}")
                assert spark_describe_out == spark_expected_out, str(spark_describe_out)

    # We use json because it is the only format that supports unicode
    # characters in field names and we want to ensure that everyone
    # interoperates correctly with unicode.
    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=supported_catalog_types(),
    )
    def test_field_names_compat(self, cloud_storage_type, query_engine, catalog_type):
        """
        Test that field names with dots, mixed case, and unicode characters are
        handled correctly. We are mainly interested in the catalog preserving
        the original field names so that schema evolution (field name matching)
        works.
        Known offender is AWS Glue which forces field names to be lowercase thus
        breaking schema evolution. Not yet tested here but should be in future.
        """
        count = 100

        schema_str = """{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "my_🍀_number": {"type": "integer"},
                "col.with.dots": {"type": "integer"},
                "MixedCase": {"type": "integer"}
            },
            "required": ["my_🍀_number", "col.with.dots", "MixedCase"]
        }"""

        def record_generator(t):
            return {
                "my_🍀_number": int(t),
                "col.with.dots": int(t),
                "MixedCase": int(t),
            }

        expected_spark = [
            SPARK_RP_FIELD_TYPE,
            ("MixedCase", "bigint", None),
            ("col.with.dots", "bigint", None),
            ("my_🍀_number", "bigint", None),
            ("", "", ""),
            ("# Partitioning", "", ""),
            ("Part 0", "hours(redpanda.timestamp)", ""),
        ]

        expected_trino = [
            TRINO_RP_FIELD_TYPE,
            # Trino forces lowercase column names. Since this is just a query
            # engine it does not affect the schema evolution in the catalog.
            # However, if the table contains both `MixedCase` and `mixedcase`
            # columns then Trino fails to query the table.
            ("mixedcase", "bigint", "", ""),
            ("col.with.dots", "bigint", "", ""),
            ("my_🍀_number", "bigint", "", ""),
        ]

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            table_name = f"redpanda.{self.topic_name}"
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_latest"
            )

            self.logger.info(f"Creating schema for topic {self.topic_name}")
            rpk = RpkTool(self.redpanda)
            rpk.create_schema_from_str(
                subject=f"{self.topic_name}-value",
                schema=schema_str,
                schema_suffix="json",
            )

            self.logger.info(f"Producing records for topic {self.topic_name}")
            producer = Producer({"bootstrap.servers": self.redpanda.brokers()})
            for _ in range(count):
                t = time.time()
                record = record_generator(t)
                producer.produce(topic=self.topic_name, value=json.dumps(record))
            producer.flush()

            self.logger.info(f"Waiting for translation for topic {self.topic_name}")
            dl.wait_for_translation(self.topic_name, msg_count=count)

            if query_engine == QueryEngineType.SPARK:
                spark = dl.spark()
                spark_describe_out = spark.run_query_fetch_all(f"describe {table_name}")

                assert spark_describe_out == expected_spark, str(spark_describe_out)

                with spark.run_query(
                    f"SELECT `my_🍀_number`, `col.with.dots`, `MixedCase` FROM {table_name} LIMIT 1"
                ) as cursor:
                    assert cursor.description == [
                        ("my_🍀_number", "BIGINT_TYPE", None, None, None, None, True),
                        ("col.with.dots", "BIGINT_TYPE", None, None, None, None, True),
                        ("MixedCase", "BIGINT_TYPE", None, None, None, None, True),
                    ]
                    row = cursor.fetchone()
                    assert row is not None
                    assert row[0] > 0
                    assert row[1] > 0
            elif query_engine == QueryEngineType.TRINO:
                trino = dl.trino()
                trino_describe_out = trino.run_query_fetch_all(f"describe {table_name}")

                assert trino_describe_out == expected_trino, str(trino_describe_out)

                with trino.run_query(
                    f"""SELECT "my_🍀_number", "col.with.dots", "MixedCase" FROM {table_name} LIMIT 1"""
                ) as cursor:
                    assert cursor.description == [
                        ("my_🍀_number", "bigint", None, None, None, None, True),
                        ("col.with.dots", "bigint", None, None, None, None, True),
                        ("MixedCase", "bigint", None, None, None, None, True),
                    ]
                    row = cursor.fetchone()
                    assert row is not None
                    assert row[0] > 0
                    assert row[1] > 0
            else:
                raise RuntimeError(
                    f"Unsupported query engine {query_engine} for this test"
                )

    # Note: nothing unique about this test so run it with single catalog/query engine.
    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_json_schema_dlq(self, cloud_storage_type, query_engine, catalog_type):
        count = 100

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            for tc_name, tc_data in JSON_SCHEMA_DLQ_TEST_CASES.items():
                self.logger.info(f"Running JSON schema dlq test case {tc_name}")
                test_case_topic_name = f"{tc_name}_test_case"
                dl.create_iceberg_enabled_topic(
                    test_case_topic_name, iceberg_mode="value_schema_latest"
                )

                self.logger.info(f"Creating schema for topic {test_case_topic_name}")
                rpk = RpkTool(self.redpanda)
                rpk.create_schema_from_str(
                    subject=f"{test_case_topic_name}-value",
                    schema=tc_data.schema_str,
                    schema_suffix="json",
                )

                self.logger.info(f"Producing records for topic {test_case_topic_name}")
                producer = Producer({"bootstrap.servers": self.redpanda.brokers()})
                for i in range(count):
                    t = time.time()
                    producer.produce(
                        topic=test_case_topic_name, value=tc_data.generate_record(t)
                    )
                producer.flush()

                self.logger.info(
                    f"Waiting for translation for dlq table for {test_case_topic_name}"
                )
                dl.wait_for_translation(
                    test_case_topic_name,
                    msg_count=count,
                    table_override=f"{test_case_topic_name}~dlq",
                )

                topic_leader = self.redpanda.partitions(test_case_topic_name)[0].leader

                assert tc_data.dlq_cause is not None

                # Check that DLQ cause is failed translation rather than some
                # other cause like missing schema which would indicate a bug in
                # the test.
                MetricCheck(
                    self.redpanda.logger,
                    self.redpanda,
                    topic_leader,
                    ["redpanda_iceberg_translation_invalid_records_total"],
                    labels={
                        "redpanda_namespace": "kafka",
                        "redpanda_topic": test_case_topic_name,
                        "redpanda_cause": tc_data.dlq_cause,
                    },
                    reduce=sum,
                    metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
                ).expect(
                    [
                        (
                            "redpanda_iceberg_translation_invalid_records_total",
                            lambda _, val: val == count,
                        )
                    ]
                )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_protobuf_references(self, cloud_storage_type, query_engine, catalog_type):
        proto_date = """
syntax = "proto3";

package redpanda.datalake;

message Date {
  int32 date = 1;
}
"""

        proto_addr = """
syntax = "proto3";

message Address {
  string street = 1;
  string city = 2;
  string state = 3;
  string zip = 4;
}
"""
        # NOTE: Person references Address.
        proto_person = """
syntax = "proto3";
import "address.proto";
import "date.proto";

message Person {
  string name = 1;
  Address address = 2;
  redpanda.datalake.Date dob = 3;
}"""

        date_file_descriptor = """
name: "date.proto"
package: "redpanda.datalake"
message_type {
  name: "Date"
  field { name: "date" number: 1 label: LABEL_OPTIONAL type: TYPE_INT32 }
}
    """

        protobuf_file_descriptor = """
name: "example.proto"
message_type {
  name: "Address"
  field { name: "street" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "city" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "state" number: 3 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "zip" number: 4 label: LABEL_OPTIONAL type: TYPE_STRING }
}
message_type {
  name: "Person"
  field { name: "name" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "address" number: 2 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".Address" }
  field { name: "dob" number: 3 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".redpanda.datalake.Date" }
}
    """
        # Using the protobuf text format, parse the raw descriptor and create a
        # dynamic message from it.
        pool = protobuf.descriptor_pool.DescriptorPool()
        date_desc_pb = protobuf.descriptor_pb2.FileDescriptorProto()
        pb_text_format.Merge(date_file_descriptor, date_desc_pb)
        pool.Add(date_desc_pb)

        file_desc_pb = protobuf.descriptor_pb2.FileDescriptorProto()
        pb_text_format.Merge(protobuf_file_descriptor, file_desc_pb)
        pool.Add(file_desc_pb)

        person_desc = pool.FindMessageTypeByName("Person")
        Person = message_factory.GetMessageClass(person_desc)
        count = 100

        def produce_protos():
            producer = Producer({"bootstrap.servers": self.redpanda.brokers()})
            for i in range(count):
                record = json.dumps(
                    {
                        "name": f"Bob{i} Protopants",
                        "address": {
                            "street": f"{i} Main St.",
                            "city": "Protoville" if i % 2 == 0 else "Buftown",
                            "state": "District 13" if i % 3 == 0 else "Hooli",
                            "zip": "8675309" if i % 4 == 0 else "12345",
                        },
                        "dob": {"date": i},
                    }
                )
                person_proto = Person()
                pb_json_format.Parse(record, person_proto)
                producer.produce(
                    topic=self.topic_name, value=person_proto.SerializeToString()
                )
            producer.flush()

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            rpk = RpkTool(self.redpanda)

            subj_person = "subject_for_person"
            subj_addr = "subject_for_addr"
            subj_date = "subject_for_date"
            rpk.create_schema_from_str(subj_addr, proto_addr, "proto")  # ID 1
            rpk.create_schema_from_str(subj_date, proto_date, "proto")  # ID 2
            rpk.create_schema_from_str(
                subj_person,
                proto_person,
                "proto",
                references=f"address.proto:{subj_addr}:1,date.proto:{subj_date}:1",
            )  # ID 3
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                iceberg_mode=f"value_schema_latest:subject={subj_person},protobuf_name=Person",
            )
            produce_protos()
            dl.wait_for_translation(self.topic_name, msg_count=count)

            table_name = f"redpanda.{self.topic_name}"
            spark = dl.spark()
            spark_expected_out = [
                SPARK_RP_FIELD_TYPE,
                ('name', 'string', None),
                ('address', 'struct<street:string,city:string,state:string,zip:string>', None),
                ('dob', 'date', None),
                ('', '', ''),
                ('# Partitioning', '', ''),
                ('Part 0', 'hours(redpanda.timestamp)', '')
            ]  # yapf: disable
            spark_describe_out = spark.run_query_fetch_all(f"describe {table_name}")
            assert spark_describe_out == spark_expected_out, str(spark_describe_out)

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_upload_after_external_update(self, cloud_storage_type, catalog_type):
        table_name = f"redpanda.{self.topic_name}"
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            count = 100
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, count)
            spark = dl.spark()
            spark.make_client().cursor().execute(f"delete from {table_name}")
            count_after_del = spark.count_table("redpanda", self.topic_name)
            assert count_after_del == 0, f"{count_after_del} rows, expected 0"

            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation_until_offset(self.topic_name, 2 * count - 1)
            count_after_produce = spark.count_table("redpanda", self.topic_name)
            assert count_after_produce == count, (
                f"{count_after_produce} rows, expected {count}"
            )

    @cluster(num_nodes=4)
    @ignore(catalog_type=CatalogType.NESSIE, cloud_storage_type=CloudStorageType.S3)
    @ignore(catalog_type=CatalogType.NESSIE, cloud_storage_type=CloudStorageType.ABS)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_remove_expired_snapshots(self, cloud_storage_type, catalog_type):
        """
        Nessie doesn't support tags, so it is ignored for this test.
        """
        table_name = f"redpanda.{self.topic_name}"
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            count = 0
            records_per_round = 100
            num_rounds = 5
            for _ in range(num_rounds):
                count += records_per_round
                dl.produce_to_topic(self.topic_name, 1024, records_per_round)

                # Waiting for rows to be visible here ensures that at least one
                # new snapshot is written in each round.
                dl.wait_for_translation(self.topic_name, count)

            spark = dl.spark()

            def num_snapshots() -> int:
                # Example: [(2445569139027301708, 1739213500520), (2859411459768103060, 1739213495458), (1069851874616045025, 1739213485410), (5648673429948705023, 1739213475351), (7558202443004267034, 1739213465282)]
                snapshots_out = spark.run_query_fetch_all(
                    f"call system.ancestors_of('{table_name}')"
                )
                return len(snapshots_out)

            num_snaps = num_snapshots()
            assert num_snaps >= num_rounds, (
                f"Expected >={num_rounds} snapshots, got {num_snaps}"
            )

            # Encourage aggressive snapshot cleanup for the table. This
            # shouldn't affect Redpanda's snapshots, since Redpanda will tag
            # its metadata with separate retention policy.
            spark.make_client().cursor().execute(
                f"alter table {table_name} set tblproperties("
                "'history.expire.max-snapshot-age-ms'='1000', "
                "'history.expire.max-ref-age-ms'='1000')"
            )

            # Expect one snapshot retained during snapshot removal + 1 that is
            # added to the table.
            dl.produce_to_topic(self.topic_name, 1, 1)
            dl.wait_for_translation(self.topic_name, count + 1)
            wait_until(lambda: num_snapshots() == 2, timeout_sec=30, backoff_sec=1)

            # Externally create another snapshot.
            spark.make_client().cursor().execute(
                f"insert into {table_name} (select * from {table_name} limit 1)"
            )
            num_snaps = num_snapshots()
            assert num_snaps == 3, f"Expected 2 snapshots after writing: {num_snaps}"

            # Redpanda won't attempt removal until the next time we add.
            time.sleep(10)
            num_snaps = num_snapshots()
            assert num_snaps == 3, (
                f"Expected Redpanda to retain 3 snapshots: {num_snaps}"
            )

            # Spark should keep the latest snapshot (the one it created) and
            # the one created by Redpanda.
            spark.make_client().cursor().execute(
                f"call system.expire_snapshots('{table_name}')"
            )
            num_snaps = num_snapshots()
            assert num_snaps == 2, f"Expected Spark to retain 2 snapshots: {num_snaps}"

            # Wait for the snapshots to expire and produce more to Redpanda.
            # This will leave 3 snapshots:
            # - the one previously created by Redpanda
            # - the one created by Spark (the current main)
            # - the new one created by Redpanda after running snapshot removal
            time.sleep(2)
            dl.produce_to_topic(self.topic_name, 1, 1)
            dl.wait_for_translation(self.topic_name, count + 3)
            num_snaps = num_snapshots()
            assert num_snaps == 3, f"Expected 3 snapshots: {num_snaps}"

            # Doing this once more, we should end up with:
            # - the one previously created by Redpanda
            # - the new one created by Redpanda after running snapshot removal
            # I.e., Redpanda should have removed the snapshot created by Spark.
            time.sleep(2)
            dl.produce_to_topic(self.topic_name, 1, 1)
            dl.wait_for_translation(self.topic_name, count + 4)
            wait_until(lambda: num_snapshots() == 2, timeout_sec=30, backoff_sec=1)

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_topic_lifecycle(self, cloud_storage_type, catalog_type):
        count = 100
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            rpk = RpkTool(self.redpanda)

            # produce some data then delete the topic
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

            rpk.alter_topic_config(self.topic_name, "redpanda.iceberg.delete", "false")
            rpk.delete_topic(self.topic_name)

            # table is not deleted, it will contain messages
            dl.wait_for_translation(self.topic_name, msg_count=count)

            # recreate topic, it will contain messages from both topic instances
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=15)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=2 * count)

            # now table should be deleted
            rpk.delete_topic(self.topic_name)

            catalog_client = dl.catalog_client()

            def table_deleted():
                return not dl.table_exists(self.topic_name, client=catalog_client)

            wait_until(
                table_deleted,
                timeout_sec=30,
                backoff_sec=5,
                err_msg="table was not deleted",
            )

            # recreate an empty topic a few times
            for _ in range(3):
                dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
                rpk.delete_topic(self.topic_name)

            # check that the table is recreated after we start producing again
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=5)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_iceberg_files_location(self, cloud_storage_type, catalog_type):
        """
        Test that redpanda writes data files to the correct location
        as directed by the catalog.
        """
        count = 100
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=2)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

            table = dl.catalog_client().load_table(f"redpanda.{self.topic_name}")

            spark = dl.spark()
            table_name = f"redpanda.{self.topic_name}"

            def assert_location_prefix(rows, prefix: str):
                assert len(rows) > 0, (
                    "Expected at least one row to be able to validate the location prefix invariant"
                )

                for row in rows:
                    assert row[0].startswith(prefix), (
                        f"Expected {row[0]} to start with {prefix}"
                    )

            files = spark.run_query_fetch_all(
                f"select file_path from {table_name}.files"
            )
            assert_location_prefix(files, table.location())

            manifests = spark.run_query_fetch_all(
                f"select path from {table_name}.manifests"
            )
            assert_location_prefix(manifests, table.location())

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
        custom_partition_spec=[None, "(timestamp_us)", "(number)"],
    )
    def test_iceberg_partition_key_file_location(
        self, cloud_storage_type, catalog_type, custom_partition_spec: str | None
    ):
        """
        Test that the data file location includes the partition key
        """
        count = 100
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            config = {}
            if custom_partition_spec:
                config["redpanda.iceberg.partition.spec"] = custom_partition_spec

            dl.create_iceberg_enabled_topic(
                self.topic_name,
                partitions=2,
                iceberg_mode="value_schema_id_prefix",
                config=config,
            )

            schema = avro.loads(avro_schema_str)
            producer = AvroProducer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "schema.registry.url": self.redpanda.schema_reg().split(",")[0],
                },
                default_value_schema=schema,
            )
            current_date = datetime.datetime.now()
            for _ in range(count):
                t = time.time()
                record = {"number": int(t), "timestamp_us": int(t * 1000000)}
                producer.produce(topic=self.topic_name, value=record)

            producer.flush()
            dl.wait_for_translation(self.topic_name, msg_count=count)

            spark = dl.spark()
            table_name = f"redpanda.{self.topic_name}"
            uri_pattern = re.compile(r"(?P<scheme>.*?)://(?P<bucket>.*?)/(?P<key>.*)")

            def validate_data_file_path(file_url):
                m = uri_pattern.match(file_url)
                assert m, f"Expected file url to match URI pattern: {file_url}"
                assert m["bucket"].startswith(self.si_settings.cloud_storage_bucket), (
                    f"Expected bucket {m['bucket']} to be {self.si_settings.cloud_storage_bucket}"
                )

                path_parts = m["key"].split("/")
                partition_key = path_parts[4]

                if custom_partition_spec is None:
                    assert (
                        f"redpanda.timestamp_hour={current_date.year}" in partition_key
                    ), (
                        f"Expected default partition key in data file location {partition_key}"
                    )
                elif custom_partition_spec == "(timestamp_us)":
                    assert f"timestamp_us={current_date.year}" in partition_key, (
                        f"Expected timestamp_us partition key in data file location {partition_key}"
                    )
                elif custom_partition_spec == "(number)":
                    assert "number=" in partition_key, (
                        f"Expected number partition key in data file location {partition_key}"
                    )

            files = spark.run_query_fetch_all(
                f"select file_path from {table_name}.files"
            )
            assert len(files) > 0, "Expected at least one file"
            for f_tuple in files:
                f_name = f_tuple[0]
                validate_data_file_path(f_name)


class DatalakeMultiBrokerE2ETest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=3,
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

    @cluster(num_nodes=5)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_latest_protobuf_schema(
        self, cloud_storage_type, query_engine, catalog_type
    ):
        """
        This test uses multiple brokers to ensure that the schema registry
        and redpanda instances can handle schema updates/refresh correctly in a
        distributed environment.
        """
        count = 100
        table_name = f"redpanda.{self.topic_name}"

        protobuf_schema_v1 = """
        syntax = "proto3";

        message Person {
          string name = 1;
          int32 id = 2;
          string email = 3;
        }
        """
        protobuf_schema_v2 = """
        syntax = "proto3";

        message Address {
          string street = 1;
          string city = 2;
          string state = 3;
          string zip = 4;
        }

        message Person {
          string name = 1;
          int32 id = 2;
          string email = 3;
          Address address = 4;  // Nested message
        }
        """
        # The text format of the above protobuf as to not have to bother setting up protoc for this test.
        protobuf_file_descriptor = """
        name: "example.proto"
        message_type {
          name: "Address"
          field { name: "street" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
          field { name: "city" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
          field { name: "state" number: 3 label: LABEL_OPTIONAL type: TYPE_STRING }
          field { name: "zip" number: 4 label: LABEL_OPTIONAL type: TYPE_STRING }
        }
        message_type {
          name: "Person"
          field { name: "name" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
          field { name: "id" number: 2 label: LABEL_OPTIONAL type: TYPE_INT32 }
          field { name: "email" number: 3 label: LABEL_OPTIONAL type: TYPE_STRING }
          field { name: "address" number: 4 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".Address" }
        }
        """

        # Using the protobuf text format, parse the raw descriptor and create a dynamic message from it.
        file_desc_pb = protobuf.descriptor_pb2.FileDescriptorProto()
        pb_text_format.Merge(protobuf_file_descriptor, file_desc_pb)
        pool = protobuf.descriptor_pool.DescriptorPool()
        pool.Add(file_desc_pb)
        factory = protobuf.message_factory.MessageFactory(pool)
        person_desc = pool.FindMessageTypeByName("Person")
        Person = factory.GetPrototype(person_desc)

        def produce_protos():
            producer = Producer({"bootstrap.servers": self.redpanda.brokers()})
            for i in range(count):
                record = json.dumps(
                    {
                        "name": f"Bob{i} Protopants",
                        "id": 1 + i,
                        "email": f"foobar{i}@gmail.com",
                        "address": {
                            "street": f"{i} Main St.",
                            "city": "Protoville" if i % 2 == 0 else "Buftown",
                            "state": "District 13" if i % 3 == 0 else "Hooli",
                            "zip": "8675309" if i % 4 == 0 else "12345",
                        },
                    }
                )
                person_proto = Person()
                pb_json_format.Parse(record, person_proto)
                producer.produce(
                    topic=self.topic_name, value=person_proto.SerializeToString()
                )
            producer.flush()

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            rpk = RpkTool(self.redpanda)
            rpk.create_schema_from_str(
                subject=f"{self.topic_name}-value",
                schema=protobuf_schema_v1,
                schema_suffix="proto",
            )
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_latest:protobuf_name=Person"
            )
            produce_protos()
            dl.wait_for_translation(self.topic_name, msg_count=count)

            spark = dl.spark()
            spark_expected_out = [
                SPARK_RP_FIELD_TYPE,
                ('name', 'string', None),
                ('id', 'int', None),
                ('email', 'string', None),
                ('', '', ''),
                ('# Partitioning', '', ''),
                ('Part 0', 'hours(redpanda.timestamp)', '')
            ]  # yapf: disable
            spark_describe_out = spark.run_query_fetch_all(f"describe {table_name}")
            assert spark_describe_out == spark_expected_out, str(spark_describe_out)

            # Be absolutely sure that the latest schema is being used by the
            # translator by waiting for the cache to expire the latest schema.
            rpk.create_schema_from_str(
                subject=f"{self.topic_name}-value",
                schema=protobuf_schema_v2,
                schema_suffix="proto",
            )
            self.redpanda.set_cluster_config(
                {"iceberg_latest_schema_cache_ttl_ms": "500"}
            )
            time.sleep(1)
            produce_protos()
            dl.wait_for_translation_until_offset(self.topic_name, 2 * count - 1)

            spark = dl.spark()
            spark_expected_out = [
                SPARK_RP_FIELD_TYPE,
                ('name', 'string', None),
                ('id', 'int', None),
                ('email', 'string', None),
                ('address', 'struct<street:string,city:string,state:string,zip:string>', None),
                ('', '', ''),
                ('# Partitioning', '', ''),
                ('Part 0', 'hours(redpanda.timestamp)', '')
            ]  # yapf: disable
            spark_describe_out = spark.run_query_fetch_all(f"describe {table_name}")
            assert spark_describe_out == spark_expected_out, str(spark_describe_out)


class DatalakeMetricsTest(RedpandaTest):
    commit_lag = "redpanda_iceberg_pending_commit_lag"
    translation_lag = "redpanda_iceberg_pending_translation_lag"

    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeMetricsTest, self).__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": "1000",
                "enable_leader_balancer": False,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        pass

    def wait_for_lag(
        self,
        metric_check: MetricCheck,
        metric_name: str,
        count: int,
        timeout_sec: int = 30,
    ):
        wait_until(
            lambda: metric_check.evaluate([(metric_name, lambda _, val: val == count)]),
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=f"Timed out waiting for {metric_name} to reach: {count}",
        )

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_lag_metrics(self, cloud_storage_type):
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[],
            catalog_type=supported_catalog_types()[0],
        ) as dl:
            # Stop the catalog to halt the translation flow
            dl.catalog_service.stop()

            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1, replicas=3)
            topic_leader = self.redpanda.partitions(self.topic_name)[0].leader
            count = randint(12, 21)
            dl.produce_to_topic(self.topic_name, 1, msg_count=count)

            m = MetricCheck(
                self.redpanda.logger,
                self.redpanda,
                topic_leader,
                [DatalakeMetricsTest.commit_lag, DatalakeMetricsTest.translation_lag],
                labels={
                    "redpanda_namespace": "kafka",
                    "redpanda_topic": self.topic_name,
                },
                reduce=sum,
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
            )

            # Wait for lag build up
            self.wait_for_lag(m, DatalakeMetricsTest.translation_lag, count)
            self.wait_for_lag(m, DatalakeMetricsTest.commit_lag, count)

            # Resume iceberg translation
            dl.catalog_service.start()

            # translation lag goes straight to zero once we reconcile coordinator state
            self.wait_for_lag(m, DatalakeMetricsTest.translation_lag, 0)
            # the committed offset is fed by a commit task that is concurrent to
            # the translation loop, so we may have to wait one `wait_for_data`
            # timeout period (30s) before the lag goes to zero.
            self.wait_for_lag(m, DatalakeMetricsTest.commit_lag, 0, timeout_sec=45)

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_rest_catalog_metrics(self, cloud_storage_type):
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            commit_table_metric = (
                "redpanda_iceberg_rest_client_num_commit_table_update_requests_total"
            )
            create_table_metric = (
                "redpanda_iceberg_rest_client_num_create_table_requests_total"
            )
            load_table_metric = (
                "redpanda_iceberg_rest_client_num_load_table_requests_total"
            )
            drop_table_metric = (
                "redpanda_iceberg_rest_client_num_drop_table_requests_total"
            )
            timeouts_metric = "redpanda_iceberg_rest_client_num_request_timeouts_total"

            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1, replicas=3)
            dl.produce_to_topic(self.topic_name, 1024, 10)

            # Wait until we have committed to the table -- this implies several
            # other metrics should be ticked as well.
            wait_until(
                lambda: self.redpanda.metric_sum(
                    commit_table_metric, MetricsEndpoint.PUBLIC_METRICS
                )
                > 0,
                timeout_sec=30,
                backoff_sec=1,
            )

            create_metric_val = self.redpanda.metric_sum(
                create_table_metric, MetricsEndpoint.PUBLIC_METRICS
            )
            assert create_metric_val > 0, (
                f"Expected >0 for {create_table_metric}: {create_metric_val}"
            )

            load_metric_val = self.redpanda.metric_sum(
                load_table_metric, MetricsEndpoint.PUBLIC_METRICS
            )
            assert load_metric_val > 0, (
                f"Expected >0 for {load_table_metric}: {load_metric_val}"
            )

            # The metric for dropped tables should not show up until we drop
            # the topic.
            drop_metric_val = self.redpanda.metric_sum(
                drop_table_metric, MetricsEndpoint.PUBLIC_METRICS
            )
            assert drop_metric_val == 0, (
                f"Expected ==0 for {drop_table_metric}: {drop_metric_val}"
            )

            rpk = RpkTool(self.redpanda)
            rpk.delete_topic(self.topic_name)

            wait_until(
                lambda: self.redpanda.metric_sum(
                    drop_table_metric, MetricsEndpoint.PUBLIC_METRICS
                )
                > 0,
                timeout_sec=30,
                backoff_sec=1,
            )

            # Stop the catalog to halt the translation flow
            dl.catalog_service.stop()

            # Our topic is deleted, so we shouldn't see any errors until we
            # start producing again.
            timeouts_metric_val = self.redpanda.metric_sum(
                timeouts_metric, MetricsEndpoint.PUBLIC_METRICS
            )
            assert timeouts_metric_val == 0, (
                f"Expected ==0 for {timeouts_metric}: {timeouts_metric_val}"
            )

            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1, replicas=3)
            dl.produce_to_topic(self.topic_name, 1024, 10)
            wait_until(
                lambda: self.redpanda.metric_sum(
                    timeouts_metric, MetricsEndpoint.PUBLIC_METRICS
                )
                > 0,
                timeout_sec=30,
                backoff_sec=1,
            )


class DatalakeDelayedEnablementTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self.target_reply_bytes = 5 * 1024 * 1024  # 5 MiB
        super(DatalakeDelayedEnablementTest, self).__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_catalog_commit_interval_ms": 5000,
                "log_compaction_interval_ms": 2000,
                "storage_target_replay_bytes": self.target_reply_bytes,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            environment={"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"},
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx

    def setUp(self):
        pass

    def is_topic_fully_caught_up(self, topic_name: str):
        admin = Admin(self.redpanda)
        partitions = admin.get_partitions(topic=topic_name)

        for p in partitions:
            p_id = p["partition_id"]
            status = admin.get_partition_state(
                "kafka", topic=topic_name, partition=p["partition_id"]
            )
            for replica in status["replicas"]:
                c_index = replica["raft_state"]["commit_index"]
                for stm in replica["raft_state"]["stms"]:
                    self.logger.debug(
                        f"{topic_name}/{p_id} state machine: {stm['name']} on: {replica['raft_state']['node_id']} last_applied_offset: {stm['last_applied_offset']}"
                    )
                    if stm["last_applied_offset"] < c_index:
                        return False

        return True

    def estimate_total_disk_bytes_read(self):
        max_samples = 30
        current_value = 0
        stable_values = 0
        expected_stable_values = 5
        for s in range(max_samples):
            total = self.do_estimate_total_disk_bytes_read()
            if total is not None:
                self.logger.debug(
                    f"Estimated total disk bytes read: {total} bytes sample {s}"
                )
                if total == current_value:
                    stable_values += 1
                    if stable_values >= expected_stable_values:
                        self.logger.debug(
                            f"Stable value reached: {current_value} bytes after {s} samples"
                        )
                        break
                else:
                    stable_values = 0
                    current_value = total

            time.sleep(0.5)

        return current_value

    def do_estimate_total_disk_bytes_read(self):
        try:
            samples = self.redpanda.metrics_sample(
                "vectorized_io_queue_total_read_bytes_total",
                nodes=self.redpanda.started_nodes(),
            )
        except Exception as e:
            self.logger.warn(
                f"Cannot check metrics, did a test finish with all nodes down? ({e})"
            )
            return None
        total = 0
        if samples is not None and samples.samples:
            for s in samples.samples:
                self.logger.debug(
                    f"{s.sample} on node {s.node.account.hostname} with labels: {s.labels} has value {s.value}"
                )
                total += s.value
        else:
            return None

        return total

    @cluster(num_nodes=6)
    # this test doesn't have to run with different catalog types
    @skip_debug_mode
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_enabling_iceberg_in_existing_cluster(
        self, cloud_storage_type, catalog_type
    ):
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            topic_name = "delayed-iceberg-topic"
            RpkTool(self.redpanda).create_topic(
                topic_name,
                partitions=3,
                replicas=3,
                config={
                    "segment.bytes": 1024 * 1024,
                    "redpanda.remote.read": False,
                    "redpanda.remote.write": False,
                },
            )

            # produce ~120 MiB to the topic
            dl.produce_to_topic(topic_name, 1024, 120 * 1024)
            # wait for a while for the local snapshot to be taken
            time.sleep(5)

            def wait_for_topic(topic_name: str):
                wait_until(
                    lambda: self.is_topic_fully_caught_up(topic_name),
                    timeout_sec=30,
                    backoff_sec=1,
                    err_msg=f"Error waiting for topic {topic_name} \
                        state machines to catch up",
                )

            wait_for_topic(topic_name)
            dl.redpanda.restart_nodes(dl.redpanda.nodes)

            wait_for_topic(topic_name)
            bytes_read_before = self.estimate_total_disk_bytes_read()
            assert bytes_read_before, (
                "Bytes read before enabling Iceberg should not be zero/none"
            )

            # enable iceberg, this will restart the cluster
            dl.redpanda.set_cluster_config(
                {"iceberg_enabled": True}, expect_restart=True
            )

            wait_for_topic(topic_name)
            bytes_read_after = self.estimate_total_disk_bytes_read()
            assert bytes_read_after, (
                "Bytes read after enabling Iceberg should not be zero/none"
            )

            self.logger.info(
                f"Bytes read before: {bytes_read_before}, bytes read after: {bytes_read_after}"
            )
            # we introduce a small tolerance here, since the read bytes may
            # increase slightly due to term changes and leader elections.
            max_read_bytes = bytes_read_before + self.target_reply_bytes
            assert bytes_read_after <= max_read_bytes, (
                f"Enabling Iceberg in the cluster should not cause a major read increase,"
                f" expected ({bytes_read_after=}) <= ({max_read_bytes=})"
            )


class DatalakeCustomNamespaceTest(RedpandaTest):
    test_namespace = ("rp", "eng", "core")

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
                "iceberg_default_catalog_namespace": self.test_namespace,
            },
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        pass

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_custom_namespace(self, cloud_storage_type, catalog_type):
        count = 10
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(
                self.topic_name,
                msg_count=count,
                namespace=self.test_namespace,
            )


class DatalakeCoordinatorResetTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        """Redpanda will be started by DatalakeServices."""
        pass

    def _get_topic_state(self):
        dl_pb = admin_v2.datalake_pb
        admin = admin_v2.Admin(self.redpanda)
        resp = admin.datalake().get_coordinator_state(
            dl_pb.GetCoordinatorStateRequest()
        )
        return resp.state.topic_states[self.topic_name]

    def _reset_coordinator_state(
        self, reset_all_partitions: bool, partition_overrides=None
    ):
        dl_pb = admin_v2.datalake_pb
        admin = admin_v2.Admin(self.redpanda)
        rev = self._get_topic_state().revision
        admin.datalake().coordinator_reset_topic_state(
            dl_pb.CoordinatorResetTopicStateRequest(
                topic_name=self.topic_name,
                revision=rev,
                reset_all_partitions=reset_all_partitions,
                partition_overrides=partition_overrides,
            )
        )

    def _count_pending_entries(self):
        ts = self._get_topic_state()
        return sum(len(ps.pending_entries) for ps in ts.partition_states.values())

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=[CatalogType.REST_JDBC],
    )
    def test_coordinator_reset(self, cloud_storage_type, catalog_type):
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                partitions=3,
                config={
                    TopicSpec.PROPERTY_ICEBERG_PARTITION_SPEC: "()",
                },
            )
            dl.produce_to_topic(self.topic_name, msg_size=1024, msg_count=10)
            dl.wait_for_translation(self.topic_name, msg_count=10)

            # Increase the commit interval to accumulate pending commits.
            self.redpanda.set_cluster_config(
                {"iceberg_catalog_commit_interval_ms": 100000}
            )
            # Sleep twice the original commit interval to ensure we will be
            # waiting on the new interval.
            time.sleep(10)

            dl.produce_to_topic(self.topic_name, msg_size=1024, msg_count=10)

            wait_until(
                lambda: self._count_pending_entries() > 0,
                timeout_sec=30,
                backoff_sec=1,
                err_msg="Expected pending entries to be present after producing records",
            )

            self._reset_coordinator_state(reset_all_partitions=False)
            assert self._count_pending_entries() > 0, (
                "Expected pending entries to still be present after no-op reset"
            )

            self._reset_coordinator_state(reset_all_partitions=True)
            assert self._count_pending_entries() == 0, (
                "Expected pending entries to be cleared after coordinator reset"
            )

            # After a plain reset, no partition should have last_committed.
            for pid, ps in self._get_topic_state().partition_states.items():
                assert not ps.HasField("last_committed"), (
                    f"Partition {pid} has unexpected last_committed"
                )

            dl_pb = admin_v2.datalake_pb

            # Reset with per-partition last_committed overrides.
            expected = {0: 5, 2: 7}
            self._reset_coordinator_state(
                reset_all_partitions=False,
                partition_overrides={
                    pid: dl_pb.PartitionStateOverride(last_committed=off)
                    for pid, off in expected.items()
                },
            )

            ts = self._get_topic_state()
            for pid, off in expected.items():
                ps = ts.partition_states[pid]
                assert ps.last_committed == off, (
                    f"Partition {pid}: expected {off}, got {ps.last_committed}"
                )
            assert not ts.partition_states[1].HasField("last_committed")
