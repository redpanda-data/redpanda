# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import random

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SISettings, SchemaRegistryConfig
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.schemas.data_types import (
    ALL_PRIMITIVE_DATA_TYPES,
    ProducerType,
)
from rptest.tests.datalake.schemas.schema_generator import SchemaGenerator
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest

QUERY_ENGINES = [
    QueryEngineType.SPARK,
    QueryEngineType.TRINO,
]


def get_random_primitive_field(schema_json):
    fully_qualified_prim_field_names = []
    avro_data_types = [t.to_avro() for t in ALL_PRIMITIVE_DATA_TYPES]

    def traverse_fields(fields, qualified_name=""):
        for field in fields:
            if isinstance(field["type"], str) and field["type"] in avro_data_types:
                fully_qualified_prim_field_names.append(
                    (f"{qualified_name}{field['name']}", field["type"])
                )
            elif isinstance(field["type"], dict):
                if "fields" in field["type"]:
                    traverse_fields(
                        field["type"]["fields"], f"{qualified_name}{field['name']}."
                    )

    traverse_fields(schema_json["fields"])
    return random.choice(fully_qualified_prim_field_names)


class SchemaScaleTester:
    def __init__(self, redpanda, topic_name):
        self.redpanda = redpanda
        self.topic_name = topic_name
        self.table_name = f"redpanda.{self.topic_name}"
        self.schema = None
        self.schema_fields = None
        self.schema_template = None
        self.partition_spec = None
        self.total = 0

    def _make_schema(self, use_partition_spec: bool = False):
        sg = SchemaGenerator(ProducerType.AVRO, max_depth=15, max_fields=1000)
        schema, template = sg.generate_schema_fields()
        self.schema = schema
        self.schema_fields = sg.fields
        self.schema_template = template

        self.redpanda.logger.debug(
            f"Using randomly generated schema: {json.dumps(self.schema.to_json(), indent=2)}"
        )

        if use_partition_spec:
            random_primitive_field = get_random_primitive_field(self.schema.to_json())
            self.partition_spec = f"({random_primitive_field[0]})"
            self.redpanda.logger.debug(
                f"Using randomly selected field for partition spec: {random_primitive_field}"
            )

    def _make_avro_producer(self) -> AvroProducer:
        self.avro_producer = AvroProducer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "schema.registry.url": self.redpanda.schema_reg().split(",")[0],
            },
            default_value_schema=avro.loads(json.dumps(self.schema.to_json())),
        )

    def _make_random_record(self, validate: bool = False):
        assert self.schema and self.schema_fields and self.schema_template, (
            "Need to set schema before _make_random_record() is called"
        )
        return SchemaGenerator.make_random_record(
            self.schema, self.schema_fields, self.schema_template, validate=validate
        )

    def produce(
        self,
        dl: DatalakeServices,
        topic_name: str,
        count: int,
        should_translate: bool = True,
        mode: ProducerType = ProducerType.AVRO,
    ):
        for i in range(count):
            record = self._make_random_record(validate=True)
            if i == 0:
                self.redpanda.logger.debug(
                    f"Producing randomly generated record: {record}"
                )
            self.avro_producer.produce(topic=topic_name, value=record)

        self.avro_producer.flush()
        if should_translate:
            dl.wait_for_translation(topic_name, msg_count=self.total + count)
            self.total += count

    def log_table_schema(
        self,
        dl: DatalakeServices,
        query_engine: QueryEngineType,
    ):
        qe = dl.spark() if query_engine == QueryEngineType.SPARK else dl.trino()
        table = qe.run_query_fetch_all(f"describe {self.table_name}")
        self.redpanda.logger.debug(f"Describe table result: {table}")

    def check_no_dlq_table(self, dl: DatalakeServices):
        # For only valid records produced, no DLQ table should be created.
        dlq_table_name = f"{self.topic_name}~dlq"
        assert not dl.table_exists(dlq_table_name), "Expected no DLQ table in catalog"

    def do_test_schema_scale(
        self,
        dl: DatalakeServices,
        query_engine: QueryEngineType,
        use_partition_spec: bool,
    ):
        config = {
            TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION: "dlq_table",
            TopicSpec.PROPERTY_CLEANUP_POLICY: TopicSpec.CLEANUP_COMPACT_DELETE,
        }
        if self.partition_spec is not None:
            config["redpanda.iceberg.partition.spec"] = self.partition_spec

        dl.create_iceberg_enabled_topic(
            self.topic_name,
            iceberg_mode="value_schema_id_prefix",
            config=config,
            partitions=10,
        )

        # Generate a random schema
        self._make_schema(use_partition_spec)

        # Make an Avro producer for the generated schema
        self._make_avro_producer()

        rpk = RpkTool(self.redpanda)
        schema_str = json.dumps(self.schema.to_json())
        rpk.create_schema_from_str(f"{self.topic_name}_schema", schema_str)

        for _ in range(3):
            self.produce(dl, self.topic_name, 500)

        self.log_table_schema(dl, query_engine)
        self.check_no_dlq_table(dl)


class SchemaScaleTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(SchemaScaleTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
                "log_compaction_interval_ms": 5000,
                "min_cleanable_dirty_ratio": 0.0,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        catalog_type=supported_catalog_types(),
        use_partition_spec=[False],
    )
    def schema_scale_test(
        self, cloud_storage_type, query_engine, catalog_type, use_partition_spec
    ):
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            # Test 5 randomly generated schemas.
            for i in range(5):
                topic_name = f"test_{i}"
                tester = SchemaScaleTester(self.redpanda, topic_name)
                tester.do_test_schema_scale(dl, query_engine, use_partition_spec)
