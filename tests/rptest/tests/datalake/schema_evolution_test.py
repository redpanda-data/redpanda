# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import tempfile
from collections.abc import Callable
from contextlib import contextmanager
from enum import Enum
from itertools import product
from time import time
from typing import NamedTuple

import pyhive
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SISettings, SchemaRegistryConfig
from rptest.tests.datalake.catalog_service_factory import (
    filesystem_catalog_type,
    supported_catalog_types,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


# for keeping track of the expected total number of rows across rounds
# of translation (i.e. calls to _produce)
class TranslationContext:
    total: int = 0
    dlq: int = 0


class ProducerType(str, Enum):
    AVRO = "avro"
    PROTO2 = "proto2"
    PROTO3 = "proto3"


class ProtobufVersion(int, Enum):
    PROTO2 = 2
    PROTO3 = 3


class ProtoProducer:
    PROTO_TEMPLATE = """
syntax = "proto{ver}";

message {name} {{
{fields}
}}
"""

    def __init__(
        self,
        redpanda,
        ver: ProtobufVersion,
        name: str,
        fields: list[dict],
        topic_name: str,
    ):
        self.redpanda = redpanda
        self.rpk = RpkTool(redpanda)
        self.message_name = name
        p_fields = self._translate_fields(fields)
        self.schema = self.PROTO_TEMPLATE.format(
            ver=ver.value,
            name=self.message_name,
            fields="\n".join(
                [
                    f"  {'optional ' if ver == ProtobufVersion.PROTO2 else ''}{p_fields[i]['type']} {p_fields[i]['name']} = {i + 1};"
                    for i in range(len(p_fields))
                ]
            ),
        )
        self.schema_id = self._create_schema(topic_name)

    def _translate_fields(self, fields: list[dict]) -> list[dict]:
        return [
            {
                "name": f["name"],
                "type": self._translate_type(f["type"]),
            }
            for f in fields
        ]

    def _translate_type(self, type: str) -> str:
        if type == "int":
            return "int32"
        elif type == "long":
            return "int64"
        else:
            return type

    def _create_schema(self, topic: str) -> int:
        with tempfile.NamedTemporaryFile(suffix=".proto") as tf:
            tf.write(bytes(self.schema, "UTF-8"))
            tf.seek(0)
            subject = f"{topic}-value"
            out = self.rpk.create_schema(subject, tf.name)
            assert out["subject"] == subject, (
                f"Expected {subject}, got {out['subject']}"
            )
            return out["id"]

    def produce(self, topic: str, value: dict):
        self.rpk.produce(
            topic,
            key="",
            msg=json.dumps(value),
            schema_id=self.schema_id,
            proto_msg=self.message_name,
        )

    def flush(self):
        pass


class GenericSchema:
    TableDescription = list[tuple[str, str]]

    def __init__(
        self,
        fields: list[dict],
        generate_record: Callable[[float], dict],
        spark_table: TableDescription = [],
        trino_table: TableDescription = [],
        name="Record",
    ):
        self._rep: dict = {
            "type": "record",
            "name": name,
            "fields": fields,
        }
        self._generate_record = generate_record
        self._spark_table = spark_table
        self._trino_table = trino_table

    def table(self, engine: QueryEngineType):
        return (
            self._spark_table if engine == QueryEngineType.SPARK else self._trino_table
        )

    @property
    def field_names(self) -> list[str]:
        return [field["name"] for field in self._rep["fields"]]

    @property
    def fields(self) -> list[dict]:
        return self._rep["fields"]

    def gen(self, x: float) -> dict:
        return self._generate_record(x)

    def _avro_producer(self, dl: DatalakeServices) -> AvroProducer:
        return AvroProducer(
            {
                "bootstrap.servers": dl.redpanda.brokers(),
                "schema.registry.url": dl.redpanda.schema_reg().split(",")[0],
            },
            default_value_schema=avro.loads(json.dumps(self._rep)),
        )

    def _proto_producer(
        self,
        dl: DatalakeServices,
        topic_name: str,
        ver=ProtobufVersion,
    ):
        return ProtoProducer(
            dl.redpanda, ver, self._rep["name"], self._rep["fields"], topic_name
        )

    def produce(
        self,
        dl: DatalakeServices,
        topic_name: str,
        count: int,
        context: TranslationContext,
        should_translate: bool = True,
        mode: ProducerType = ProducerType.AVRO,
    ):
        producer = (
            self._avro_producer(dl)
            if mode == ProducerType.AVRO
            else self._proto_producer(
                dl,
                topic_name,
                ProtobufVersion.PROTO2
                if mode == ProducerType.PROTO2
                else ProtobufVersion.PROTO3,
            )
        )

        assert producer is not None

        for i in range(count):
            record = self.gen(time())
            producer.produce(topic=topic_name, value=record)

        producer.flush()
        if should_translate:
            dl.wait_for_translation(topic_name, msg_count=context.total + count)
            context.total = context.total + count
        else:
            dl.wait_for_translation(
                topic_name,
                msg_count=context.dlq + count,
                table_override=f"{topic_name}~dlq",
            )
            context.dlq = context.dlq + count

    def check_table_schema(
        self,
        dl: DatalakeServices,
        table_name: str,
        query_engine: QueryEngineType,
    ):
        qe = dl.spark() if query_engine == QueryEngineType.SPARK else dl.trino()
        table = qe.run_query_fetch_all(f"describe {table_name}")

        if query_engine == QueryEngineType.SPARK:
            table = [(t[0], t[1]) for t in table[1:-3]]
        elif query_engine == QueryEngineType.TRINO:
            table = [(t[0], t[1]) for t in table[1:]]

        expect_table = self.table(query_engine)
        assert table == expect_table, (
            f"Expected table description {expect_table}, got {str(table)}"
        )


class EvolutionTestCase(NamedTuple):
    initial_schema: GenericSchema
    next_schema: GenericSchema
    partition_spec: str | None = None
    valid: bool = True


TEST_CASES = {
    "add_column": EvolutionTestCase(
        initial_schema=GenericSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
            },
            spark_table=[
                ("verifier_string", "string"),
            ],
            trino_table=[
                ("verifier_string", "varchar"),
            ],
        ),
        next_schema=GenericSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
                "ordinal": int(x),
            },
            spark_table=[
                ("verifier_string", "string"),
                ("ordinal", "int"),
            ],
            trino_table=[
                ("verifier_string", "varchar"),
                ("ordinal", "integer"),
            ],
        ),
        partition_spec="(verifier_string)",
    ),
    "drop_column": EvolutionTestCase(
        initial_schema=GenericSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
                "ordinal": int(x),
            },
            spark_table=[
                ("verifier_string", "string"),
                ("ordinal", "int"),
            ],
            trino_table=[
                ("verifier_string", "varchar"),
                ("ordinal", "integer"),
            ],
        ),
        next_schema=GenericSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
            },
            spark_table=[
                ("verifier_string", "string"),
            ],
            trino_table=[
                ("verifier_string", "varchar"),
            ],
        ),
        partition_spec="(verifier_string)",
    ),
    "promote_column": EvolutionTestCase(
        initial_schema=GenericSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "ordinal": int(x),
            },
            spark_table=[
                ("ordinal", "int"),
            ],
            trino_table=[
                ("ordinal", "integer"),
            ],
        ),
        next_schema=GenericSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "long",
                },
            ],
            generate_record=lambda x: {
                "ordinal": int(x),
            },
            spark_table=[
                ("ordinal", "bigint"),
            ],
            trino_table=[
                ("ordinal", "bigint"),
            ],
        ),
        partition_spec="(ordinal)",
    ),
    "reorder_columns": EvolutionTestCase(
        initial_schema=GenericSchema(
            fields=[
                {
                    "name": "first",
                    "type": "string",
                },
                {
                    "name": "second",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "first": "first",
                "second": "second",
            },
            spark_table=[
                ("first", "string"),
                ("second", "string"),
            ],
            trino_table=[
                ("first", "varchar"),
                ("second", "varchar"),
            ],
        ),
        next_schema=GenericSchema(
            fields=[
                {
                    "name": "second",
                    "type": "string",
                },
                {
                    "name": "first",
                    "type": "string",
                },
                {
                    "name": "third",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "second": "second",
                "first": "first",
                "third": "third",
            },
            spark_table=[
                ("second", "string"),
                ("first", "string"),
                ("third", "string"),
            ],
            trino_table=[
                ("second", "varchar"),
                ("first", "varchar"),
                ("third", "varchar"),
            ],
        ),
        partition_spec="(first)",
    ),
    "illegal_promotion_int_to_string": EvolutionTestCase(
        initial_schema=GenericSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "ordinal": int(x),
            },
            spark_table=[
                ("ordinal", "int"),
            ],
            trino_table=[
                ("ordinal", "integer"),
            ],
        ),
        next_schema=GenericSchema(
            fields=[
                {
                    "name": "ordinal",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "ordinal": str(x),
            },
        ),
        valid=False,
    ),
    "drop column that appears in partition spec": EvolutionTestCase(
        initial_schema=GenericSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
                {
                    "name": "ordinal",
                    "type": "int",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
                "ordinal": int(x),
            },
            spark_table=[
                ("verifier_string", "string"),
                ("ordinal", "int"),
            ],
            trino_table=[
                ("verifier_string", "varchar"),
                ("ordinal", "integer"),
            ],
        ),
        next_schema=GenericSchema(
            fields=[
                {
                    "name": "verifier_string",
                    "type": "string",
                },
            ],
            generate_record=lambda x: {
                "verifier_string": f"verify-{x}",
            },
        ),
        partition_spec="(ordinal)",
    ),
}

QUERY_ENGINES = [
    QueryEngineType.SPARK,
    QueryEngineType.TRINO,
]

PRODUCER_MODES = [
    ProducerType.AVRO,
    ProducerType.PROTO2,
    ProducerType.PROTO3,
]


class SchemaEvolutionE2ETests(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(SchemaEvolutionE2ETests, self).__init__(
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

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def select(
        self,
        dl: DatalakeServices,
        table: str,
        query_engine: QueryEngineType,
        cols=list[str],
        sort_by_offset: bool = True,
    ):
        qe = dl.spark() if query_engine == QueryEngineType.SPARK else dl.trino()
        query = f"select redpanda.offset, {', '.join(cols)} from {table}"
        self.redpanda.logger.debug(f"QUERY: '{query}'")
        out = qe.run_query_fetch_all(query)
        if sort_by_offset:
            out.sort(key=lambda r: r[0])
        return out

    @property
    def valid_cases(self) -> dict[str, EvolutionTestCase]:
        return {label: tc for label, tc in self.all_cases.items() if tc.valid}

    @property
    def invalid_cases(self) -> dict[str, EvolutionTestCase]:
        return {label: tc for label, tc in self.all_cases.items() if not tc.valid}

    @property
    def all_cases(self) -> dict[str, EvolutionTestCase]:
        return TEST_CASES

    def cases_by_modes(
        self, cases: dict[str, EvolutionTestCase]
    ) -> list[tuple[str, EvolutionTestCase, ProducerType]]:
        return [t + (p,) for t, p in product(cases.items(), PRODUCER_MODES)]

    def topic_and_table(self, base: str, mode: ProducerType) -> tuple[str, str]:
        topic = f"{base}_{mode}"
        table = f"redpanda.{topic}"
        return (
            topic,
            table,
        )

    @contextmanager
    def setup_services(
        self,
        query_engine: QueryEngineType,
        compat_level: str = "NONE",
        catalog_type: CatalogType = filesystem_catalog_type(),
        test_cases: dict[str, EvolutionTestCase] = {},
        with_partitioning: bool = False,
    ):
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[
                query_engine,
            ],
        ) as dl:
            for label, tc, produce_mode in self.cases_by_modes(test_cases):
                topic, _ = self.topic_and_table(label, produce_mode)
                config = {TopicSpec.PROPERTY_ICEBERG_INVALID_RECORD_ACTION: "dlq_table"}
                if with_partitioning and tc.partition_spec is not None:
                    config["redpanda.iceberg.partition.spec"] = tc.partition_spec
                dl.create_iceberg_enabled_topic(
                    topic,
                    iceberg_mode="value_schema_id_prefix",
                    config=config,
                )
                SchemaRegistryClient(
                    {"url": self.redpanda.schema_reg().split(",")[0]}
                ).set_compatibility(subject_name=f"{topic}-value", level=compat_level)
            yield dl
            # make sure nothing we did trashed our ability to read the whole table
            for label, tc, produce_mode in self.cases_by_modes(test_cases):
                _, table = self.topic_and_table(label, produce_mode)
                self.select(
                    dl,
                    table,
                    query_engine,
                    cols=["*"],
                )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        catalog_type=supported_catalog_types(),
    )
    def test_schema_evolution(self, cloud_storage_type, query_engine, catalog_type):
        """
        Test that rows written with schema A are still readable after evolving
        the table to schema B.
        """

        with self.setup_services(
            query_engine,
            catalog_type=catalog_type,
            test_cases=self.all_cases,
        ) as dl:
            for label, tc, produce_mode in self.cases_by_modes(self.all_cases):
                topic, table = self.topic_and_table(label, produce_mode)
                count = 10
                ctx = TranslationContext()
                tc.initial_schema.produce(dl, topic, count, ctx, mode=produce_mode)

                tc.initial_schema.check_table_schema(dl, table, query_engine)
                tc.next_schema.produce(
                    dl, topic, count, ctx, mode=produce_mode, should_translate=tc.valid
                )
                if tc.valid:
                    tc.next_schema.check_table_schema(dl, table, query_engine)

                    select_out = self.select(
                        dl,
                        table,
                        query_engine,
                        tc.next_schema.field_names,
                    )
                    assert len(select_out) == count * 2, (
                        f"Expected {count * 2} rows, got {select_out}"
                    )
                else:
                    tc.initial_schema.check_table_schema(dl, table, query_engine)

                    select_out = self.select(
                        dl, table, query_engine, tc.next_schema.field_names
                    )
                    assert len(select_out) == count, (
                        f"Expected {count} rows, got {select_out}"
                    )
                    assert ctx.dlq == count, (
                        f"Expected {count} records were dlq'ed, got {ctx.dlq}"
                    )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        produce_mode=PRODUCER_MODES,
        catalog_type=supported_catalog_types(),
    )
    def test_dropped_column_no_collision(
        self, cloud_storage_type, query_engine, produce_mode, catalog_type
    ):
        """
        Translate some records, drop field A, translate some more, reintroduce field A  *by name*
        (this should create a *new* column). Confirm that 'select A' reads only the new column,
        producing nulls for all rows written prior to the final update.
        """
        label = "drop_column"
        tc = self.valid_cases[label]
        with self.setup_services(query_engine, catalog_type=catalog_type) as dl:
            topic, table = self.topic_and_table(label, produce_mode)
            count = 10
            ctx = TranslationContext()
            initial_schema = tc.initial_schema
            next_schema = tc.next_schema

            dropped_field_names = list(
                set(initial_schema.field_names) - set(next_schema.field_names)
            )

            for schema in [initial_schema, next_schema]:
                schema.produce(dl, topic, count, ctx, mode=produce_mode)
                schema.check_table_schema(dl, table, query_engine)
                select_out = self.select(dl, table, query_engine, schema.field_names)
                assert len(select_out) == ctx.total, (
                    f"Expected {ctx.total} rows, got {select_out}"
                )

            restored_schema = GenericSchema(
                fields=[
                    {
                        "name": "verifier_string",
                        "type": "string",
                    },
                    {
                        "name": "ordinal",
                        "type": "long",
                    },
                ],
                generate_record=lambda x: {
                    "verifier_string": f"verify-{x}",
                    "ordinal": int(x),
                },
                spark_table=[
                    ("verifier_string", "string"),
                    ("ordinal", "bigint"),
                ],
                trino_table=[
                    ("verifier_string", "varchar"),
                    ("ordinal", "bigint"),
                ],
            )

            restored_schema.produce(dl, topic, count, ctx, mode=produce_mode)
            restored_schema.check_table_schema(dl, table, query_engine)

            select_out = self.select(dl, table, query_engine, dropped_field_names)
            assert len(select_out) == count * 3, (
                f"Expected {count * 3} rows, got {select_out}"
            )

            assert all(r[1] is None for r in select_out[: count * 2]), (
                f"Expected nulls for reintroduced {dropped_field_names} in first {count * 2} rows, got {select_out[: count * 2]}"
            )
            assert all(r[1] is not None for r in select_out[count * 2 :]), (
                f"Expected non-nulls for {dropped_field_names} in last {count} rows, got {select_out[count * 2 :]}"
            )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        produce_mode=PRODUCER_MODES,
        catalog_type=supported_catalog_types(),
    )
    def test_dropped_column_select_fails(
        self, cloud_storage_type, query_engine, produce_mode, catalog_type
    ):
        """
        Test that selecting a dropped column fails "gracefully" - or at least
        predictably and consistently.
        """
        label = "drop_column"
        tc = self.valid_cases[label]
        with self.setup_services(query_engine, catalog_type=catalog_type) as dl:
            topic, table = self.topic_and_table(label, produce_mode)
            count = 10
            ctx = TranslationContext()
            initial_schema = tc.initial_schema
            next_schema = tc.next_schema
            dropped_field_names = list(
                set(initial_schema.field_names) - set(next_schema.field_names)
            )

            for schema in [initial_schema, next_schema]:
                schema.produce(dl, topic, count, ctx, mode=produce_mode)
                schema.check_table_schema(dl, table, query_engine)

            if query_engine == QueryEngineType.SPARK:
                with expect_exception(
                    pyhive.exc.OperationalError,
                    lambda e: "UNRESOLVED_COLUMN" in e.args[0].status.errorMessage,
                ):
                    self.select(dl, table, query_engine, dropped_field_names)
            else:
                with expect_exception(
                    pyhive.exc.DatabaseError,
                    lambda e: e.args[0].get("errorName") == "COLUMN_NOT_FOUND",
                ):
                    select_out = self.select(dl, query_engine, dropped_field_names)

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        catalog_type=supported_catalog_types(),
    )
    def test_reorder_columns(self, cloud_storage_type, query_engine, catalog_type):
        """
        Test that changing the order of columns doesn't change the values
        associated with a column or field name.
        """
        label = "reorder_columns"
        tc = self.valid_cases[label]
        with self.setup_services(
            query_engine,
            catalog_type=catalog_type,
            test_cases={
                label: tc,
            },
        ) as dl:
            for produce_mode in PRODUCER_MODES:
                topic, table = self.topic_and_table(label, produce_mode)
                count = 10
                ctx = TranslationContext()
                initial_schema = tc.initial_schema
                next_schema = tc.next_schema
                for schema in [initial_schema, next_schema]:
                    schema.produce(dl, topic, count, ctx, mode=produce_mode)
                    schema.check_table_schema(dl, table, query_engine)

                for field in initial_schema.field_names:
                    select_out = self.select(dl, table, query_engine, [field])
                    assert len(select_out) == count * 2, (
                        f"Expected {count * 2} rows, got {len(select_out)}"
                    )
                    assert all(r[1] == field for r in select_out), (
                        f"{field} column mangled: {select_out}"
                    )

    @cluster(num_nodes=3)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=QUERY_ENGINES,
        catalog_type=supported_catalog_types(),
    )
    def test_old_schema_writer(self, cloud_storage_type, query_engine, catalog_type):
        """
        Tests that, after a backwards compatible update from schema A to schema B, we can keep
        tranlsating records produced with schema A without another schema update by falling back
        to an already extant parquet writer for schema A.
        """
        with self.setup_services(
            query_engine,
            catalog_type=catalog_type,
            test_cases=self.valid_cases,
            with_partitioning=False,
        ) as dl:
            for label, tc, produce_mode in self.cases_by_modes(self.valid_cases):
                topic, table = self.topic_and_table(label, produce_mode)
                count = 10
                ctx = TranslationContext()

                initial_schema, next_schema, _, _ = tc

                for schema in [initial_schema, next_schema]:
                    schema.produce(dl, topic, count, ctx, mode=produce_mode)
                    schema.check_table_schema(dl, table, query_engine)

                initial_schema.produce(dl, topic, count, ctx, mode=produce_mode)
                next_schema.check_table_schema(dl, table, query_engine)

                select_out = self.select(
                    dl, table, query_engine, next_schema.field_names
                )

                assert len(select_out) == count * 3, (
                    f"Expected {count * 3} rows, got {len(select_out)}"
                )

                # Check that producing with latest schema still works too.
                next_schema.produce(dl, topic, count, ctx, mode=produce_mode)

                select_out = self.select(
                    dl, table, query_engine, next_schema.field_names
                )

                assert len(select_out) == count * 4, (
                    f"Expected {count * 4} rows, got {len(select_out)}"
                )
