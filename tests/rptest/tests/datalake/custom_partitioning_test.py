# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import itertools
import json
import time
import random
from uuid import uuid4

from requests.exceptions import HTTPError
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types
from rptest.utils.rpcn_utils import counter_stream_config


class DatalakeCustomPartitioningConfigTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeCustomPartitioningConfigTest,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={"iceberg_enabled": True},
                             *args,
                             **kwargs)

    @cluster(num_nodes=1)
    def test_configs(self):
        rpk = RpkTool(self.redpanda)
        try:
            rpk.create_topic("foo",
                             config={
                                 "redpanda.iceberg.mode":
                                 "value_schema_id_prefix",
                                 "redpanda.iceberg.partition.spec":
                                 "(hour(field"
                             })
        except RpkException:
            pass
        else:
            assert False, "creating topic with invalid spec should be forbidden"

        # should succeed
        rpk.create_topic("foo",
                         config={
                             "redpanda.iceberg.mode": "value_schema_id_prefix",
                             "redpanda.iceberg.partition.spec": "(hour(field))"
                         })

        try:
            rpk.alter_topic_config("foo", "redpanda.iceberg.partition.spec",
                                   "(unknown_transform(field))")
        except RpkException:
            pass
        else:
            assert False, "altering spec to invalid string should be forbidden"

        for spec in [
                "((unparseable", "(not_redpanda_field)", "(redpanda.offset)"
        ]:
            try:
                self.redpanda.set_cluster_config(
                    {"iceberg_default_partition_spec": spec})
            except HTTPError as e:
                if e.response.status_code != 400:
                    raise
            else:
                assert False, "setting default spec to invalid string should be forbidden"

        # should succeed
        self.redpanda.set_cluster_config(
            {"iceberg_default_partition_spec": "(day(redpanda.timestamp))"})

        # topic with default spec
        rpk.create_topic(
            "bar", config={"redpanda.iceberg.mode": "value_schema_id_prefix"})

        topic_configs = rpk.describe_topic_configs("bar")
        assert topic_configs["redpanda.iceberg.partition.spec"][0] == \
            "(day(redpanda.timestamp))"


AVRO_SCHEMA_STR = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "event_type", "type": "string"},
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}}
    ]
}
"""

AVRO_SCHEMA_STR_2 = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "event_type", "type": "string"},
        {"name": "number", "type": "long"},
        {"name": "timestamp_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "timestamp_date", "type": {"type": "int", "logicalType": "date"}}
    ]
}
"""


class DatalakeCustomPartitioningTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeCustomPartitioningTest,
              self).__init__(test_ctx,
                             num_brokers=4,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": True,
                                 "iceberg_catalog_commit_interval_ms": 5000,
                                 "iceberg_target_lag_ms": 5000,
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             *args,
                             **kwargs)

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def create_producer(self, schema=AVRO_SCHEMA_STR):
        value_serializer = AvroSerializer(
            SchemaRegistryClient(
                {"url": self.redpanda.schema_reg().split(",")[0]}), schema)
        return SerializingProducer({
            'bootstrap.servers': self.redpanda.brokers(),
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': value_serializer,
        })

    def produce(self,
                dl,
                producer,
                topic_name,
                msg_count,
                already_produced=0,
                gen_record=None):
        # Have all records share the same timestamp, so that they are
        # guaranteed to end up in the same hour partition.
        timestamp = time.time()
        for i in range(msg_count):
            ev_type = random.choice(["type_A", "type_B"])
            record = gen_record(already_produced + i, ev_type, timestamp) if gen_record is not None \
                else {
                        "event_type": ev_type,
                        "number": already_produced + i,
                        "timestamp_us": int(timestamp * 1000000),
                }
            producer.produce(
                topic=topic_name,
                # key to ensure that all partitions get some records
                key=str(uuid4()),
                value=record)
        producer.flush()
        dl.wait_for_translation(topic_name,
                                msg_count=already_produced + msg_count)

    def list_files(self,
                   dl: DatalakeServices,
                   query_engine: QueryEngineType,
                   topic_name: str,
                   select="file_path"):
        if query_engine == QueryEngineType.SPARK:
            return set(dl.spark().run_query_fetch_all(
                f"select {select} from redpanda.{topic_name}.files"))
        elif query_engine == QueryEngineType.TRINO:
            return set(dl.trino().run_query_fetch_all(
                f'select {select} from redpanda."{topic_name}$files"'))

    def num_files(self, dl: DatalakeServices, query_engine: QueryEngineType,
                  topic_name: str):
        partitions = self.list_files(dl,
                                     query_engine,
                                     topic_name,
                                     select="file_path")
        self.logger.debug(f"iceberg files partitions: {partitions}")
        return len(partitions)

    def num_partitions(self, dl: DatalakeServices,
                       query_engine: QueryEngineType, topic_name: str):
        partitions = self.list_files(dl,
                                     query_engine,
                                     topic_name,
                                     select="partition")
        self.logger.debug(f"iceberg files partitions: {partitions}")
        return len(partitions)

    def describe_partitioning(self, dl: DatalakeServices,
                              query_engine: QueryEngineType, topic_name: str):
        table_name = f"redpanda.{topic_name}"
        if query_engine == QueryEngineType.SPARK:
            spark = dl.spark()
            spark_describe_out = spark.run_query_fetch_all(
                f"describe {table_name}")
            # If there is just 1 field in the partition spec, partition info
            # starts with '# Partition Information', if there is more, it starts
            # with '# Partitioning'.
            return list(
                itertools.dropwhile(
                    lambda r: not r[0].startswith('# Partition'),
                    spark_describe_out))
        elif query_engine == QueryEngineType.TRINO:
            trino = dl.trino()
            desc = trino.run_query_fetch_all(
                f'describe redpanda."{topic_name}$partitions"')
            return [e for e in desc if e[0] == 'partition']
        else:
            assert False

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            catalog_type=supported_catalog_types())
    def test_basic(self, cloud_storage_type, catalog_type):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              catalog_type=catalog_type,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            # Create an iceberg topic with a custom partition spec and produce
            # some data.

            topic_name = "foo"
            msg_count = 1000
            partitions = 5
            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=partitions,
                iceberg_mode="value_schema_id_prefix",
                config={
                    "redpanda.iceberg.partition.spec":
                    "(hour(timestamp_us), event_type)"
                })

            producer = self.create_producer()
            self.produce(dl, producer, topic_name, msg_count)

            # Check that created table has the correct partition spec.

            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name)
            expected_partitioning = [
                ('# Partitioning', '', ''),
                ('Part 0', 'hours(timestamp_us)', ''),
                ('Part 1', 'event_type', ''),
            ]
            assert describe_partitioning == expected_partitioning

            # Check that files are correctly partitioned and that
            # spark can use this partitioning for a delete query.

            table_name = f"redpanda.{topic_name}"

            files_before = self.list_files(dl, QueryEngineType.SPARK,
                                           topic_name)
            assert len(files_before) == partitions * 2, \
                f"Expected {partitions * 2} partitions, got {partitions_before}"

            dl.spark().make_client().cursor().execute(
                f"delete from {table_name} where event_type='type_A'")

            files_after = self.list_files(dl, QueryEngineType.SPARK,
                                          topic_name)
            assert len(files_after) == partitions, \
                f"Expected {partitions} partitions, got {len(files_after)}"
            assert files_after.issubset(files_before)

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            catalog_type=supported_catalog_types())
    def test_spec_evolution(self, cloud_storage_type, catalog_type):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              catalog_type=catalog_type,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            # Create an iceberg topic with a custom partition spec and produce
            # some data.

            topic_name = "foo"
            msg_count = 1000
            partitions = 5
            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=partitions,
                iceberg_mode="value_schema_id_prefix",
                config={"redpanda.iceberg.partition.spec": "()"})

            producer = self.create_producer()

            already_produced = 0

            def produce():
                nonlocal already_produced
                self.produce(dl, producer, topic_name, msg_count,
                             already_produced)
                already_produced += msg_count

            produce()

            table_name = f"redpanda.{topic_name}"
            spark = dl.spark()

            # The translator for each partition should produce one file.
            n = self.num_files(dl, QueryEngineType.SPARK, topic_name)
            assert n == partitions, f"Expected {partitions=}, got {n}"

            # partition spec should reflect the original value
            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name)
            assert describe_partitioning == []

            self.logger.info("adding a 2-field partition spec...")
            rpk = RpkTool(self.redpanda)
            rpk.alter_topic_config(topic_name,
                                   "redpanda.iceberg.partition.spec",
                                   "(hour(timestamp_us), event_type)")

            produce()

            # The translator for each partition should produce one file
            # for each event type.
            n = self.num_files(dl, QueryEngineType.SPARK, topic_name)
            assert n == partitions * 3, \
                f"Expected partitions={partitions*3}, got {n}"

            # partition spec should reflect the altered value
            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name)
            expected_partitioning = [
                ('# Partitioning', '', ''),
                ('Part 0', 'hours(timestamp_us)', ''),
                ('Part 1', 'event_type', ''),
            ]
            assert describe_partitioning == expected_partitioning

            self.logger.info("removing one field from partition spec...")
            rpk.alter_topic_config(topic_name,
                                   "redpanda.iceberg.partition.spec",
                                   "(event_type)")

            produce()

            # The translator for each partition should produce one file
            # for each event type.
            n = self.num_files(dl, QueryEngineType.SPARK, topic_name)
            assert n == partitions * 5, \
                f"Expected partitions={partitions*5}, got {n}"

            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name)
            expected_partitioning = [
                ('# Partition Information', '', ''),
                ('# col_name', 'data_type', 'comment'),
                ('event_type', 'string', None),
            ]
            assert describe_partitioning == expected_partitioning

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            catalog_type=supported_catalog_types())
    def test_sticky_default(self, cloud_storage_type, catalog_type):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              catalog_type=catalog_type,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:

            self.redpanda.set_cluster_config(
                {"iceberg_default_partition_spec": "()"})

            # Create an iceberg topic and produce
            # some data.

            topic_name = "foo"
            msg_count = 1000
            partitions = 5
            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=partitions,
                iceberg_mode="value_schema_id_prefix")

            producer = self.create_producer()

            already_produced = 0

            def produce(topic):
                nonlocal already_produced
                self.produce(dl, producer, topic, msg_count, already_produced)
                already_produced += msg_count

            produce(topic_name)

            # partition spec should reflect the original value
            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name)
            assert describe_partitioning == []

            self.logger.info("altering default partition spec...")
            self.redpanda.set_cluster_config({
                "iceberg_default_partition_spec":
                "(hour(redpanda.timestamp))"
            })

            produce(topic_name)

            # partition spec should reflect the original value
            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name)
            assert describe_partitioning == []

            # create another topic, but don't enable iceberg just yet.
            another_topic = "bar"
            rpk = RpkTool(self.redpanda)
            rpk.create_topic(topic=another_topic,
                             partitions=partitions,
                             replicas=3)

            self.redpanda.set_cluster_config({
                "iceberg_default_partition_spec":
                "(day(redpanda.timestamp))"
            })
            rpk.alter_topic_config(another_topic, "redpanda.iceberg.mode",
                                   "value_schema_id_prefix")

            self.redpanda.set_cluster_config(
                {"iceberg_default_partition_spec": "()"})

            already_produced = 0
            produce(another_topic)

            # partition spec should reflect the value at the time iceberg was enabled
            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, another_topic)
            expected_partitioning = [
                ('# Partitioning', '', ''),
                ('Part 0', 'days(redpanda.timestamp)', ''),
            ]
            assert describe_partitioning == expected_partitioning

    @cluster(num_nodes=7)
    @matrix(cloud_storage_type=supported_storage_types(),
            catalog_type=supported_catalog_types())
    def test_spec_evolution_rpcn(self, cloud_storage_type, catalog_type):
        """
        Test changing the partition spec with uninterrupted produce load
        coming from redpanda connect.
        """

        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              catalog_type=catalog_type,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            avro_schema = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "event_type", "type": "int"},
        {"name": "number", "type": "long"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "verifier_string", "type": "string"}
    ]
}
            """

            rpk = RpkTool(self.redpanda)
            rpk.create_schema_from_str("verifier_schema", avro_schema)

            topic_name = "foo"
            partitions = 5
            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=partitions,
                iceberg_mode="value_schema_id_prefix",
                config={
                    "redpanda.iceberg.partition.spec": "(hour(timestamp))"
                })

            connect = RedpandaConnectService(self.test_context, self.redpanda)
            connect.start()

            # Use stream config that will produce events until we stop it explicitly
            timestamp = time.time()
            num_event_types = 3
            avro_stream_config = counter_stream_config(
                self.redpanda,
                topic_name,
                "verifier_schema",
                dict(
                    event_type=f"random_int(min:1, max:{num_event_types})",
                    number="this",
                    timestamp=f"{int(timestamp * 1000)}",
                    verifier_string="uuid_v4()",
                ),
                1_000_000,
                interval_ms=1)
            connect.start_stream(name="ducky_stream",
                                 config=avro_stream_config)

            verifier = DatalakeVerifier(self.redpanda, topic_name, dl.spark())
            verifier.start()

            dl.wait_for_iceberg_table("redpanda",
                                      topic_name,
                                      timeout=60,
                                      backoff_sec=5)
            self.redpanda.wait_until(
                lambda: dl.spark().count_table("redpanda", topic_name) >= 1000,
                timeout_sec=60,
                backoff_sec=2)

            n = self.num_partitions(dl, QueryEngineType.SPARK, topic_name)
            assert n == 1, f"expected partitions={1}, got {n}"

            self.logger.info("altering topic partition spec...")
            rpk.alter_topic_config(topic_name,
                                   "redpanda.iceberg.partition.spec",
                                   "(event_type)")

            self.redpanda.wait_until(
                lambda: self.num_partitions(dl, QueryEngineType.SPARK,
                                            topic_name) == 1 + num_event_types,
                timeout_sec=60,
                backoff_sec=5,
                err_msg="failed to wait for new partitions to appear")

            expected_partitioning = [
                ('# Partition Information', '', ''),
                ('# col_name', 'data_type', 'comment'),
                ('event_type', 'int', None),
            ]
            assert self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name) == expected_partitioning

            connect.stop_stream("ducky_stream", should_finish=False)
            verifier.wait()

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            catalog_type=supported_catalog_types(),
            query_engine=[
                QueryEngineType.SPARK,
                QueryEngineType.TRINO,
            ])
    def test_schema_evolution(self, cloud_storage_type, catalog_type,
                              query_engine):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              catalog_type=catalog_type,
                              include_query_engines=[query_engine]) as dl:
            topic_name = "foo"
            msg_count = 100
            partitions = 1
            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=partitions,
                iceberg_mode="value_schema_id_prefix",
                config={
                    "redpanda.iceberg.partition.spec":
                    "(day(timestamp_us), event_type)",
                })

            SchemaRegistryClient({
                'url':
                self.redpanda.schema_reg().split(",")[0]
            }).set_compatibility(subject_name=f"{topic_name}-value",
                                 level="NONE")

            already_produced = 0

            def produce(schema: str, gen_record=None):
                nonlocal already_produced
                producer = self.create_producer(schema)
                self.produce(dl,
                             producer,
                             topic_name,
                             msg_count,
                             already_produced,
                             gen_record=gen_record)
                already_produced += msg_count

            produce(AVRO_SCHEMA_STR)

            table_name = f"redpanda.{topic_name}"

            # TODO(oren): remove
            # qe = dl.spark() if query_engine == QueryEngineType.SPARK \
            #     else dl.trino()

            # Check that created table has the correct partition spec.
            describe_partitioning = self.describe_partitioning(
                dl, query_engine, topic_name)
            expected_partitioning = None
            if query_engine == QueryEngineType.SPARK:
                expected_partitioning = [
                    ('# Partitioning', '', ''),
                    ('Part 0', 'days(timestamp_us)', ''),
                    ('Part 1', 'event_type', ''),
                ]
            elif query_engine == QueryEngineType.TRINO:
                expected_partitioning = [(
                    'partition',
                    'row(timestamp_us_day date, event_type varchar)',
                    '',
                    '',
                )]

            assert describe_partitioning == expected_partitioning, \
                    f"{expected_partitioning=}, got {describe_partitioning=}"

            files = self.list_files(dl, query_engine, topic_name)
            assert len(files) == partitions * 2, \
                f"Expected {partitions * 2} files, got {json.dumps(list(files), indent=1)}"

            dl.set_partition_spec_on_topic(
                topic_name, "(day(timestamp_date), event_type)")

            produce(AVRO_SCHEMA_STR_2,
                    gen_record=lambda n, ev, t: {
                        "event_type": ev,
                        "number": n,
                        "timestamp_us": int(t * 1000000),
                        "timestamp_date": int(t / 60 / 60 / 24),
                    })

            describe_partitioning = self.describe_partitioning(
                dl, query_engine, topic_name)

            if query_engine == QueryEngineType.SPARK:
                expected_partitioning = [
                    ('# Partitioning', '', ''),
                    ('Part 0', 'days(timestamp_date)', ''),
                    ('Part 1', 'event_type', ''),
                ]
            elif query_engine == QueryEngineType.TRINO:
                expected_partitioning = [(
                    'partition',
                    'row(timestamp_us_day date, event_type varchar, timestamp_date_day date)',
                    '',
                    '',
                )]

            assert describe_partitioning == expected_partitioning, \
                    f"{expected_partitioning=}, got {describe_partitioning=}"

            files = self.list_files(dl, query_engine, topic_name)
            assert len(files) == partitions * 4, \
                f"Expected {partitions * 2} files, got {json.dumps(list(files), indent=1)}"

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            catalog_type=supported_catalog_types())
    def test_many_partitions(self, cloud_storage_type, catalog_type):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              catalog_type=catalog_type,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            topic_name = "foo"
            msg_count = 100000
            partitions = 5
            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=partitions,
                iceberg_mode="value_schema_id_prefix",
                config={
                    "redpanda.iceberg.partition.spec": "(timestamp_us)",
                })

            producer = self.create_producer(AVRO_SCHEMA_STR)
            self.produce(
                dl,
                producer,
                topic_name,
                msg_count,
                0,
                gen_record=lambda n, ev, t: {
                    "event_type": ev,
                    "number": n,
                    "timestamp_us": int(t + n),
                },
            )

            describe_partitioning = self.describe_partitioning(
                dl, QueryEngineType.SPARK, topic_name)
            expected_partitioning = [
                ('# Partition Information', '', ''),
                ('# col_name', 'data_type', 'comment'),
                ('timestamp_us', 'timestamp_ntz', None),
            ]

            assert describe_partitioning == expected_partitioning, \
                    f"{expected_partitioning=}, got {describe_partitioning=}"

            files = self.list_files(dl, QueryEngineType.SPARK, topic_name)
            assert len(files) ==  msg_count, \
                f"Expected {partitions * msg_count} files, got {len(files)}"
