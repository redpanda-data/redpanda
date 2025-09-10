# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import itertools
import random
import time
from uuid import uuid4

import pyhive
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.parallel import execute_in_parallel


class DatalakeManyTopicsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeManyTopicsTest, self).__init__(
            test_ctx,
            num_brokers=6,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000,
                "iceberg_target_lag_ms": 5000,
            },
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def create_topics(self, n_topics, partitions=5, replicas=3):
        all_topic_names = [f"topic_{i:06d}" for i in range(n_topics)]

        def create_batch(names):
            # create own admin and rpk instances to avoid concurrent accesses
            # to common ones
            rpk = RpkTool(self.redpanda)
            for n in names:
                rpk.create_topic(
                    topic=n,
                    partitions=partitions,
                    replicas=replicas,
                    config={"redpanda.iceberg.mode": "value_schema_id_prefix"},
                )

        execute_in_parallel(all_topic_names, create_batch)
        return all_topic_names

    def create_producer(self, schema):
        value_serializer = AvroSerializer(
            SchemaRegistryClient({"url": self.redpanda.schema_reg().split(",")[0]}),
            schema,
        )

        return SerializingProducer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": value_serializer,
            }
        )

    def produce_messages(self, schema, create_record, topics, msg_per_topic):
        def produce_batch(topics):
            producer = self.create_producer(schema)
            for topic in topics:
                for i in range(msg_per_topic):
                    producer.produce(
                        topic=topic,
                        # key to ensure that all partitions get some records
                        key=str(uuid4()),
                        value=create_record(i),
                    )
            producer.flush()

        execute_in_parallel(topics, produce_batch)

    @cluster(num_nodes=8)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_basic(self, cloud_storage_type):
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            catalog_type=CatalogType.REST_JDBC,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            all_topics = self.create_topics(
                1000 if self.redpanda.dedicated_nodes else 20
            )
            self.logger.info("creating topics finished")

            schema1 = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "number", "type": "long"}
    ]
}
            """

            def create_record1(i):
                return {
                    "timestamp": int(time.time()) * 1000,
                    "number": i,
                }

            self.produce_messages(
                schema1, create_record1, topics=all_topics, msg_per_topic=100
            )
            self.logger.info("producing finished")

            def tables_created():
                try:
                    return len(
                        spark.run_query_fetch_all("show tables in redpanda")
                    ) >= len(all_topics)
                except pyhive.exc.OperationalError:
                    # thrown if the namespace hasn't been created yet
                    return False

            spark = dl.spark()
            self.redpanda.wait_until(tables_created, timeout_sec=180, backoff_sec=5)
            self.logger.info("table creation finished")

            def all_translated(msg_per_topic):
                for topic in random.sample(all_topics, 10):
                    count = spark.run_query_fetch_all(
                        f"select count(*) from redpanda.{topic}"
                    )
                    if count[0][0] < msg_per_topic:
                        return False
                return True

            self.redpanda.wait_until(
                lambda: all_translated(100),
                timeout_sec=180,
                backoff_sec=5,
                err_msg="timed out waiting for first translation",
            )
            self.logger.info("translation finished")

            schema2 = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {"name": "event_type", "type": "string", "default": "type_A"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "number", "type": "long"}
    ]
}
            """

            def create_record2(i):
                return {
                    "event_type": random.choice(["type_A", "type_B"]),
                    "timestamp": int(time.time()) * 1000,
                    "number": i,
                }

            self.produce_messages(
                schema2, create_record2, topics=all_topics, msg_per_topic=100
            )
            self.logger.info("producing with new schema finished")

            self.redpanda.wait_until(
                lambda: all_translated(200),
                timeout_sec=180,
                backoff_sec=5,
                err_msg="timed out waiting for translation after schema change",
            )
            self.logger.info("translation finished")

            def all_updated_schema():
                for topic in random.sample(all_topics, 10):
                    describe = spark.run_query_fetch_all(f"describe redpanda.{topic}")
                    expected_line = ("event_type", "string", None)
                    if expected_line not in describe:
                        return False
                return True

            self.redpanda.wait_until(
                all_updated_schema,
                timeout_sec=30,
                backoff_sec=5,
                err_msg="timed out waiting for schema update",
            )

            def set_property_for_batch(batch):
                rpk = RpkTool(self.redpanda)
                for topic in batch:
                    rpk.alter_topic_config(
                        topic, "redpanda.iceberg.partition.spec", "(event_type)"
                    )

            execute_in_parallel(all_topics, set_property_for_batch)
            self.logger.info("partition spec property changed")

            self.produce_messages(
                schema2, create_record2, topics=all_topics, msg_per_topic=100
            )
            self.logger.info("producing with new partition spec finished")

            self.redpanda.wait_until(
                lambda: all_translated(300),
                timeout_sec=180,
                backoff_sec=5,
                err_msg="timed out waiting for translation after spec update",
            )
            self.logger.info("translation finished")

            def all_updated_spec():
                for topic in random.sample(all_topics, 10):
                    describe = spark.run_query_fetch_all(f"describe redpanda.{topic}")
                    partitioning = list(
                        itertools.dropwhile(
                            lambda r: not r[0].startswith("# Partition"), describe
                        )
                    )
                    expected_line = ("event_type", "string", None)
                    if expected_line not in partitioning:
                        return False
                return True

            self.redpanda.wait_until(
                all_updated_spec,
                timeout_sec=30,
                backoff_sec=5,
                err_msg="timed out waiting for partition spec update",
            )
