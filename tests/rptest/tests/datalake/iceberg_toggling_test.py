# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import operator
import time
from threading import Thread

import ducktape.errors
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest

AVRO_SCHEMA_STR = """
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "Counter",
    "fields": [
        {"name": "count", "type": "long"}
    ]
}
"""


class IcebergTogglingTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(IcebergTogglingTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_target_lag_ms": 1000,
                "iceberg_catalog_commit_interval_ms": 1000,
                # Help datalake verifier queries scan less data.
                "iceberg_default_partition_spec": "redpanda.partition",
            },
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )
        self.rpk = RpkTool(self.redpanda)
        self.topic_name = "test"
        self.stream_name = "ducky_stream"

    def setUp(self):
        pass

    def produce_avro_records(self, schema=AVRO_SCHEMA_STR, record_count=100, seed=0):
        value_serializer = AvroSerializer(
            SchemaRegistryClient({"url": self.redpanda.schema_reg().split(",")[0]}),
            schema,
        )
        producer = SerializingProducer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": value_serializer,
            }
        )

        for i in range(record_count):
            record = {
                "count": seed + i,
            }
            producer.produce(
                topic=self.topic_name,
                key=str(seed + i),
                value=record,
            )
        producer.flush()

    def set_iceberg_mode(self, mode: str):
        self.rpk.alter_topic_config(
            self.topic_name, TopicSpec.PROPERTY_ICEBERG_MODE, mode
        )
        self.logger.info(f"Set iceberg mode to {mode} for topic {self.topic_name}")

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_iceberg_toggling(self, cloud_storage_type):
        """
        Toggle iceberg mode on and off while streaming data to a topic and verify
        that the translation is paused and resumed correctly.

        Using higher partition count to increase the chance of catching potential
        issues if they exist.
        """
        NUM_PARTITIONS = 100
        ADDITIONAL_RECORDS_PER_ITERATION = 50
        NUM_TOGGLING_ITERATIONS = 5

        with DatalakeServices(
            self.test_context,
            self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=NUM_PARTITIONS)

            # A long running counter that runs until stopped
            connect = dl.start_counter_stream(
                topic=self.topic_name, name=self.stream_name, count=0, interval="1ms"
            )

            def ensure_stream_progress(target: int):
                wait_until(
                    lambda: connect.total_records_sent(self.stream_name) >= target,
                    timeout_sec=60,
                    backoff_sec=5,
                    err_msg=f"Timed out waiting for stream producer to reach target: {target}",
                )

            def wait_iceberg_quiesced(
                *, min_records=0, proof_interval_sec=5, timeout_sec=30
            ) -> int:
                """
                :param proof_interval_sec: The interval at which we check if the
                    iceberg has quiesced. Generally this should be a couple of
                    times larger than `iceberg_catalog_commit_interval_ms` and
                    `iceberg_target_lag_ms`, plus some buffer for committing to
                    the catalog.
                :param timeout_sec: The maximum time to wait for the iceberg to
                    quiesce.
                :return: The number of records in the iceberg table when it
                    quiesced.
                """
                # Record start time for timeout purposes
                start_time = time.time()

                # Record current values
                last_time = time.time()
                last_count = dl.spark().count_table("redpanda", self.topic_name)

                while time.time() - start_time < timeout_sec:
                    current_count = dl.spark().count_table("redpanda", self.topic_name)
                    self.logger.debug("Current count: {current_count}")

                    if current_count == last_count:
                        # If no more records were added and the proof interval
                        # has passed then we can consider the iceberg quiesced.
                        if (
                            last_time + proof_interval_sec < time.time()
                            and current_count >= min_records
                        ):
                            self.logger.info(
                                f"Iceberg quiesced in {time.time() - start_time} with {current_count} records"
                            )
                            return current_count
                    else:
                        # Reset the timer and count.
                        self.logger.debug(
                            f"Records added: {current_count - last_count}"
                        )
                        last_time = time.time()
                        last_count = current_count
                    # Don't spin too fast
                    time.sleep(1)

                assert last_count >= min_records, (
                    f"Timed out waiting for minimum records in iceberg {last_count=} < {min_records=}"
                )
                assert False, "Iceberg did not quiesce in time"

            for ix in range(1, NUM_TOGGLING_ITERATIONS + 1):
                self.logger.info(f"Starting toggling iteration {ix}")

                # Wait for some records to be produced.
                ensure_stream_progress(
                    connect.total_records_sent(self.stream_name)
                    + ADDITIONAL_RECORDS_PER_ITERATION
                )

                # Wait for records to be translated.
                min_records_expected = connect.total_records_sent(self.stream_name)
                dl.wait_for_translation(
                    self.topic_name,
                    msg_count=min_records_expected,
                    timeout=120,
                    progress_sec=30,
                    op=operator.gt,
                )

                self.rpk.alter_topic_config(
                    self.topic_name, TopicSpec.PROPERTY_ICEBERG_MODE, "disabled"
                )

                wait_iceberg_quiesced(min_records=min_records_expected)

                self.rpk.alter_topic_config(
                    self.topic_name, TopicSpec.PROPERTY_ICEBERG_MODE, "key_value"
                )

            # We're done, let's stop the stream.
            connect.stop_stream(name=self.stream_name, should_finish=None)

            # Wait for everything to be translated.
            total_row_count = connect.total_records_sent(self.stream_name)
            dl.wait_for_translation(self.topic_name, total_row_count)

            # Optimize the parquet files to aid the verifier.
            dl.spark().optimize_parquet_files("redpanda", self.topic_name)

            # Verify the data.
            DatalakeVerifier.oneshot(self.redpanda, self.topic_name, dl.spark())

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_iceberg_mode_toggling(self, cloud_storage_type):
        """
        Test toggling iceberg mode between disabled, value_schema_latest and value_schema_id_prefix
        on the same partition leader.
        """
        NUM_PARTITIONS = 1
        RECORD_COUNT = 100
        produced_records = 0

        with DatalakeServices(
            self.test_context,
            self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            # Create the topic with iceberg disabled
            dl.create_iceberg_enabled_topic(
                self.topic_name, partitions=NUM_PARTITIONS, iceberg_mode="disabled"
            )

            # Misconfigure iceberg mode to value_schema_latest
            self.set_iceberg_mode("value_schema_latest:subject=foo")
            self.produce_avro_records(record_count=RECORD_COUNT)
            produced_records += RECORD_COUNT

            try:
                dl.wait_for_translation(
                    self.topic_name,
                    msg_count=produced_records,
                    timeout=90,
                    progress_sec=30,
                )
                assert False, "Expected translation to fail but it did not"
            except ducktape.errors.TimeoutError:
                pass

            self.set_iceberg_mode("value_schema_id_prefix")

            dl.wait_for_translation(
                self.topic_name,
                msg_count=produced_records,
                timeout=90,
                progress_sec=30,
            )

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_concurrent_mode_toggling(self, cloud_storage_type):
        """
        Test toggling iceberg mode while producing data and translation is in progress.
        """
        NUM_PARTITIONS = 1
        RECORDS_PER_PRODUCE = 100
        TOGGLES = 200

        with DatalakeServices(
            self.test_context,
            self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        ) as dl:
            # Create the topic with iceberg disabled
            dl.create_iceberg_enabled_topic(
                self.topic_name, partitions=NUM_PARTITIONS, iceberg_mode="disabled"
            )
            stopped = False
            total_produced = 0

            def produce_until_stopped():
                nonlocal total_produced
                while not stopped:
                    self.produce_avro_records(record_count=RECORDS_PER_PRODUCE)
                    total_produced += RECORDS_PER_PRODUCE
                    time.sleep(0.1)

            produce_t = Thread(target=produce_until_stopped, daemon=True)
            produce_t.start()

            for i in range(TOGGLES):
                self.set_iceberg_mode("value_schema_latest:subject=foo")
                time.sleep(0.1)
                self.set_iceberg_mode("value_schema_id_prefix")

            stopped = True
            produce_t.join()

            self.set_iceberg_mode("value_schema_id_prefix")
            dl.wait_for_translation(
                self.topic_name, msg_count=total_produced, timeout=90, progress_sec=30
            )
