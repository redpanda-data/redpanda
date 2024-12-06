# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.utils import supported_storage_types
from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark, LocalPayloadDirectory
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ducktape.mark import matrix

import rptest.tests.datalake.schemas.linear_pb2 as linear_pb2

import string
import random


class DatalakeOMBTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(DatalakeOMBTest,
              self).__init__(test_ctx,
                             num_brokers=3,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             pandaproxy_config=PandaproxyConfig(),
                             *args,
                             **kwargs)

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def _get_schema_registry_client(self) -> SchemaRegistryClient:
        schema_registry_url = self.redpanda.schema_reg().split(',', 1)[0]
        schema_registry_conf: dict[str, str] = {'url': schema_registry_url}

        return SchemaRegistryClient(schema_registry_conf)

    def _random_linear_msg(self, msg_type, str_len: int):
        """ For a protobuf message of `msg_type` which is just a linear list of string fields generate a random message. """
        msg_descriptor = msg_type.DESCRIPTOR
        msg = msg_type()
        fields: list[str] = [field for field in msg_descriptor.fields_by_name]
        for field in fields:
            random_str = ''.join(
                random.choices(string.ascii_letters, k=str_len))
            setattr(msg, field, random_str)

        return msg

    @cluster(num_nodes=7)
    @matrix(cloud_storage_type=supported_storage_types())
    def basic_workload_linear_20_test(self, cloud_storage_type):
        topic_name = "atestingtopic"
        msg_type = linear_pb2.Linear20
        msg_field_size_bytes = 13
        producer_rate_bytes_s = 2 * 1024 * 1024

        with DatalakeServices(self._ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[]) as dl:
            dl.create_iceberg_enabled_topic(
                name=topic_name,
                partitions=50,
                replicas=3,
                iceberg_mode="value_schema_id_prefix")

            schema_registry_client = self._get_schema_registry_client()

            payloads = LocalPayloadDirectory()
            # Note that `ps` is used to serialize the message and register it's schema with Redpanda's schema registry.
            ps = ProtobufSerializer(msg_type, schema_registry_client,
                                    {'use.deprecated.format': False})

            payload_size = 0
            for i in range(0, 1000):
                msg = self._random_linear_msg(msg_type, msg_field_size_bytes)
                msg_bytes = ps(
                    msg, SerializationContext(topic_name, MessageField.VALUE))
                payload_size = len(msg_bytes)
                payloads.add_payload(f"message_{i}", msg_bytes)

            workload = {
                "name": "CommonWorkload",
                "existing_topic_list": [topic_name],
                "subscriptions_per_topic": 1,
                "consumer_per_subscription": 25,
                "producers_per_topic": 25,
                "producer_rate": producer_rate_bytes_s // payload_size,
                "consumer_backlog_size_GB": 0,
                "test_duration_minutes": 3,
                "warmup_duration_minutes": 2,
            }
            driver = {
                "name": "CommonWorkloadDriver",
                "replication_factor": 3,
                "request_timeout": 300000,
                "producer_config": {
                    "enable.idempotence": "true",
                    "acks": "all",
                    "linger.ms": 10,
                    "max.in.flight.requests.per.connection": 5,
                    "batch.size": 16384,
                },
                "consumer_config": {
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "false",
                    "max.partition.fetch.bytes": 131072
                },
            }
            validator = {
                OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                    OMBSampleConfigurations.gte(
                        (0.9 * producer_rate_bytes_s) // (1024 * 1024))
                ]
            }

            benchmark = OpenMessagingBenchmark(ctx=self._ctx,
                                               redpanda=self.redpanda,
                                               driver=driver,
                                               workload=(workload, validator),
                                               topology="ensemble",
                                               local_payload_dir=payloads)
            benchmark.start()
            benchmark_time_min = benchmark.benchmark_time() + 5
            benchmark.wait(timeout_sec=benchmark_time_min * 60)
            benchmark.check_succeed()
