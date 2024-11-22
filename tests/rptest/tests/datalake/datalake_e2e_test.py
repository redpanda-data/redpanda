# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from typing import Optional
from rptest.clients.serde_client_utils import SchemaType, SerdeClientType
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster

from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings
from rptest.services.serde_client import SerdeClient
from rptest.services.redpanda import CloudStorageType, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix


class DatalakeE2ETests(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeE2ETests,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 5000
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             pandaproxy_config=PandaproxyConfig(),
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def _get_serde_client(
            self,
            schema_type: SchemaType,
            client_type: SerdeClientType,
            topic: str,
            count: int,
            skip_known_types: Optional[bool] = None,
            subject_name_strategy: Optional[str] = None,
            payload_class: Optional[str] = None,
            compression_type: Optional[TopicSpec.CompressionTypes] = None):
        schema_reg = self.redpanda.schema_reg().split(',', 1)[0]
        sec_cfg = self.redpanda.kafka_client_security().to_dict()

        return SerdeClient(self.test_context,
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
                           compression_type=compression_type)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(),
            query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
            filesystem_catalog_mode=[True])
    def test_e2e_basic(self, cloud_storage_type, query_engine,
                       filesystem_catalog_mode):
        # Create a topic
        # Produce some events
        # Ensure they end up in datalake
        count = 100
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(),
            query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO])
    def test_avro_schema(self, cloud_storage_type, query_engine):
        count = 100
        table_name = f"redpanda.{self.topic_name}"

        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name)
            avro_serde_client = self._get_serde_client(SchemaType.AVRO,
                                                       SerdeClientType.Golang,
                                                       self.topic_name, count)
            avro_serde_client.start()
            avro_serde_client.wait()
            dl.wait_for_translation(self.topic_name, msg_count=count)

            if query_engine == QueryEngineType.TRINO:
                trino = dl.trino()
                trino_expected_out = [(
                    'redpanda',
                    'row(partition integer, offset bigint, timestamp timestamp(6), headers array(row(key varbinary, value varbinary)), key varbinary)',
                    '', ''), ('val', 'bigint', '', '')]
                trino_describe_out = trino.run_query_fetch_all(
                    f"describe {table_name}")
                assert trino_describe_out == trino_expected_out, str(
                    trino_describe_out)
            else:
                spark = dl.spark()
                spark_expected_out = [(
                    'redpanda',
                    'struct<partition:int,offset:bigint,timestamp:timestamp_ntz,headers:array<struct<key:binary,value:binary>>,key:binary>',
                    None), ('val', 'bigint', None), ('', '', ''),
                                      ('# Partitioning', '', ''),
                                      ('Part 0', 'hours(redpanda.timestamp)',
                                       '')]
                spark_describe_out = spark.run_query_fetch_all(
                    f"describe {table_name}")
                assert spark_describe_out == spark_expected_out, str(
                    spark_describe_out)
