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
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from random import randint

from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings
from rptest.services.serde_client import SerdeClient
from rptest.services.redpanda import CloudStorageType, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.metrics_check import MetricCheck

NO_SCHEMA_ERRORS = [
    r'Must have parsed schema when using structured data mode',
    r'Error translating data to binary record'
]


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
            filesystem_catalog_mode=[False, True])
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
            dl.create_iceberg_enabled_topic(
                self.topic_name, iceberg_mode="value_schema_id_prefix")
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

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(),
            filesystem_catalog_mode=[True, False])
    def test_topic_lifecycle(self, cloud_storage_type,
                             filesystem_catalog_mode):
        count = 100
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            rpk = RpkTool(self.redpanda)

            # produce some data then delete the topic
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

            rpk.alter_topic_config(self.topic_name, "redpanda.iceberg.delete",
                                   "false")
            rpk.delete_topic(self.topic_name)

            # table is not deleted, it will contain messages from both topic instances
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=15)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=2 * count)

            # now table should be deleted
            rpk.delete_topic(self.topic_name)

            catalog_client = dl.catalog_client()

            def table_deleted():
                return not dl.table_exists(self.topic_name,
                                           client=catalog_client)

            wait_until(table_deleted,
                       timeout_sec=30,
                       backoff_sec=5,
                       err_msg="table was not deleted")

            # recreate an empty topic a few times
            for _ in range(3):
                dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
                rpk.delete_topic(self.topic_name)

            # check that the table is recreated after we start producing again
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=5)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)

    @cluster(num_nodes=3, log_allow_list=NO_SCHEMA_ERRORS)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_metrics(self, cloud_storage_type):

        commit_lag = 'vectorized_cluster_partition_iceberg_offsets_pending_commit'
        translation_lag = 'vectorized_cluster_partition_iceberg_offsets_pending_translation'

        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[]) as dl:

            dl.create_iceberg_enabled_topic(
                self.topic_name,
                partitions=1,
                replicas=1,
                iceberg_mode="value_schema_id_prefix")
            count = randint(12, 21)
            # Populate schemaless messages in schema-ed mode, this should
            # hold up translation and commits
            dl.produce_to_topic(self.topic_name, 1024, msg_count=count)

            m = MetricCheck(self.redpanda.logger,
                            self.redpanda,
                            self.redpanda.nodes[0],
                            [commit_lag, translation_lag],
                            labels={
                                'namespace': 'kafka',
                                'topic': self.topic_name,
                                'partition': '0'
                            },
                            reduce=sum)
            expectations = []
            for metric in [commit_lag, translation_lag]:
                expectations.append([metric, lambda _, val: val == count])

            # Ensure lag metric builds up as expected.
            wait_until(
                lambda: m.evaluate(expectations),
                timeout_sec=30,
                backoff_sec=5,
                err_msg=f"Timed out waiting for metrics to reach: {count}")
