# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import time
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.catalog_service import CatalogType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.util import firewall_blocked
from rptest.utils.rpcn_utils import counter_stream_config
from typing import Any


class TranslatorsStressTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(TranslatorsStressTest,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 1000
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             pandaproxy_config=PandaproxyConfig(),
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"
        self.rpcn = RedpandaConnectService(self.test_context, self.redpanda)

    def setUp(self):
        # NOTE: defer cluster startup to DatalakeServices.
        self.rpcn.start()

    def start_unstructured_topic_stream(
        self,
        dl: DatalakeServices,
        topic: str,
        replicas: int = 1,
        partitions: int = 1,
        create_topic: bool = True,
        topic_config: dict[str, Any] = dict()) -> str:
        """
        Creates a RPCN stream ingesting to non-structured the given Iceberg
        topic, optionally creating the topic. Returns the stream name.
        """
        # Send a low volume of records. We don't want to overwhelm the cluster.
        rpcn_modest_interval_ms = 100
        cfg = counter_stream_config(
            self.redpanda,
            topic,
            "",  # subject
            cnt=0,  # indefinite count
            interval_ms=rpcn_modest_interval_ms)
        if create_topic:
            dl.create_iceberg_enabled_topic(topic,
                                            replicas=replicas,
                                            partitions=partitions,
                                            iceberg_mode="key_value",
                                            config=topic_config)
        stream = f"{topic}_stream"
        self.rpcn.start_stream(name=stream, config=cfg)
        return stream

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translation_intervals(self, cloud_storage_type):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              include_query_engines=[QueryEngineType.SPARK],
                              catalog_type=CatalogType.REST_HADOOP) as dl:
            max_lag_prop_name = "redpanda.iceberg.target.lag.ms"
            realtime_topic = "rapidash"
            laggy_topic = "slowpoke"
            realtime_stream = self.start_unstructured_topic_stream(
                dl,
                realtime_topic,
                partitions=200,
                topic_config={max_lag_prop_name: 10000})
            laggy_stream = self.start_unstructured_topic_stream(
                dl,
                laggy_topic,
                partitions=200,
                topic_config={max_lag_prop_name: 30000})

            spark = dl.spark()

            def max_offsets_by_partition(topic):
                max_offsets_query =  \
                  "select redpanda.partition, count(*) " \
                  f"from redpanda.{topic} " \
                  "group by redpanda.partition " \
                  "order by redpanda.partition"
                return dict(spark.run_query_fetch_all(max_offsets_query))

            def all_partitions_translated(topic, partitions, count):
                max_offsets = max_offsets_by_partition(topic)
                self.redpanda.logger.debug(f"Current translated offsets for {topic}, has {len(max_offsets)}: {max_offsets}")
                for p in range(partitions):
                    if p not in max_offsets:
                        self.redpanda.logger.debug(f"Missing {topic}/{p}")
                        return False
                    o = max_offsets[p]
                    if o < count:
                        self.redpanda.logger.debug(f"{topic}/{p} offset {o} < {count}")
                        return False
                return True

            # time.sleep(30)
            # s3_port = self.si_settings.cloud_storage_api_endpoint_port
            # with firewall_blocked(self.redpanda.nodes, s3_port):
            #     time.sleep(10)

            # time.sleep(30)

            # with firewall_blocked(self.redpanda.nodes, s3_port):
            #     time.sleep(10)
            time.sleep(80)

            wait_until(
                lambda: all_partitions_translated(realtime_topic, 200, 1),
                timeout_sec=120,
                backoff_sec=1)
            wait_until(lambda: all_partitions_translated(laggy_topic, 200, 1),
                       timeout_sec=120,
                       backoff_sec=1)
