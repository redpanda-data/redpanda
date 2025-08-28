# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.mark import matrix

OOM_ALLOW_LIST = [
    # partitioning_writer.cc:65 - Failed to create new writer: Memory exhausted
    re.compile("Failed to create new writer: Memory exhausted")
]


class DatalakeTranslationInterruptionsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeTranslationInterruptionsTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 1000,
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

    def do_run(self, dl: DatalakeServices, partitions: int, rows: int):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic=self.topic_name,
            partitions=partitions,
            replicas=1,
            config={TopicSpec.PROPERTY_ICEBERG_TARGET_LAG_MS: 10000},
        )
        # Hydrating topic before translation leads to more interesting concurrent
        # translations rather than interleaving with produce
        dl.produce_to_topic(topic=self.topic_name, msg_size=1024, msg_count=rows)
        dl.set_iceberg_mode_on_topic(topic=self.topic_name, mode="key_value")
        dl.wait_for_translation(self.topic_name, msg_count=rows)

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[CatalogType.REST_HADOOP],
    )
    def test_scheduler_time_slice_interruptions(
        self, cloud_storage_type, query_engine, catalog_type
    ):
        """This test verifies the error paths triggered due to scheduler
        time slice violation. Particularly exceptional paths in multiplexer/writers
        triggered by exceptions from abort source"""

        # A low scheduler time slice guarantees that translation is frequently interrupted
        # triggering exceptional paths.

        self.redpanda.add_extra_rp_conf({"datalake_scheduler_time_slice_ms": "1000"})
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            self.do_run(dl, partitions=10, rows=100000)

    @cluster(num_nodes=4, log_allow_list=OOM_ALLOW_LIST)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[CatalogType.REST_HADOOP],
    )
    def test_oom_interruptions(self, cloud_storage_type, query_engine, catalog_type):
        """This test verifies the error paths triggered due to scheduler
        oom interruptions. Particularly exceptional paths in multiplexer/writers
        triggered by exceptions from abort source"""

        # A large block size forces the translators to OOM rather fairly quickly
        # since the total memory allocated per shard to datalake is ~88 MB
        # which only supports one active translator a time.

        self.redpanda.add_extra_rp_conf(
            {
                "datalake_scheduler_block_size_bytes": 64 * 1024 * 1024,
                "datalake_scheduler_max_concurrent_translations": 2,
            }
        )
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            self.do_run(dl, partitions=10, rows=100000)
