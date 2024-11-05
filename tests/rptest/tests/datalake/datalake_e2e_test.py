# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
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
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"

    @cluster(num_nodes=4)
    @matrix(query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
            filesystem_catalog_mode=[True])
    def test_e2e_basic(self, query_engine, filesystem_catalog_mode):
        # Create a topic
        # Produce some events
        # Ensure they end up in datalake
        count = 100
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name)
            dl.produce_to_topic(self.topic_name, 1024, count)
            dl.wait_for_translation(self.topic_name, msg_count=count)
