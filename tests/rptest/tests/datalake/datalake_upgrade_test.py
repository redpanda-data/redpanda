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
from rptest.utils.mode_checks import skip_debug_mode
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix


class DatalakeUpgradeTest(RedpandaTest):
    def __init__(self, test_context):
        super(DatalakeUpgradeTest,
              self).__init__(test_context,
                             num_brokers=3,
                             si_settings=SISettings(test_context=test_context),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 5000
                             })
        self.test_ctx = test_context
        self.topic_name = "upgrade_topic"

        # Initial version that supported Iceberg.
        self.initial_version = (24, 3)

    def setUp(self):
        self.redpanda._installer.install(self.redpanda.nodes,
                                         self.initial_version)

    @cluster(num_nodes=6)
    @skip_debug_mode
    @matrix(cloud_storage_type=supported_storage_types(),
            query_engine=[QueryEngineType.SPARK])
    def test_upload_through_upgrade(self, cloud_storage_type, query_engine):
        """
        Test that Iceberg translation can progress through different versions
        of Redpanda (e.g. ensuring that data format changes or additional
        Iceberg fields don't block progress).
        """
        total_count = 0
        versions = self.load_version_range(self.initial_version)[1:]
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=True,
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)

            def run_workload():
                nonlocal total_count
                count = 100
                dl.produce_to_topic(self.topic_name, 1024, msg_count=count)
                total_count += count
                dl.wait_for_translation(self.topic_name, msg_count=total_count)

            versions = self.load_version_range(self.initial_version)
            for v in self.upgrade_through_versions(versions_in=versions,
                                                   already_running=True):
                self.logger.info(f"Updated to {v}")
                run_workload()
