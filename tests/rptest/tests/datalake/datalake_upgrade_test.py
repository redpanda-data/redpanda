# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
from rptest.services.cluster import cluster

from rptest.services.redpanda import SISettings
from rptest.utils.mode_checks import skip_debug_mode
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix
from rptest.tests.datalake.catalog_service_factory import filesystem_catalog_type


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
        self.min_version_with_lag_support = (25, 1)

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
                              catalog_type=filesystem_catalog_type(),
                              include_query_engines=[query_engine]) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name,
                                            partitions=10,
                                            target_lag_ms=10000)

            def run_workload():
                nonlocal total_count
                count = 100
                dl.produce_to_topic(self.topic_name, 1024, msg_count=count)
                total_count += count
                dl.wait_for_translation(self.topic_name, msg_count=total_count)

            lag_set = self.initial_version >= self.min_version_with_lag_support  # type: ignore
            versions = self.load_version_range(self.initial_version)
            for v in self.upgrade_through_versions(versions_in=versions,
                                                   already_running=True):
                self.logger.info(f"Updated to {v}")
                if not lag_set and v >= self.min_version_with_lag_support:
                    # When upgrading from older versions, unsupported topic properties
                    # are just ignored. Force a cluster config change right after upgrading
                    # to first version with the support
                    self.redpanda.set_cluster_config(
                        {"iceberg_target_lag_ms": 10000})
                    lag_set = True
                run_workload()

            # Run some spot checks to ensure that the data is readable.
            result = dl.spark().run_query_fetch_one(f"""
                                                    SELECT count(*)
                                                    FROM redpanda.{self.topic_name}
                                                    WHERE redpanda.offset < 10
                                                      AND redpanda.partition = 0
                                                    """)
            assert result[0] == 10, f"Expected 10 rows, got {result[0]}"

            result = dl.spark().run_query_fetch_one(f"""
                                                    SELECT count(*)
                                                    FROM redpanda.{self.topic_name}
                                                    WHERE redpanda.timestamp >= '2025-01-01 00:00:00'
                                                    """)
            assert result[
                0] == total_count, f"Expected {total_count} rows, got {result[0]}"

            # Check that all fields are queryable and the structure of the row
            # matches the expected structure.
            with dl.spark().run_query(f"""
                                      SELECT *
                                      FROM redpanda.{self.topic_name}
                                      """) as cursor:
                assert cursor.description == [
                    ('redpanda', 'STRUCT_TYPE', None, None, None, None, True),
                    ('value', 'BINARY_TYPE', None, None, None, None, True)
                ], f"Unexpected cursor description: {cursor.description}"

                rows = cursor.fetchall()
                assert rows

                # We're not checking internal redpanda fields as it is close to
                # impossible with our current client PyHive which returns a string
                # representation of the struct. It also loses some type information
                # and binary data which looks like numbers is represented as numbers.
                # If assert below begin to fail maybe we have changed the client and
                # now it is possible to check the types.
                assert isinstance(rows[0][0],
                                  str), f"Unexpected type {type(rows[0][0])}"
                assert isinstance(rows[0][1],
                                  bytes), f"Unexpected type {type(rows[0][1])}"

            # Check nested fields of redpanda struct. Fetch all rows
            with dl.spark().run_query(f"""
                                    SELECT to_json(redpanda)
                                    FROM redpanda.{self.topic_name}
                                    """) as cursor:
                rows = cursor.fetchall()
                assert rows

                # Fetch all rows to make sure the underlying query engine does
                # not fail internally but it should be enough to check a single
                # row from the result set.
                assert json.loads(rows[0][0]).keys() == {
                    "partition", "offset", "timestamp", "headers", "key"
                }, f"Unexpected JSON keys: {json.loads(rows[0][0]).keys()}"
