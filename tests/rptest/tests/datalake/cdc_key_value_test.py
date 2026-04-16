# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Test for cdc_key_value iceberg mode.

Verifies that records with keys produce equality deletes, and
tombstones (null value) are treated as pure deletes.
"""

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest

TOPIC = "cdc-kv-test"


class CdcKeyValueTest(RedpandaTest):
    """Tests for cdc_key_value iceberg mode."""

    def __init__(self, test_ctx):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            schema_registry_config=SchemaRegistryConfig(),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000,
            },
        )
        self.dl = DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        )

    def setUp(self):
        self.dl.setUp()

    def tearDown(self):
        self.dl.tearDown()
        super().tearDown()

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_cdc_key_value_upsert_delete(self, cloud_storage_type):
        """Insert, update (overwrite key), and delete (tombstone)."""
        self.dl.create_iceberg_enabled_topic(
            TOPIC,
            iceberg_mode="cdc_key_value",
            config={
                TopicSpec.PROPERTY_ICEBERG_PARTITION_SPEC: "(identity(redpanda.key))",
            },
        )

        rpk = RpkTool(self.redpanda)

        # Insert three records.
        rpk.produce(TOPIC, key="k1", msg="alice")
        rpk.produce(TOPIC, key="k2", msg="bob")
        rpk.produce(TOPIC, key="k3", msg="charlie")

        self.dl.wait_for_translation(TOPIC, msg_count=3)

        # Update k1 (produces equality delete for k1 + new data row).
        rpk.produce(TOPIC, key="k1", msg="alicia")
        # Delete k2 via tombstone.
        rpk.produce(TOPIC, key="k2", msg="", tombstone=True)

        spark = self.dl.query_engine(QueryEngineType.SPARK)
        tbl = f"redpanda.{spark.escape_identifier(TOPIC)}"

        def _check():
            try:
                rows = spark.run_query_fetch_all(
                    f"SELECT redpanda.key, value FROM {tbl} ORDER BY redpanda.key"
                )
                self.logger.info(f"Rows: {rows}")
                # k1 updated to 'alicia', k2 deleted, k3 unchanged.
                # Key and value are binary; Spark returns them as bytes.
                expected = [
                    (bytearray(b"k1"), bytearray(b"alicia")),
                    (bytearray(b"k3"), bytearray(b"charlie")),
                ]
                return rows == expected
            except Exception as e:
                self.logger.info(f"Query failed: {e}")
                return False

        wait_until(
            _check,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="CDC final state not reflected in Iceberg",
        )
