# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import os
import tempfile
import time
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix

from rptest.utils.mode_checks import ignore


# Test checking that the verifier can detect Iceberg table anomalies
class DatalakeVerifierTest(RedpandaTest):
    def __init__(self, test_context):
        self._topic = None
        super(DatalakeVerifierTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=SISettings(test_context,
                                   cloud_storage_enable_remote_read=False,
                                   cloud_storage_enable_remote_write=False),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000
            },
            schema_registry_config=SchemaRegistryConfig())

    def simple_stream(self, topic, subject):
        return {
            "input": {
                "generate": {
                    "mapping": "root = counter()",
                    "interval": "",
                    "count": 100,
                    "batch_size": 1
                }
            },
            "pipeline": {
                "processors": []
            },
            "output": {
                "redpanda": {
                    "seed_brokers": self.redpanda.brokers_list(),
                    "topic": topic,
                }
            }
        }

    def setUp(self):
        pass

    def _prepare_test_data(self, topic_name: str, dl: DatalakeServices):

        dl.create_iceberg_enabled_topic(topic_name,
                                        partitions=1,
                                        replicas=1,
                                        iceberg_mode="key_value")

        connect = RedpandaConnectService(self.test_context, self.redpanda)
        connect.start()
        # create a stream
        connect.start_stream(name="ducky_stream",
                             config=self.simple_stream(topic_name,
                                                       "verifier_schema"))
        dl.wait_for_translation(topic_name, 100)
        connect.wait_for_stream_to_finish("ducky_stream")

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_detecting_gap_in_offset_sequence(self, cloud_storage_type):
        topic_name = "ducky_topic"
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[QueryEngineType.TRINO
                                                     ]) as dl:
            self._prepare_test_data(topic_name, dl)
            dl.trino().run_query_fetch_all(
                f"DELETE FROM redpanda.{topic_name} WHERE redpanda.offset=10")
            verifier = DatalakeVerifier(self.redpanda, topic_name, dl.trino())

            verifier.start()
            try:
                verifier.wait()
                assert False, "Verifier should have failed"
            except Exception as e:
                assert "gap in the table" in str(
                    e), f"Error: {e} should contain 'gap in the table'"

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_detecting_duplicates(self, cloud_storage_type):
        topic_name = "ducky_topic"
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[QueryEngineType.TRINO
                                                     ]) as dl:
            self._prepare_test_data(topic_name, dl)

            # Insert duplicate
            dl.trino().run_query_fetch_all(
                f"INSERT INTO redpanda.{topic_name}  VALUES ( ROW(0, 10, TIMESTAMP '2024-11-29 10:00:00' , ARRAY[], CAST('key' as VARBINARY )), CAST('value' AS VARBINARY))"
            )

            verifier = DatalakeVerifier(self.redpanda, topic_name, dl.trino())
            verifier.start()
            try:
                verifier.wait()
                assert False, "Verifier should have failed"
            except Exception as e:
                assert "Duplicate" in str(
                    e), f"Error: {e} should contain 'duplicate'"
