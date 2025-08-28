# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.utils import supported_storage_types
from rptest.utils.rpcn_utils import counter_stream_config
from ducktape.mark import matrix

from rptest.utils.data_migrations import DataMigrationTestMixin


class RedpandaConnectIcebergTest(RedpandaTest, DataMigrationTestMixin):
    TOPIC_NAME = "ducky_topic"
    PARTITION_COUNT = 5
    FAST_COMMIT_INTVL_S = 5

    verifier_schema_avro = """
{
    "type": "record",
    "name": "VerifierRecord",
    "fields": [
        {
            "name": "verifier_string",
            "type": "string"
        },
        {
            "name": "ordinal",
            "type": "long"
        },
        {
            "name": "timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        }
    ]
}
    """

    def __init__(self, test_context):
        self._topic = None
        super(RedpandaConnectIcebergTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(
                test_context,
                cloud_storage_enable_remote_read=False,
                cloud_storage_enable_remote_write=False,
            ),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": self.FAST_COMMIT_INTVL_S * 1000,
            },
            schema_registry_config=SchemaRegistryConfig(),
        )
        self.dl = DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        )

    def setUp(self):
        self.dl.setUp()
        self.dl.create_iceberg_enabled_topic(
            self.TOPIC_NAME,
            partitions=self.PARTITION_COUNT,
            replicas=3,
            iceberg_mode="value_schema_id_prefix",
        )
        rpk = RpkTool(self.redpanda)
        rpk.create_schema_from_str("verifier_schema", self.verifier_schema_avro)

    def tearDown(self):
        self.dl.tearDown()

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translating_avro_serialized_records(self, cloud_storage_type):
        connect = RedpandaConnectService(self.test_context, self.redpanda)
        connect.start()
        verifier = DatalakeVerifier(self.redpanda, self.TOPIC_NAME, self.dl.spark())
        mapping = dict(
            ordinal="this",
            timestamp="timestamp_unix_milli()",
            verifier_string="uuid_v4()",
        )
        avro_stream_config = counter_stream_config(
            self.redpanda, self.TOPIC_NAME, "verifier_schema", mapping, 3000
        )
        connect.start_stream(name="ducky_stream", config=avro_stream_config)
        verifier.start()
        connect.stop_stream("ducky_stream")
        verifier.wait()
