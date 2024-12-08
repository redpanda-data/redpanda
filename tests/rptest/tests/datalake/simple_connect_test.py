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


class RedpandaConnectIcebergTest(RedpandaTest):

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
            si_settings=SISettings(test_context,
                                   cloud_storage_enable_remote_read=False,
                                   cloud_storage_enable_remote_write=False),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000
            },
            schema_registry_config=SchemaRegistryConfig())

    def avro_stream_config(self, topic, subject):
        return {
            "input": {
                "generate": {
                    "mapping": "root = counter()",
                    "interval": "",
                    "count": 3000,
                    "batch_size": 1
                }
            },
            "pipeline": {
                "processors": [{
                    "mapping":
                    """
                            root.ordinal = this
                            root.timestamp = timestamp_unix_milli()
                            root.verifier_string = uuid_v4()
                        """
                }, {
                    "schema_registry_encode": {
                        "url": self.redpanda.schema_reg().split(",")[0],
                        "subject": subject,
                        "refresh_period": "10s"
                    }
                }]
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

    def _create_schema(self, subject: str, schema: str, schema_type="avro"):
        rpk = RpkTool(self.redpanda)
        with tempfile.NamedTemporaryFile(suffix=f".{schema_type}") as tf:
            tf.write(bytes(schema, 'UTF-8'))
            tf.flush()
            rpk.create_schema(subject, tf.name)

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translating_avro_serialized_records(self, cloud_storage_type):
        topic_name = "ducky-topic"
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[
                                  QueryEngineType.SPARK,
                              ]) as dl:

            dl.create_iceberg_enabled_topic(
                topic_name,
                partitions=5,
                replicas=3,
                iceberg_mode="value_schema_id_prefix")

            self._create_schema("verifier_schema", self.verifier_schema_avro)
            connect = RedpandaConnectService(self.test_context, self.redpanda)
            connect.start()

            # create verifier
            verifier = DatalakeVerifier(self.redpanda, topic_name, dl.spark())
            # create a stream
            connect.start_stream(name="ducky_stream",
                                 config=self.avro_stream_config(
                                     topic_name, "verifier_schema"))

            verifier.start()
            connect.stop_stream("ducky_stream")
            verifier.wait()
