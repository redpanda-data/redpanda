# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import tempfile
import time
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from jinja2 import Template

avro_schema = """
{
  "type": "record",
  "name": "Person",
  "fields" : [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
"""


def stream_config(brokers, topic, schema_registry_url, subject):
    return {
        "input": {
            "generate": {
                "mapping": """root = {"name": "John","age": 30}""",
                "interval": "1s",
                "count": 20,
                "batch_size": 1
            }
        },
        "pipeline": {
            "processors": [{
                "schema_registry_encode": {
                    "url": schema_registry_url,
                    "subject": subject,
                    "refresh_period": "10s"
                }
            }]
        },
        "output": {
            "redpanda": {
                "seed_brokers": brokers,
                "topic": topic,
            }
        }
    }


class RedpandaConnectTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(test_context=test_ctx,
                         *args,
                         **kwargs,
                         num_brokers=3,
                         si_settings=SISettings(test_context=test_ctx),
                         schema_registry_config=SchemaRegistryConfig())

    def _create_schema(self, subject: str, schema: str, schema_type="avro"):
        rpk = RpkTool(self.redpanda)
        with tempfile.NamedTemporaryFile(suffix=f".{schema_type}") as tf:
            tf.write(bytes(schema, 'UTF-8'))
            tf.seek(0)
            rpk.create_schema(subject, tf.name)

    @cluster(num_nodes=4)
    def test_redpanda_connect_producer(self):
        """
        Test verifying if Redpanda Connect can work in ducktape
        """
        topic = TopicSpec(name='connect-test-topic',
                          partition_count=1,
                          replication_factor=3)

        DefaultClient(self.redpanda).create_topic(topic)

        self._create_schema("test", avro_schema)
        # Start redpanda connect
        connect = RedpandaConnectService(self.test_context, self.redpanda)
        connect.start()

        # create a stream
        connect.start_stream(name="ducky_stream",
                             config=stream_config(
                                 self.redpanda.brokers_list(), topic.name,
                                 self.redpanda.schema_reg().split(",")[0],
                                 "test"))

        # wait for the stream to finish
        connect.stop_stream(name="ducky_stream")
        connect.wait()

        # check if the messages are in the topic
        rpk = RpkTool(self.redpanda)
        partitions = rpk.describe_topic(topic.name)

        assert all(
            p.high_watermark > 0 for p in
            partitions), "Error redpanda connect should produce messages"
