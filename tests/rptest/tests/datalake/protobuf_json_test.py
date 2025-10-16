# Copyright 2025 Redpanda Data, Inc.
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
from typing import Any, cast

from ducktape.mark import matrix
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class ProtobufJsonTest(RedpandaTest):
    """
    Test translation of google.protobuf.Struct, Value, and ListValue to
    JSON strings in Iceberg.
    """

    TOPIC_NAME = "proto_json"

    def __init__(self, test_context):
        super(ProtobufJsonTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(
                test_context,
                cloud_storage_enable_remote_read=False,
                cloud_storage_enable_remote_write=False,
            ),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 1000,
            },
            schema_registry_config=SchemaRegistryConfig(),
        )

    def setUp(self):
        # Redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=5)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
    )
    def test_protobuf_struct_value_listvalue_to_json(
        self, cloud_storage_type, query_engine
    ):
        """
        Test that google.protobuf.Struct, Value, and ListValue fields are correctly
        translated to JSON strings in Iceberg tables.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.TOPIC_NAME,
                partitions=1,
                replicas=3,
                iceberg_mode="value_schema_id_prefix",
            )

            proto_schema = """
syntax = "proto3";

import "google/protobuf/struct.proto";

message TestMessage {
  google.protobuf.Struct struct_field = 1;
  google.protobuf.Value value_field = 2;
  google.protobuf.ListValue list_field = 3;
}
"""

            # Create schema in the schema registry.
            rpk = RpkTool(self.redpanda)
            with tempfile.NamedTemporaryFile(
                suffix=".proto", mode="w", delete=False
            ) as tf:
                tf.write(proto_schema)
                tf.flush()
                schema_file = tf.name
            try:
                subject = f"{self.TOPIC_NAME}-value"
                result = cast(dict[str, Any], rpk.create_schema(subject, schema_file))
                schema_id: int = int(result["id"])
                self.logger.info(f"Created schema with ID: {schema_id}")
            finally:
                os.unlink(schema_file)

            # The values are expected to round-trip through Redpanda and Iceberg.
            test_cases = [
                {
                    "name": "simple_struct",
                    "struct_field": {"name": "Alice", "age": 30, "active": True},
                    "value_field": "simple string",
                    "list_field": [1, 2, 3],
                },
                {
                    "name": "nested_struct",
                    "struct_field": {
                        "user": {"name": "Bob", "details": {"age": 25, "city": "NYC"}}
                    },
                    "value_field": 42,
                    "list_field": ["a", "b", "c"],
                },
                {
                    "name": "null_values",
                    "struct_field": {},
                    "value_field": None,
                    "list_field": [],
                },
            ]

            for tc in test_cases:
                message = {
                    "struct_field": tc["struct_field"],
                    "value_field": tc["value_field"],
                    "list_field": tc["list_field"],
                }
                msg_json = json.dumps(message)
                rpk.produce(
                    self.TOPIC_NAME,
                    key="",
                    msg=msg_json,
                    schema_id=schema_id,
                    proto_msg="TestMessage",
                )
                self.logger.info(f"Produced message for test case: {tc['name']}")

            dl.wait_for_translation(self.TOPIC_NAME, msg_count=len(test_cases))

            results = dl.spark().run_query_fetch_all(
                f"SELECT struct_field, value_field, list_field FROM redpanda.{self.TOPIC_NAME} ORDER BY redpanda.offset"
            )

            assert len(results) == len(test_cases), (
                f"Expected {len(test_cases)} rows, got {len(results)}"
            )

            for _, (tc, row) in enumerate(zip(test_cases, results)):
                struct_json, value_json, list_json = row

                self.logger.info(f"Verifying test case: {tc['name']}")
                self.logger.info(f"  struct_field: {struct_json}")
                self.logger.info(f"  value_field: {value_json}")
                self.logger.info(f"  list_field: {list_json}")

                actual_struct = json.loads(struct_json) if struct_json else None
                actual_value = json.loads(value_json) if value_json else None
                actual_list = json.loads(list_json) if list_json else None

                assert actual_struct == tc["struct_field"], (
                    f"Test case '{tc['name']}': struct_field mismatch. "
                    f"Expected: {tc['struct_field']}, Got: {actual_struct}"
                )
                assert actual_value == tc["value_field"], (
                    f"Test case '{tc['name']}': value_field mismatch. "
                    f"Expected: {tc['value_field']}, Got: {actual_value}"
                )
                assert actual_list == tc["list_field"], (
                    f"Test case '{tc['name']}': list_field mismatch. "
                    f"Expected: {tc['list_field']}, Got: {actual_list}"
                )

            self.logger.info("All test cases passed!")
