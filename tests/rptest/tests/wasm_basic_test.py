# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import uuid
import time

from ducktape.mark.resource import cluster

from kafka import KafkaConsumer, TopicPartition

from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.wasm.topics_result_set import TopicsResultSet
from rptest.wasm.topic import construct_materialized_topic


class WasmBasicTest(RedpandaTest):
    # TODO: Once theres a build system for coprocessors we can randomly
    # generate this topic name and pass it as the input topic to the script.
    # For now this value is hardcoded in the script this test launches
    topics = (TopicSpec(name="myInputTopic",
                        partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            developer_mode=True,
            enable_coproc=True,
        )

        super(WasmBasicTest, self).__init__(test_context=test_context,
                                            extra_rp_conf=extra_rp_conf)

        self.test_file_path = os.path.join(self.resource_dir(),
                                           "wasm-identity-transform.js")

        if not os.path.exists(self.test_file_path):
            raise Exception(
                f"This test expects {self.test_file_path} to be in the resources dir"
            )

        self.rpk_tool = RpkTool(self.redpanda)
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.input_topic = self.topics[0].name
        self.output_topic = construct_materialized_topic(
            self.input_topic, "myOutputTopic")

    def _read_records(self, tps, n):
        results = TopicsResultSet()
        consumer = None
        try:
            consumer = KafkaConsumer(client_id=uuid.uuid4(),
                                     bootstrap_servers=self.redpanda.brokers(),
                                     request_timeout_ms=1000,
                                     enable_auto_commit=False,
                                     auto_offset_reset="earliest")
        except ValueError as e:
            self.redpanda.logger.error(
                f"Failed to create KafkaConsumer: {str(e)})")
            return results

        consumer.assign(tps)
        empty_iterations = 10
        while results.num_records() < n and empty_iterations > 0:
            r = consumer.poll(timeout_ms=100, max_records=1024)
            # If we've looped 'empty_iterations' times without data, exit
            if len(r) == 0:
                empty_iterations -= 1
                time.sleep(1)
            else:
                results.append(r)
        consumer.close()
        return results

    @cluster(num_nodes=3)
    def wasm_basic_test(self):
        # Produce all data
        self.kafka_tools.produce(self.input_topic, 1024, 1024)

        # Deploy coprocessor
        self.rpk_tool.wasm_deploy(self.test_file_path, "ducktape")

        # Read all of the data from the materialized topic, expecting the same
        # number of records which were produced onto the input topic
        output_tps = [
            TopicPartition(self.output_topic, 0),
            TopicPartition(self.output_topic, 1),
            TopicPartition(self.output_topic, 2)
        ]
        input_tps = [
            TopicPartition(self.input_topic, 0),
            TopicPartition(self.input_topic, 1),
            TopicPartition(self.input_topic, 2)
        ]

        # Assert that the records on both topics are identical
        input_results = self._read_records(input_tps, 1024)
        output_results = self._read_records(output_tps, 1024)
        assert input_results.num_records() == 1024
        if input_results != output_results:
            raise Exception(
                "Expected all records across topics to be equivalent")
