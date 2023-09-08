# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from kafka import TopicPartition
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.wasm.wasm_build_tool import WasmTemplateRepository
from rptest.wasm.wasm_test import WasmTest
from rptest.wasm.wasm_script import WasmScript
from rptest.wasm.topic import construct_materialized_topic
from rptest.wasm.native_kafka_consumer import NativeKafkaConsumer
from ducktape.utils.util import wait_until


class WasmFilterTest(WasmTest):
    topics = (TopicSpec(partition_count=3, ), )

    def __init__(self, test_context, extra_rp_conf=None):
        super(WasmFilterTest, self).__init__(test_context,
                                             extra_rp_conf=extra_rp_conf or {})
        self._num_records = 32
        self._expected_record_cnt = self._num_records / 2
        self._output_topic = "default_output"
        self._script = WasmScript(
            inputs=[x.name for x in self.topics],
            outputs=[self._output_topic],
            script=WasmTemplateRepository.FILTER_TRANSFORM,
        )

    def push_test_data_to_inputs(self, topic, n_partitions, amt):
        for i in range(n_partitions):
            for j in range(amt):
                self._rpk_tool.produce(topic, str(j), str(j), [], partition=i)

    @cluster(num_nodes=3)
    def verify_filter_test(self):
        # 1. Fill source topics with test data
        num_partitions = self.topics[0].partition_count
        self.push_test_data_to_inputs(self.topic, num_partitions,
                                      self._num_records)

        # 2. Start coprocessor
        self._build_script(self._script)

        # 3. Drain from output topics within timeout
        materialized_topic = construct_materialized_topic(
            self.topic, self._output_topic)
        output_tps = [
            TopicPartition(materialized_topic, i)
            for i in range(num_partitions)
        ]
        expected_total = self._expected_record_cnt * num_partitions
        topic_spec = {self.topic: expected_total}
        consumer = NativeKafkaConsumer(self.redpanda.brokers(), output_tps,
                                       topic_spec)

        # Wait until materialized topic is up
        def topic_created():
            topics = self._rpk_tool.list_topics()
            return materialized_topic in list(topics)

        wait_until(topic_created, timeout_sec=10, backoff_sec=1)

        # Consume from materialized topic
        def finished():
            self.logger.info("Recs read: %s" % consumer.results.num_records())
            return consumer.is_finished() or consumer.results.num_records(
            ) >= expected_total

        consumer.start()
        wait_until(finished, timeout_sec=10, backoff_sec=1)
        consumer.join()

        # Assert success
        assert consumer.results.num_records() == expected_total
