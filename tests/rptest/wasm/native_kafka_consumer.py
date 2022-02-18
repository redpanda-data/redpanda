# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random

from kafka import KafkaConsumer
from rptest.wasm.background_task import BackgroundTask
from rptest.wasm.topics_result_set import TopicsResultSet


class NativeKafkaConsumer(BackgroundTask):
    def __init__(self,
                 brokers,
                 topic_partitions,
                 max_records_per_partition,
                 batch_size=4092):
        super(NativeKafkaConsumer, self).__init__()
        self._topic_partitions = topic_partitions
        self._max_records_per_partition = max_records_per_partition
        self._brokers = brokers
        self._batch_size = batch_size
        self._max_attempts = 20
        self.results = TopicsResultSet()

    def task_name(self):
        return f"consumer-worker-{str(random.randint(0,9999))}"

    def total_expected_records(self):
        return sum(self._max_records_per_partition.values())

    def _init_consumer(self):
        consumer = KafkaConsumer(client_id=self.task_name(),
                                 bootstrap_servers=self._brokers,
                                 request_timeout_ms=1000,
                                 enable_auto_commit=False,
                                 auto_offset_reset="latest")
        consumer.assign(self._topic_partitions)
        for tps in self._topic_partitions:
            consumer.seek_to_beginning(tps)
        return consumer

    def _finished_consume(self):
        for topic, throughput in self._max_records_per_partition.items():
            if self.results.num_records_for_topic(topic) < throughput:
                return False
        return True

    def _run(self):
        def stop_consume(empty_attempts):
            waited_enough = empty_attempts >= self._max_attempts
            return self.is_finished() or self._finished_consume(
            ) or waited_enough

        consumer = self._init_consumer()
        empty_reads = 0
        while not stop_consume(empty_reads):
            results = consumer.poll(timeout_ms=1000,
                                    max_records=self._batch_size)
            if results is None or len(results) == 0:
                empty_reads += 1
                time.sleep(1)
                consumer.close()
                consumer = self._init_consumer()
            else:
                empty_reads = 0
                self.results.append(results)
