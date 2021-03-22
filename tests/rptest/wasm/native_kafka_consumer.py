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
from functools import reduce


class NativeKafkaConsumer(BackgroundTask):
    def __init__(self,
                 brokers,
                 topic_partitions,
                 num_records=1,
                 batch_size=4092):
        super(NativeKafkaConsumer, self).__init__()
        self._topic_partitions = topic_partitions
        self._num_records = num_records
        self._batch_size = batch_size
        self._consumer = KafkaConsumer(client_id=self.task_name(),
                                       bootstrap_servers=brokers,
                                       request_timeout_ms=1000,
                                       enable_auto_commit=False,
                                       auto_offset_reset="earliest")
        self.results = TopicsResultSet()

    def task_name(self):
        return f"consumer-worker-{str(random.randint(0,9999))}"

    def _run(self):
        def stop_consume(empty_iterations):
            read_all = self.results.num_records() >= self._num_records
            waited_enough = empty_iterations <= 0
            return self.is_finished() or read_all or waited_enough

        self._consumer.assign(self._topic_partitions)
        empty_iterations = 10
        total = 0
        while not stop_consume(empty_iterations):
            r = self._consumer.poll(timeout_ms=100,
                                    max_records=self._batch_size)
            if len(r) == 0:
                empty_iterations -= 1
                time.sleep(1)
            else:
                total += reduce(lambda acc, x: acc + len(x), r.values(), 0)
                empty_iterations = 10
                self.results.append(r)
