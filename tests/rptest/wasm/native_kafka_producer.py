# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import random

from rptest.wasm.background_task import BackgroundTask
from kafka import KafkaProducer


class NativeKafkaProducer(BackgroundTask):
    def __init__(self, brokers, topic, num_records, records_size=4192):
        super(NativeKafkaProducer, self).__init__()
        self._topic = topic
        self._num_records = num_records
        self._records_size = records_size
        self._producer = KafkaProducer(bootstrap_servers=brokers,
                                       linger_ms=5,
                                       acks=1,
                                       retries=10,
                                       key_serializer=str.encode)

    def task_name(self):
        return f"producer-worker-{str(random.randint(0, 9999))}"

    def _run(self):
        for i in range(0, self._num_records):
            if self.is_finished():
                break
            key = str(hash(str(i) + self._topic))
            value = os.urandom(self._records_size)
            self._producer.send(self._topic, key=key, value=value)
        self._producer.flush()
