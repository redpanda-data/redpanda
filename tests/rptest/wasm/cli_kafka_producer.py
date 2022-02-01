# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.wasm.background_task import BackgroundTask
from rptest.clients.kafka_cli_tools import KafkaCliTools


class CliKafkaProducer(BackgroundTask):
    def __init__(self, redpanda, topic, num_records, records_size=4192):
        super(CliKafkaProducer, self).__init__()
        self._redpanda = redpanda
        self._topic = topic
        self._num_records = num_records
        self._records_size = records_size

    def task_name(self):
        return f"producer-worker-{str(random.randint(0, 9999))}"

    def _run(self):
        producer = KafkaCliTools(self._redpanda)
        producer.produce(self._topic,
                         self._num_records,
                         self._records_size,
                         acks=1)
