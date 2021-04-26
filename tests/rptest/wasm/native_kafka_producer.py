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
from kafka.errors import KafkaTimeoutError


class NativeKafkaProducer(BackgroundTask):
    def __init__(self,
                 brokers,
                 topic,
                 num_records,
                 max_outstanding_futures=100,
                 records_size=4192):
        super(NativeKafkaProducer, self).__init__()
        self._topic = topic
        self._brokers = brokers
        self._num_records = num_records
        self._records_size = records_size
        self._max_outstanding_futures = max_outstanding_futures

    def task_name(self):
        return f"producer-worker-{str(random.randint(0, 9999))}"

    def _run(self):
        producer = KafkaProducer(bootstrap_servers=self._brokers,
                                 acks='all',
                                 retries=10,
                                 key_serializer=str.encode)

        outstanding_futures = []
        i = 0
        while i < self._num_records:
            if self.is_finished():
                break
            value = os.urandom(self._records_size)
            f = producer.send(self._topic, key=str(i), value=value)
            i += 1
            outstanding_futures.append(f)
            threshold_reached = len(
                outstanding_futures) >= self._max_outstanding_futures
            final_enumeration = (i +
                                 len(outstanding_futures)) >= self._num_records
            if threshold_reached or final_enumeration:
                try:
                    producer.flush(timeout=20)
                    exceptions = [
                        x.exception for x in outstanding_futures
                        if x.exception is not None
                    ]
                    n_errors = len(exceptions)
                    if n_errors > 0:
                        raise Exception((exceptions[0], n_errors))
                except Exception as e:
                    exc, n_errors = e.args[0]
                    i -= n_errors
                except KafkaTimeoutError:
                    i -= len(outstanding_futures)
                finally:
                    outstanding_futures = []

        assert (len(outstanding_futures) == 0)
