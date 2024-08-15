# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
import time


class RetryableConsumer:
    def __init__(self, logger, brokers):
        self.logger = logger
        self.brokers = brokers
        self.consumer = None

    def init(self, topic, retries=1, timeout=10):
        config = {
            "bootstrap.servers": self.brokers,
            "enable.auto.commit": False,
            "group.id": "group1",
            "topic.metadata.refresh.interval.ms": 5000,  # default: 300000
            "metadata.max.age.ms": 10000,  # default: 900000
            "topic.metadata.refresh.fast.interval.ms": 250,  # default: 250
            "topic.metadata.propagation.max.ms": 10000,  # default: 30000
            "socket.timeout.ms": 10000,  # default: 60000
            "connections.max.idle.ms": 0,  # default: 0
            "reconnect.backoff.ms": 100,  # default: 100
            "reconnect.backoff.max.ms": 10000,  # default: 10000
            "statistics.interval.ms": 0,  # default: 0
            "api.version.request.timeout.ms": 10000,  # default: 10000
            "api.version.fallback.ms": 0,  # default: 0
            "fetch.wait.max.ms": 500,  # default: 0
            "isolation.level": "read_committed"
        }

        while True:
            if retries == 0:
                raise Exception("Can't connect to the redpanda cluster")
            retries -= 1
            if self.consumer != None:
                try:
                    self.consumer.close()
                except:
                    pass
            self.logger.debug(
                f"Attempting to init a consumer using {self.brokers}")
            self.consumer = Consumer(config)
            self.consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
            msgs = self.consumer.consume(timeout=timeout)
            for msg in msgs:
                if msg is None:
                    continue
                if msg.error():
                    self.logger.debug("Consumer error: {}".format(msg.error()))
                    continue
                self.logger.debug(
                    "Consumer is initialized, rewinding to the begining")
                self.consumer.seek(TopicPartition(topic, 0, OFFSET_BEGINNING))
                return
            time.sleep(timeout)

    def consume(self, timeout=10):
        return self.consumer.consume(timeout=timeout)

    def close(self):
        self.consumer.close()
