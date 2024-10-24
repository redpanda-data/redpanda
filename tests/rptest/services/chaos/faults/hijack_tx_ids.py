# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import sleep

from confluent_kafka import Producer
from confluent_kafka import KafkaException

from .fault import OneoffFault


class HijackTxIDsFault(OneoffFault):
    def __init__(self, redpanda, ids, logger):
        self.redpanda = redpanda
        self.ids = ids
        self.logger = logger

    @property
    def name(self):
        return "hijack_tx_ids"

    def execute(self):
        for tx_id in self.ids:
            producer_config = {
                "bootstrap.servers": self.redpanda.brokers(),
                "topic.metadata.refresh.interval.ms": 5000,
                "metadata.max.age.ms": 10000,
                "topic.metadata.refresh.fast.interval.ms": 250,
                "topic.metadata.propagation.max.ms": 10000,
                "socket.timeout.ms": 10000,
                "connections.max.idle.ms": 0,
                "reconnect.backoff.ms": 100,
                "reconnect.backoff.max.ms": 10000,
                "statistics.interval.ms": 0,
                "api.version.request.timeout.ms": 10000,
                "api.version.fallback.ms": 0,
                "queue.buffering.max.ms": 0,
                "retry.backoff.ms": 100,
                "sticky.partitioning.linger.ms": 10,
                "message.timeout.ms": 10000,
                "request.required.acks": -1,
                "retries": 5,
                "enable.idempotence": True,
                "transactional.id": tx_id
            }

            attempt = 0
            while True:
                self.logger.debug(f"hijacking {tx_id}")
                try:
                    producer = Producer(producer_config)
                    producer.init_transactions()
                    return
                except KafkaException as e:
                    if attempt > 6:
                        raise e
                attempt += 1
                if attempt % 2 == 0:
                    sleep(1)
