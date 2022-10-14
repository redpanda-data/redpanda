# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool

from confluent_kafka import (Producer, KafkaException)


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)


class ScramlessEosTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(ScramlessEosTest, self).__init__(test_context=test_context,
                                               extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_idempotent_write_passes(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")

        producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5
        })
        producer.produce("topic1",
                         key="key1".encode('utf-8'),
                         value="value1".encode('utf-8'),
                         callback=on_delivery)
        producer.flush()

    @cluster(num_nodes=3)
    def test_tx_init_passes(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")

        producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "transactional.id": "tx-id-1",
            "retries": 5
        })
        producer.init_transactions()
