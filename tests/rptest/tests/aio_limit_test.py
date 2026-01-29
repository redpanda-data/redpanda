# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.tests.test import TestContext
from rptest.services.producer_swarm import ProducerSwarm
from rptest.tests.redpanda_test import RedpandaTest

# Test for https://github.com/redpanda-data/seastar/pull/87
# We spawn RP with --max-networking-io-control-blocks=100 and then create 500
# which would make us run into the assert. With the patch we don't crash but just slow down.


class AioLimitTest(RedpandaTest):
    TARGET_IOCBS = 100

    def __init__(self, ctx: TestContext):
        super().__init__(test_context=ctx, num_brokers=1)

    def setUp(self):
        aio_arg = f"--max-networking-io-control-blocks={self.TARGET_IOCBS}"
        self.redpanda.start_node(self.redpanda.nodes[0], extra_cli=[aio_arg])
        self.rpk = RpkTool(self.redpanda)

    def teardown(self):
        super().teardown()

    @cluster(num_nodes=2)
    def test_aio_limit(self):
        topic = "test-topic"
        self.rpk.create_topic(topic, partitions=5, replicas=1)
        producer = ProducerSwarm(
            self.test_context,
            self.redpanda,
            topic=topic,
            producers=self.TARGET_IOCBS * 5,
            records_per_producer=20,
            messages_per_second_per_producer=1,
        )
        producer.start()
        # RP shouldn't crash and still be able to handle the low rate
        producer.wait(timeout_sec=80)
