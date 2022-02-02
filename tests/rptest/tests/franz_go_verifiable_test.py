# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

import time
import random

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer


class FranzGoVerifiableTest(RedpandaTest):
    MSG_SIZE = 120000

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    def __init__(self, ctx):
        super(FranzGoVerifiableTest, self).__init__(test_context=ctx,
                                                    num_brokers=3)

        self._node_for_franz_go = ctx.cluster.alloc(
            ClusterSpec.simple_linux(1))

        self._producer = FranzGoVerifiableProducer(
            ctx, self.redpanda, self.topic, FranzGoVerifiableTest.MSG_SIZE,
            100000, self._node_for_franz_go)
        self._seq_consumer = FranzGoVerifiableSeqConsumer(
            ctx, self.redpanda, self.topic, FranzGoVerifiableTest.MSG_SIZE,
            self._node_for_franz_go)
        self._rand_consumer = FranzGoVerifiableRandomConsumer(
            ctx, self.redpanda, self.topic, FranzGoVerifiableTest.MSG_SIZE,
            1000, 20, self._node_for_franz_go)

    # In the future producer will signal about json creation
    def _create_json_file(self):
        small_producer = FranzGoVerifiableProducer(
            self.test_context, self.redpanda, self.topic,
            FranzGoVerifiableTest.MSG_SIZE, 1000, self._node_for_franz_go)
        small_producer.start()
        small_producer.wait()

    @cluster(num_nodes=4)
    def test_with_all_type_of_loads(self):
        # Need create json file for consumer at first
        self._create_json_file()

        self._producer.start()
        self._seq_consumer.start()
        self._rand_consumer.start()

        self._producer.wait()
