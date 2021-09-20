# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.services.compatibility.compat_producer import CompatProducer
from rptest.services.compatibility.compat_example import CompatExample
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec


class SaramaTest(RedpandaTest):
    """
    Test three of Sarama's examples: topic interceptor, http server, and consumer group.
    All three examples have some piece that runs in the background, so we use a 
    BackgroundThreadService (i.e., CompatExample).
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        super(SaramaTest, self).__init__(test_context=test_context)

        # The produce is only for the consumer group example
        self._producer = CompatProducer(test_context, self.redpanda,
                                        self.topic)

        # A representation of the example to be run in the background
        self._example = CompatExample(test_context, self.redpanda, self.topic)

    @cluster(num_nodes=5)
    def test_sarama_interceptors(self):
        # Start the example
        self._example.start()

        # Wait until the example is OK to terminate
        wait_until(lambda: self._example.ok(),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="sarama interceptors test failed")

    @cluster(num_nodes=5)
    def test_sarama_http_server(self):
        # Start the example
        self._example.start()

        # Wait for the server to load
        wait_until(lambda: self._example.ok(),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="sarama http_server failed to load")

        # Get the node the server is on and
        # a ducktape node
        server_name = self._example.node_name()
        n = random.randint(0, len(self.redpanda.nodes))
        node = self.redpanda.get_node(n)

        # Http get request using curl
        curl = f"curl -SL http://{server_name}:8080/"

        def try_curl():
            result = node.account.ssh_output(curl, timeout_sec=5).decode()
            return "Your data is stored with unique identifier" in result

        # Using wait_until for auto-retry because sometimes
        # redpanda is in the middle of a leadership election when
        # we try to http get.
        wait_until(lambda: try_curl(),
                   timeout_sec=60,
                   backoff_sec=5,
                   err_msg="sarama http_server test failed")

    @cluster(num_nodes=5)
    def test_sarama_consumergroup(self):
        # Run the publisher
        self._producer.start()

        # Start the example
        self._example.start()

        # Wait until the example is OK to terminate
        wait_until(lambda: self._example.ok(),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="sarama consumergroup test failed")
