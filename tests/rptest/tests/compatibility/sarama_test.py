# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.services.rpk_producer import RpkProducer
from rptest.services.compatibility.example_runner import ExampleRunner
import rptest.services.compatibility.sarama_examples as SaramaExamples
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SecurityConfig
from rptest.clients.types import TopicSpec


class SaramaTest(RedpandaTest):
    """
    Test three of Sarama's examples: topic interceptor, http server, and consumer group.
    All three examples have some piece that runs in the background, so we use a 
    BackgroundThreadService (i.e., ExampleRunner).
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        super(SaramaTest, self).__init__(
            test_context=test_context,
            extra_rp_conf={'auto_create_topics_enabled': True})

        self._ctx = test_context

        # timeout 600s was sufficient for 5k events on local
        # so 1200s should be OK for CI & release
        self._timeout = 30 if self.scale.local else 1200

    @cluster(num_nodes=4)
    def test_sarama_interceptors(self):
        sarama_example = SaramaExamples.SaramaInterceptors(
            self.redpanda, self.topic)
        example = ExampleRunner(self._ctx,
                                sarama_example,
                                timeout_sec=self._timeout)

        # Start the example
        example.start()

        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

    @cluster(num_nodes=4)
    def test_sarama_http_server(self):
        sarama_example = SaramaExamples.SaramaHttpServer(self.redpanda)
        example = ExampleRunner(self._ctx,
                                sarama_example,
                                timeout_sec=self._timeout)

        # Start the example
        example.start()

        # Wait for the server to load
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Get the node the server is on and
        # a ducktape node
        server_name = example.node_name()
        node = random.choice(self.redpanda.nodes)

        # Http get request using curl
        curl = f"curl -SL http://{server_name}:8080/"

        def try_curl():
            result = node.account.ssh_output(curl, timeout_sec=5).decode()
            self.logger.debug(result)
            return "Your data is stored with unique identifier" in result

        # Using wait_until for auto-retry because sometimes
        # redpanda is in the middle of a leadership election when
        # we try to http get.
        wait_until(try_curl,
                   timeout_sec=self._timeout,
                   backoff_sec=5,
                   err_msg="sarama http_server test failed")

    @cluster(num_nodes=5)
    def test_sarama_consumergroup(self):
        count = 10 if self.scale.local else 5000

        sarama_example = SaramaExamples.SaramaConsumerGroup(
            self.redpanda, self.topic, count)
        example = ExampleRunner(self._ctx,
                                sarama_example,
                                timeout_sec=self._timeout)
        producer = RpkProducer(self._ctx,
                               self.redpanda,
                               self.topic,
                               4,
                               count,
                               acks=-1,
                               printable=True)

        def until_partitions():
            storage = self.redpanda.storage()
            return len(list(storage.partitions("kafka", self.topic))) == 3

        # Must wait for the paritions to materialize or else
        # kaf may try to produce during leadership election.
        # This results in a skipped record since kaf doesn't auto-retry.
        wait_until(until_partitions,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Expected partition did not materialize")

        # Run the producer and wait for the worker
        # threads to finish producing
        producer.start()
        producer.wait()

        # Start the example
        example.start()

        # Wait until the example is OK to terminate
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class SaramaScramTest(RedpandaTest):
    """
    Test Sarama's example that uses SASL/SCRAM authentication.
    The example runs in the foreground so there is no need for
    a BackgroundThreadService.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        security = SecurityConfig()
        security.enable_sasl = True
        super(SaramaScramTest, self).__init__(test_context, security=security)

    @cluster(num_nodes=3)
    def test_sarama_sasl_scram(self):
        # Get the SASL SCRAM command and a ducktape node
        cmd = SaramaExamples.sarama_sasl_scram(self.redpanda, self.topic)
        node = random.choice(self.redpanda.nodes)

        def try_cmd():
            # Allow fail because the process exits with
            # non-zero if redpanda is in the middle of a
            # leadership election. Instead we want to
            # retry the cmd.
            result = node.account.ssh_output(cmd,
                                             allow_fail=True,
                                             timeout_sec=10).decode()
            self.logger.debug(result)
            return "wrote message at partition:" in result

        # Using wait_until for auto-retry because sometimes
        # redpanda is in the middle of a leadership election
        wait_until(try_cmd,
                   timeout_sec=60,
                   backoff_sec=5,
                   err_msg="sarama sasl scram test failed")
