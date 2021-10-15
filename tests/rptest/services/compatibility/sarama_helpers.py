# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from .helpers_base import HelperBase, HelperFactoryBase

# The Sarama root directory
TESTS_DIR = os.path.join("/opt", "sarama")


class SaramaInterceptorsHelper(HelperBase):
    """
    The helper class for Sarama's interceptors example
    """
    def __init__(self, redpanda, topic):
        super(SaramaInterceptorsHelper, self).__init__(redpanda)

        # The kafka topic
        self._topic = topic

    # The internal condition to determine if the
    # example is successful. Returns boolean.
    def _condition(self, line):
        return 'SpanContext' in line

    # Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/interceptors")
        cmd = f"interceptors -brokers {self._redpanda.brokers()} -topic {self._topic}"
        return os.path.join(EXAMPLE_DIR, cmd)

    # Return the process name to kill
    def process_to_kill(self):
        return "interceptors"


class SaramaHttpServerHelper(HelperBase):
    """
    The helper class for Sarama's http server example
    """
    def __init__(self, redpanda):
        super(SaramaHttpServerHelper, self).__init__(redpanda)

        # The name of the node assigned to this example
        self._node_name = ""

    # The internal condition to determine if the
    # example is successful. Returns boolean.
    def _condition(self, line):
        return 'Listening for requests' in line

    # Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/http_server")
        cmd = f"http_server -addr {self._node_name}:8080 -brokers {self._redpanda.brokers()}"
        return os.path.join(EXAMPLE_DIR, cmd)

    # Return the process name to kill
    def process_to_kill(self):
        return "http_server"

    # What is the name of the node
    # assigned to this example?
    def node_name(self):
        return self._node_name

    # Set the name of the node assigned to
    # this example.
    def set_node_name(self, node_name):
        self._node_name = node_name


class SaramaConsumerGroupHelper(HelperBase):
    """
    The helper class for Sarama's consumergroup example
    """
    def __init__(self, redpanda, topic, extra_conf):
        super(SaramaConsumerGroupHelper, self).__init__(redpanda)

        # The kafka topic
        self._topic = topic

        self._count = extra_conf.get("count") or 1

    # The internal condition to determine if the
    # example is successful. Returns boolean.
    def _condition(self, line):
        self._count -= 'Message claimed:' in line
        return self._count <= 0

    # Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/consumergroup")
        cmd = f"consumer -brokers=\"{self._redpanda.brokers()}\" -topics=\"{self._topic}\" -group=\"example\""
        return os.path.join(EXAMPLE_DIR, cmd)

    # Return the process name to kill
    def process_to_kill(self):
        return "consumer"


# A factory method to produce the command to run
# Sarama's SASL/SCRAM authentication example.
# Here, we do not create a HelperBase because
# the SASL/SCRAM example runs in the foreground.
def sarama_sasl_scram(redpanda, topic):
    EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/sasl_scram_client")
    creds = redpanda.SUPERUSER_CREDENTIALS
    cmd = f"sasl_scram_client -brokers {redpanda.brokers()} -username {creds[0]} -passwd {creds[1]} -topic {topic} -algorithm sha256"

    return os.path.join(EXAMPLE_DIR, cmd)


class SaramaHelperFactory(HelperFactoryBase):
    """
    The concrete factory for creating Sarama's
    helper classes.
    """
    def __init__(self, func_name, redpanda, topic, extra_conf):
        super(SaramaHelperFactory, self).__init__(func_name, redpanda, topic,
                                                  extra_conf)

    # The factory method for sarama
    def create_sarama_helpers(self):
        if self._func_name == "test_sarama_interceptors":
            return SaramaInterceptorsHelper(self._redpanda, self._topic)
        elif self._func_name == "test_sarama_http_server":
            return SaramaHttpServerHelper(self._redpanda)
        elif self._func_name == "test_sarama_consumergroup":
            return SaramaConsumerGroupHelper(self._redpanda, self._topic,
                                             self._extra_conf)
        else:
            raise RuntimeError("create_helper failed: Invalid function name")
