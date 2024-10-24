# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import sys
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError
from threading import Event


class LibrdkafkaTestcase(BackgroundThreadService):
    TEST_DIR = "/opt/librdkafka/tests"
    ROOT = "/mnt/librdkafka_tests"
    CONF_FILE = os.path.join(ROOT, "test.conf")
    LOG_FILE = os.path.join(ROOT, "rdkafka_test.log")
    KAFKA_VERSION = "3.0.0"
    KAFKA_PATH = f"/opt/kafka-{KAFKA_VERSION}"

    logs = {
        "test_case_output": {
            "path": LOG_FILE,
            "collect_default": False
        },
    }

    def __init__(self, context, redpanda, test_case_number):
        super(LibrdkafkaTestcase, self).__init__(context, num_nodes=1)
        self.redpanda = redpanda

        self.test_case_number = test_case_number
        self.error = None

    def _worker(self, _idx, node):
        node.account.ssh("mkdir -p %s" % LibrdkafkaTestcase.ROOT,
                         allow_fail=False)
        # configure test cases
        node.account.create_file(
            LibrdkafkaTestcase.CONF_FILE,
            f"bootstrap.servers={self.redpanda.brokers()}")
        # compile test case
        self.logger.info(f"running librdkafka test {self.test_case_number}")

        # environment to run the tests with
        env = {
            "TEST_KAFKA_VERSION": LibrdkafkaTestcase.KAFKA_VERSION,
            "TESTS": f"{self.test_case_number:04}",
            "RDKAFKA_TEST_CONF": LibrdkafkaTestcase.CONF_FILE,
            "KAFKA_VERSION": LibrdkafkaTestcase.KAFKA_VERSION,
            "KAFKA_PATH": LibrdkafkaTestcase.KAFKA_PATH,
            "BROKERS": self.redpanda.brokers(),
        }
        env_str = " ".join([f"{k}={v}" for k, v in env.items()])
        try:
            node.account.ssh(
                f"{env_str} {LibrdkafkaTestcase.TEST_DIR}/test-runner > {LibrdkafkaTestcase.LOG_FILE} 2>&1"
            )
        except RemoteCommandError as error:
            self.logger.info(
                f"Librdkafka test case: {self.test_case_number} failed with error: {error}"
            )
            self.error = error

    def stop_node(self, node):
        pass

    def clean_node(self, node):
        node.account.ssh("rm -rf " + self.ROOT, allow_fail=False)
