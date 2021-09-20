# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
from ducktape.services.background_thread import BackgroundThreadService


class CompatProducer(BackgroundThreadService):
    """
    The background service for producing a single event.
    Currently, this service is used only for Sarama's
    consumer group example.
    """
    def __init__(self, context, redpanda, topic):
        super(CompatProducer, self).__init__(context, num_nodes=1)
        # The instance of redpanda
        self._redpanda = redpanda

        # The kafka topic
        self._topic = topic

    def _worker(self, idx, node):
        cmd = f"echo \"redpanda is fast\" | kafkacat -b {self._redpanda.brokers()} -t {self._topic}"
        node.account.ssh_output(cmd, timeout_sec=10)

    def stop_node(self, node):
        node.account.kill_process("kafkacat", clean_shutdown=False)

    def clean_node(self, nodes):
        pass
