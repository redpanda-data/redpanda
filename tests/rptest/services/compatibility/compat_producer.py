# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
from ducktape.services.background_thread import BackgroundThreadService
from rptest.util import Scale


class CompatProducer(BackgroundThreadService):
    """
    The background service for producing events.
    """
    def __init__(self, context, redpanda, topic, msg_gen, count=1):
        super(CompatProducer, self).__init__(context, num_nodes=1)
        # The instance of redpanda
        self._redpanda = redpanda

        # The kafka topic
        self._topic = topic

        self._msg_gen = msg_gen

        self._count = count

        self._fin = False

        self._scale = Scale(context)

    def _worker(self, idx, node):
        for i in range(self._count):
            cmd = f"echo -e \"{self._msg_gen()}\" | kcat -P -b {self._redpanda.brokers()} -t {self._topic}"
            result = node.account.ssh_output(cmd, timeout_sec=10).decode()
            self.logger.debug(result)

        self._fin = True

    def ok(self):
        return self._fin

    def stop_node(self, node):
        node.account.kill_process("kcat", clean_shutdown=False)

    def clean_node(self, nodes):
        pass
