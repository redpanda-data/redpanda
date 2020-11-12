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


class KafProducer(BackgroundThreadService):
    def __init__(self, context, redpanda, topic, num_records=sys.maxsize):
        super(KafProducer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._num_records = num_records

    def _worker(self, idx, node):
        for i in range(self._num_records):
            cmd = "echo record-%08d | kaf produce -b %s --key key-%08d %s" % (
                i, self._redpanda.brokers(), i, self._topic)
            for line in node.account.ssh_capture(cmd, timeout_sec=10):
                self.logger.debug(line.rstrip())

    def stop_node(self, node):
        node.account.kill_process("kaf", clean_shutdown=False)
