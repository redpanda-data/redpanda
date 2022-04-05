# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
import threading
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError


class ProducerSwarm(BackgroundThreadService):
    def __init__(self, context, redpanda, topic: str, producers: int,
                 records_per_producer: int):
        super(ProducerSwarm, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._producers = producers
        self._records_per_producer = records_per_producer
        self._stopping = threading.Event()

    def _worker(self, idx, node):
        cmd = f"RUST_LOG=INFO client-swarm producers {self._redpanda.brokers()} {self._topic} {self._producers} {self._records_per_producer}"

        try:
            for line in node.account.ssh_capture(cmd):
                self.logger.debug(line.rstrip())

                if self._stopping.is_set():
                    break
        except RemoteCommandError:
            if not self._stopping.is_set():
                raise

    def stop_all(self):
        self._stopping.set()
        self.stop()

    def stop_node(self, node):
        node.account.kill_process("conn_test", clean_shutdown=False)
