# Copyright 2020 Redpanda Data, Inc.
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
    def __init__(self,
                 context,
                 redpanda,
                 topic: str,
                 producers: int,
                 records_per_producer: int,
                 log_level="INFO",
                 properties={},
                 timeout_ms: int = 1000):
        super(ProducerSwarm, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._producers = producers
        self._records_per_producer = records_per_producer
        self._stopping = threading.Event()
        self._stopped = False
        self._log_level = log_level
        self._properties = properties
        self._timeout_ms = timeout_ms

    def _worker(self, idx, node):
        cmd = f"RUST_LOG={self._log_level} client-swarm --brokers {self._redpanda.brokers()} producers --topic {self._topic} --count {self._producers} --messages {self._records_per_producer} --timeout-ms {self._timeout_ms}"
        for k, v in self._properties.items():
            cmd += f" --properties {k}={v}"
        try:
            for line in node.account.ssh_capture(cmd):
                self.logger.debug(line.rstrip())

                if self._stopping.is_set():
                    break
        except RemoteCommandError:
            if not self._stopping.is_set():
                raise
        finally:
            self._stopped = True

    def wait_node(self, node, timeout_sec=None):
        if timeout_sec is None:
            timeout_sec = 600
        self._redpanda.wait_until(lambda: self._stopped == True,
                                  timeout_sec=timeout_sec,
                                  backoff_sec=5)
        return super().wait_node(node, timeout_sec=timeout_sec)

    def stop_all(self):
        self._stopping.set()

        self.stop()

    def stop_node(self, node):
        node.account.kill_process("client-swarm", clean_shutdown=False)
