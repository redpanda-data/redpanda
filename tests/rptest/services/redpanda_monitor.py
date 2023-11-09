# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import threading

from rptest.services.redpanda import RedpandaService

from ducktape.services.background_thread import BackgroundThreadService


class RedpandaMonitor(BackgroundThreadService):
    def __init__(self, context, redpanda: RedpandaService):
        super(RedpandaMonitor, self).__init__(context, num_nodes=1)
        self.redpanda = redpanda
        self.stopping = threading.Event()
        self.interval = 2  # seconds

    def _worker(self, idx, node):
        self.stopping.clear()

        self.redpanda.logger.info(
            f"Started RedpandaMonitor on {node.account.hostname}")

        while not self.stopping.is_set():
            started_dead_nodes = []
            for n in self.redpanda.started_nodes():
                try:
                    self.redpanda.logger.info(
                        f"RedpandaMonitor checking {n.account.hostname}")
                    pid = self.redpanda.redpanda_pid(n)
                    if pid is None:
                        started_dead_nodes.append(n)

                except Exception as e:
                    self.redpanda.logger.warn(
                        f"Failed to fetch PID for node {n.account.hostname}: {e}"
                    )

            for n in started_dead_nodes:
                self.redpanda.remove_from_started_nodes(n)

            for n in started_dead_nodes:
                try:
                    self.redpanda.logger.info(
                        f"RedpandaMonitor restarting {n.account.hostname}")
                    self.redpanda.start_node(node=n,
                                             write_config=False,
                                             timeout=60)
                except Exception as e:
                    self.redpanda.logger.error(
                        f"RedpandaMonitor failed to restart {n.account.hostname}: {e}"
                    )

            time.sleep(self.interval)

        self.redpanda.logger.info(
            f"Stopped RedpandaMonitor on {node.account.hostname}")

    def stop_node(self, node):
        self.redpanda.logger.info(
            f"Stopping RedpandaMonitor on {node.account.hostname}")
        self.stopping.set()

    def clean_node(self, nodes):
        pass
