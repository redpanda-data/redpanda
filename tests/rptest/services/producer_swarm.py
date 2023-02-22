# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service


class ProducerSwarm(Service):
    EXE = "client-swarm"
    LOG_PATH = "/opt/remote/var/client-swarm.log"

    logs = {"repeater_log": {"path": LOG_PATH, "collect_default": True}}

    def __init__(self,
                 context,
                 redpanda,
                 topic: str,
                 producers: int,
                 records_per_producer: int,
                 log_level="DEBUG",
                 properties={},
                 timeout_ms: int = 1000):
        super(ProducerSwarm, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._producers = producers
        self._records_per_producer = records_per_producer
        self._log_level = log_level
        self._properties = properties
        self._timeout_ms = timeout_ms

    def clean_node(self, node):
        self.redpanda.logger.debug(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process(self.EXE, clean_shutdown=False)
        if node.account.exists(self.LOG_PATH):
            node.account.remove(self.LOG_PATH)

    def start_node(self, node, clean=None):
        cmd = f"{self.EXE}"
        cmd += f" --brokers {self._redpanda.brokers()}"
        cmd += f" producers --topic {self._topic}"
        cmd += f" --count {self._producers}"
        cmd += f" --messages {self._records_per_producer}"
        cmd += f" --timeout-ms {self._timeout_ms}"
        for k, v in self._properties.items():
            cmd += f" --properties {k}={v}"
        cmd = f"RUST_LOG={self._log_level} bash /opt/remote/control/start.sh {self.EXE} \"{cmd}\""
        node.account.ssh(cmd)
        self._redpanda.wait_until(
            lambda: self.is_alive(node),
            timeout_sec=600,
            backoff_sec=1,
            err_msg=
            f"producer_swarm service {node.account.hostname} failed to start within {600} sec",
        )

    def is_alive(self, node):
        result = node.account.ssh_output(
            f"bash /opt/remote/control/alive.sh {self.EXE}")
        result = result.decode("utf-8")
        return "YES" in result

    def wait_node(self, node, timeout_sec=600):
        self._redpanda.wait_until(lambda: not self.is_alive(node),
                                  timeout_sec=timeout_sec,
                                  backoff_sec=5)
        return True

    def stop_node(self, node):
        node.account.ssh(f"bash /opt/remote/control/stop.sh {self.EXE}")
