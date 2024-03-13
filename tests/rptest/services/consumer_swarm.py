# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
from ducktape.tests.test import TestContext
from ducktape.services.service import Service
from typing import Optional

import time
import requests


class ConsumerSwarm(Service):
    EXE = "client-swarm"
    LOG_PATH = "/opt/remote/var/client-swarm.log"

    logs = {"repeater_log": {"path": LOG_PATH, "collect_default": True}}

    def __init__(self,
                 context: TestContext,
                 redpanda,
                 topic: str,
                 group: str,
                 consumers: int,
                 records_per_consumer: int,
                 log_level="DEBUG",
                 properties={},
                 unique_topics: Optional[bool] = False,
                 static_prefix: Optional[bool] = False,
                 unique_groups=False):
        super(ConsumerSwarm, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._group = group
        self._consumers = consumers
        self._records_per_consumer = records_per_consumer
        self._log_level = log_level
        self._properties = properties
        self._unique_topics = unique_topics
        self._unique_groups = unique_groups
        self._static_prefix = static_prefix
        self._node = None
        self._remote_port = 8080
        self._remote_addr = "0.0.0.0"

        if hasattr(redpanda, 'GLOBAL_CLOUD_CLUSTER_CONFIG'):
            security_config = redpanda.security_config()
            properties['security.protocol'] = 'SASL_SSL'
            properties['sasl.mechanism'] = security_config.get(
                'sasl_mechanism', 'PLAIN')
            properties['sasl.username'] = security_config.get(
                'sasl_plain_username', None)
            properties['sasl.password'] = security_config.get(
                'sasl_plain_password', None)

    def clean_node(self, node):
        self._redpanda.logger.debug(f"{self.__class__.__name__}.clean_node")
        self._node = None
        node.account.kill_process(self.EXE, clean_shutdown=False)
        if node.account.exists(self.LOG_PATH):
            node.account.remove(self.LOG_PATH)

    def start_node(self, node, clean=None):
        cmd = f"{self.EXE}"
        cmd += f" --brokers {self._redpanda.brokers()}"
        cmd += f" --metrics-address {self._remote_addr}"
        cmd += f" consumers --topic {self._topic}"
        cmd += f" --group {self._group}"
        cmd += f" --count {self._consumers}"
        cmd += f" --messages {self._records_per_consumer}"
        for k, v in self._properties.items():
            cmd += f" --properties {k}={v}"

        if self._unique_topics:
            cmd += " --unique-topics"

        if self._unique_groups:
            cmd += " --unique-groups"

        cmd = f"RUST_LOG={self._log_level} bash /opt/remote/control/start.sh {self.EXE} \"{cmd}\""
        node.account.ssh(cmd)
        self._redpanda.wait_until(
            lambda: self.is_alive(node),
            timeout_sec=600,
            backoff_sec=1,
            err_msg=
            f"producer_swarm service {node.account.hostname} failed to start within {600} sec",
        )

        self._node = node

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

    def _remote_url(self, node, path) -> str:
        return f"http://{node.account.hostname}:{self._remote_port}/{path}"

    def _get(self, node, rest_handle):
        """
            Perform a GET to client_swarm's metrics API.
        """
        url = self._remote_url(node, rest_handle)
        try:
            r = requests.get(url, timeout=10)
        except Exception as e:
            raise RuntimeError(f"Failed to get data from '{url}'") from e
        return r.json()

    @dataclass
    class MetricsSummary:
        p0: int
        p50: int
        p100: int

    def get_metrics_summary(self, seconds=None) -> MetricsSummary:
        path = f"metrics/summary"
        if seconds:
            path = f"{path}?seconds={seconds}"

        res = self._get(self._node, path)

        return self.MetricsSummary(int(res["min"]), int(res["median"]),
                                   int(res["max"]))

    def await_progress(self, target_msg_rate, timeout_sec, err_msg=None):
        def check():
            return self.get_metrics_summary(seconds=20).p50 >= target_msg_rate

        self._redpanda.wait_until(check,
                                  timeout_sec=timeout_sec,
                                  backoff_sec=1,
                                  err_msg=err_msg)
