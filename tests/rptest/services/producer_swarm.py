# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
from math import ceil
from ducktape.tests.test import TestContext
from ducktape.services.service import Service
from typing import Any, Callable, Optional

import requests

from rptest.services.redpanda import AnyRedpandaService


class ProducerSwarm(Service):
    EXE = "client-swarm"
    LOG_PATH = "/opt/remote/var/client-swarm.log"

    # client swarm throttles producer startups to one every 33 ms by default,
    # see client_spawn_wait_ms in client-swarm
    CLIENT_SPAWN_WAIT_MS = 33

    logs = {"repeater_log": {"path": LOG_PATH, "collect_default": True}}

    def __init__(self,
                 context: TestContext,
                 redpanda: AnyRedpandaService,
                 topic: str,
                 producers: int,
                 records_per_producer: int,
                 log_level="DEBUG",
                 properties={},
                 timeout_ms: int = 1000,
                 compression_type: Optional[str] = None,
                 compressible_payload: Optional[bool] = None,
                 min_record_size: Optional[int] = None,
                 max_record_size: Optional[int] = None,
                 keys: Optional[int] = None,
                 unique_topics: Optional[bool] = False,
                 messages_per_second_per_producer: Optional[int] = None):
        super(ProducerSwarm, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._producers = producers
        self._records_per_producer = records_per_producer
        self._messages_per_second_per_producer = messages_per_second_per_producer
        self._log_level = log_level
        self._properties = properties
        self._timeout_ms = timeout_ms
        self._compression_type = compression_type
        self._compressible_payload = compressible_payload
        self._min_record_size = min_record_size
        self._max_record_size = max_record_size
        self._keys = keys
        self._unique_topics = unique_topics
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
        assert self._node is None or self._node == node, f'started on more than one node? {self._node} {node}'
        self._node = node

        cmd = f"{self.EXE}"
        cmd += f" --brokers {self._redpanda.brokers()}"
        cmd += f" --metrics-address {self._remote_addr}"
        cmd += f" producers --topic {self._topic}"
        cmd += f" --count {self._producers}"
        cmd += f" --messages {self._records_per_producer}"
        cmd += f" --timeout-ms {self._timeout_ms}"
        for k, v in self._properties.items():
            cmd += f" --properties {k}={v}"

        if self._compressible_payload:
            cmd += f" --compressible-payload"

        if self._compression_type is not None:
            cmd += f" --compression-type={self._compression_type}"

        if self._min_record_size is not None:
            cmd += f" --min-record-size={self._min_record_size}"

        if self._max_record_size is not None:
            cmd += f" --max-record-size={self._max_record_size}"

        if self._keys is not None:
            cmd += f" --keys={self._keys}"
        else:
            # by default use a very large key space so all partitions are written to
            cmd += " --keys=18446744073709551557"

        if self._unique_topics:
            cmd += " --unique-topics"

        if self._messages_per_second_per_producer is not None:
            cmd += f" --messages-per-second {self._messages_per_second_per_producer}"

        cmd += f" --client-spawn-wait-ms={self.CLIENT_SPAWN_WAIT_MS}"

        cmd = f"RUST_LOG={self._log_level} bash /opt/remote/control/start.sh {self.EXE} \"{cmd}\""
        node.account.ssh(cmd)
        self._redpanda.wait_until(
            self.is_alive,
            timeout_sec=600,
            backoff_sec=1,
            err_msg=
            f"producer_swarm service {node.account.hostname} failed to start within {600} sec",
        )
        self._redpanda.wait_until(
            lambda: self.is_metrics_available(node),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=
            f"producer_swarm metrics endpoint at {self._remote_url(node, 'metrics/summary')} failed to answer after {30} sec",
        )

    def wait_for_all_started(self):
        """Wait until the requested number of producers have started. Note that if the expected
        swarm runtime (messages / rate) is short, this may fail as all producers and start and
        finish before we are able to see this via the metrics endpoint and an exception is thrown."""

        # Calculate the theoretical start time based on the spawn rate and then
        # use 3x that + 30 seconds as the timeout. In addition to the sources of random
        # variation, producer swarm is subject to additional variance:
        #
        #   - The CLIENT_SPAWN_WAIT_MS is a sleep time in between spawns, not the
        # actual inter-spawn time, since the spawn itself takes non-zero time and also the
        # sleep may take longer that specified. So the actual inter-spawn times are longer
        # than the configured value and under heavy load may be substantially longer.
        #
        #   - A client is not considered started util is successfully sends a message to
        # the cluster. Since we have often just created the topic, this could be delayed
        # due to leadership transfers which occur in a burst some time between 0 and 5
        # minutes after the topic is created.
        timeout_s = 30 + 3 * ceil(
            self._producers * self.CLIENT_SPAWN_WAIT_MS / 1000)

        def started_count():
            started = self.get_metrics_summary().clients_started
            self.logger.debug(f'{started} producers started so far')
            return started

        self.logger.info(
            f'Waiting up to {timeout_s}s for all {self._producers} to start')

        self._redpanda.wait_until(
            lambda: started_count() == self._producers,
            timeout_sec=timeout_s,
            backoff_sec=1,
            err_msg=lambda: f"{self} did not start after {timeout_s}: "
            f"{started_count()} started, expected {self._producers}")

    def is_metrics_available(self, node):
        path = f"metrics/summary"
        path = f"{path}?seconds=1"
        try:
            self._get(node, path)
            return True
        except:
            return False

    def is_alive(self) -> bool:
        result = self._node.account.ssh_output(
            f"bash /opt/remote/control/alive.sh {self.EXE}")
        result = result.decode("utf-8")
        return "YES" in result

    def wait_node(self, node, timeout_sec=600) -> bool:
        try:
            self._redpanda.wait_until(lambda: not self.is_alive(),
                                      timeout_sec=timeout_sec,
                                      backoff_sec=5)
        except TimeoutError:
            return False
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
        """The p0 (minimum value) for single-interval message rate (msg/s)."""
        p50: int
        """The p50 (median value) for single-interval message rate (msg/s)."""
        p100: int
        """The p100 (maximum value) for single-interval message rate (msg/s)."""
        total_success: int
        """Number of messages delivered successfully during the entire lifetime of the swarm process."""
        total_error: int
        """Number of failed deliveries during the entire lifetime of the swarm process."""
        clients_started: int
        """The number of clients that have ever been started (includes ones that have subsequently stopped).
        To get the number of _currently_ active clients, use clients_alive."""
        clients_stopped: int
        """The number of clients that have stopped as they reached their target message count."""
        @property
        def clients_alive(self):
            """The number of clients running as of this snapshot."""
            return self.clients_started - self.clients_stopped

        @property
        def total_attempts(self):
            """The total number of messages we attempted to send, whether successful or not."""
            return self.total_success + self.total_error

    def get_metrics_summary(self,
                            seconds: int | None = None) -> MetricsSummary:
        path = f"metrics/summary"
        if seconds:
            path = f"{path}?seconds={seconds}"

        res = self._get(self._node, path)

        def i(name: str, input: dict[str, Any] = res, type: Any = int):
            """Get the value with the given key, casted to type"""
            return type(input[name])

        # response looks like:
        # {'min': 0, 'max': 10, 'median': 0, 'counts_from_start': {'success_count': 1078, 'error_count': 0}, 'clients_started': 10, 'clients_stopped': 0}
        cfs = res['counts_from_start']
        return self.MetricsSummary(i("min", type=float), i("median",
                                                           type=float),
                                   i("max", type=float),
                                   i('success_count', cfs),
                                   i('error_count', cfs), i('clients_started'),
                                   i('clients_stopped'))

    def await_progress(self, target_msg_rate, timeout_sec, err_msg=None):
        def check():
            return self.get_metrics_summary(seconds=20).p50 >= target_msg_rate

        self._redpanda.wait_until(check,
                                  timeout_sec=timeout_sec,
                                  backoff_sec=1,
                                  err_msg=err_msg)
