# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service
from rptest.util import wait_until
import requests
import sys

OUTPUT_LOG = "/opt/remote/var/pausable_idempotent_producer.log"


class IdempotencyClientFailure(Exception):
    pass


class PausableIdempotentProducer(Service):
    logs = {
        "pausable_idempotent_producer_stdout_stderr": {
            "path": OUTPUT_LOG,
            "collect_default": True
        }
    }

    def __init__(self, context, redpanda):
        super(PausableIdempotentProducer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda

    def is_alive(self, node):
        result = node.account.ssh_output(
            "bash /opt/remote/control/alive.sh pausable_idempotent_producer")
        result = result.decode("utf-8")
        return "YES" in result

    def is_ready(self):
        try:
            self.remote_ping()
            return True
        except requests.exceptions.ConnectionError:
            return False

    def raise_on_violation(self, node):
        self.logger.info(
            f"Scanning node {node.account.hostname} log for violations...")

        for line in node.account.ssh_capture(
                f"grep -e Exception {OUTPUT_LOG} || true"):
            raise IdempotencyClientFailure(line)

    def start_node(self, node, timeout_sec=10):
        node.account.ssh(
            f"bash /opt/remote/control/start.sh pausable_idempotent_producer \"java -cp /opt/verifiers/verifiers.jar io.vectorized.idempotency.App\""
        )
        wait_until(
            lambda: self.is_alive(node),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"pausable_idempotent_producer service {node.account.hostname} failed to start within {timeout_sec} sec",
            retry_on_exc=False)
        self._node = node
        wait_until(
            lambda: self.is_ready(),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"pausable_idempotent_producer service {node.account.hostname} failed to become ready within {timeout_sec} sec",
            retry_on_exc=False)

    def stop_node(self, node):
        node.account.ssh(
            "bash /opt/remote/control/stop.sh pausable_idempotent_producer")
        self.raise_on_violation(node)

    def clean_node(self, node):
        pass

    def wait_node(self, node, timeout_sec=sys.maxsize):
        wait_until(
            lambda: not (self.is_alive(node)),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"pausable_idempotent_producer service {node.account.hostname} failed to stop within {timeout_sec} sec",
            retry_on_exc=False)
        return True

    def remote_ping(self):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/ping")
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def start_producer(self, topic, partitions):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/start-producer",
                          json={
                              "brokers": self._redpanda.brokers(),
                              "topic": topic,
                              "partitions": partitions,
                          })
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def get_progress(self):
        ip = self._node.account.hostname
        return requests.get(f"http://{ip}:8080/progress")

    def ensure_progress(self, expected=1000, timeout_sec=10):
        def check_progress():
            r = self.get_progress()
            if r.status_code != 200:
                return False
            output = r.json()
            self._redpanda.logger.debug(f"progress response: {output}")
            return output["num_produced"] >= expected

        wait_until(
            check_progress,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"pausable_idempotent_producer service {self._node.account.hostname} failed to make progress in {timeout_sec} sec",
            retry_on_exc=False)

    def pause_producer(self):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/pause-producer")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def stop_producer(self):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/stop-producer")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )
