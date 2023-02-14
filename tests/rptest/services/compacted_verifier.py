# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service
from rptest.util import wait_until_result
import requests
from time import sleep
import sys
import json
from rptest.util import wait_until
from enum import Enum


class CrushedException(Exception):
    pass


class ConsistencyViolationException(Exception):
    pass


OUTPUT_LOG = "/opt/remote/var/rw.log"


class Workload(str, Enum):
    IDEMPOTENCY = "IDEMPOTENCY"
    TX = "TX"
    TX_UNIQUE_KEYS = "TX_UNIQUE_KEYS"


class CompactedVerifier(Service):
    logs = {"rw_stdout_stderr": {"path": OUTPUT_LOG, "collect_default": True}}

    def __init__(self, context, redpanda, workload):
        super(CompactedVerifier, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._is_done = False
        self._node = None
        if workload not in Workload:
            raise Exception(f"Unknown workload {workload}")
        self._workload = workload
        self._partitions = None

    def is_alive(self, node):
        result = node.account.ssh_output(
            "bash /opt/remote/control/alive.sh rw")
        result = result.decode("utf-8")
        return "YES" in result

    def is_ready(self):
        try:
            self.remote_ping()
            return True
        except requests.exceptions.ConnectionError:
            return False

    def ensure_progress(self, delta, timeout_sec):
        if not self.is_alive(self._node):
            self.raise_on_violation(self._node)
            raise CrushedException(
                "CompactedVerifier is crashed (it often happens on the consistency violation)"
            )

        old_state = self.remote_info()
        min_writes = old_state["min_writes"]

        def check_writes():
            new_state = self.remote_info()
            if new_state["min_writes"] - min_writes < delta:
                return False
            return True

        wait_until(
            check_writes,
            timeout_sec,
            2,
            err_msg=
            f"writes got stuck: hasn't written {delta} records in {timeout_sec}s"
        )

    def has_cleared(self, threshold, timeout_sec):
        if not self.is_alive(self._node):
            self.raise_on_violation(self._node)
            raise CrushedException(
                "CompactedVerifier is crashed (it often happens on the consistency violation)"
            )

        def check():
            new_state = self.remote_info()
            if new_state["min_writes"] < threshold:
                return False
            return True

        wait_until(
            check,
            timeout_sec,
            2,
            err_msg=
            f"num of rw iteration hasn't reached {threshold} in {timeout_sec}")

    def raise_on_violation(self, node):
        self.logger.info(
            f"Scanning node {node.account.hostname} log for violations...")

        for line in node.account.ssh_capture(
                f"grep -e violation {OUTPUT_LOG} || true"):
            raise ConsistencyViolationException(line)

    ### Service overrides

    def start_node(self, node, timeout_sec=10):
        node.account.ssh(
            f"bash /opt/remote/control/start.sh rw \"java -cp /opt/verifiers/verifiers.jar io.vectorized.compaction.App\""
        )
        wait_until(
            lambda: self.is_alive(node),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"rw service {node.account.hostname} failed to start within {timeout_sec} sec",
            retry_on_exc=False)
        self._node = node
        wait_until(
            lambda: self.is_ready(),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"rw service {node.account.hostname} failed to become ready within {timeout_sec} sec",
            retry_on_exc=False)

    def stop_node(self, node):
        node.account.ssh("bash /opt/remote/control/stop.sh rw")
        self.raise_on_violation(node)

    def clean_node(self, node):
        pass

    def wait_node(self, node, timeout_sec=sys.maxsize):
        wait_until(
            lambda: not (self.is_alive(node)),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"rw service {node.account.hostname} failed to stop within {timeout_sec} sec",
            retry_on_exc=False)
        return True

    ### RPCs

    def remote_ping(self):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/ping")
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def remote_start_producer(self,
                              connection,
                              topic,
                              partitions,
                              key_set_cardinality=10):
        self._partitions = partitions
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/start-producer",
                          json={
                              "workload": self._workload.name,
                              "brokers": connection,
                              "topic": topic,
                              "partitions": partitions,
                              "key_set_cardinality": key_set_cardinality
                          })
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def remote_start_consumer(self):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/start-consumer")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def remote_stop_producer(self):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/stop-producer")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def remote_wait_producer(self):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/wait-producer")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def remote_wait_consumer(self, timeout_sec=30):
        def check_consumed():
            info = self.remote_info()
            if len(info["partitions"]) != self._partitions:
                return False
            for partition in info["partitions"]:
                if not partition["consumed"]:
                    self._redpanda.logger.debug(
                        f"Consumption of partition {partition['partition']} isn't finished"
                    )
                    return False
            return True

        wait_until(check_consumed,
                   timeout_sec,
                   2,
                   err_msg=f"consumers haven't finished in {timeout_sec}s")

        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/wait-consumer")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def _remote_info(self):
        if not self.is_alive(self._node):
            self.raise_on_violation(self._node)
            raise CrushedException(
                "CompactedVerifier is crashed (it often happens on the consistency violation)"
            )
        ip = self._node.account.hostname
        url = f"http://{ip}:8080/info"
        self._redpanda.logger.debug(f"Dispatching {url}")
        r = requests.get(url)
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )
        self._redpanda.logger.debug(f"Received {json.dumps(r.json())}")
        return r.json()

    def remote_info(self):
        def wrapper():
            try:
                return (True, self._remote_info())
            except CrushedException:
                raise
            except ConsistencyViolationException:
                raise
            except:
                self._redpanda.logger.debug(
                    "Got error on fetching info, retrying?!", exc_info=True)
            return (False, None)

        # info is a lightweight request, it's ok to have small timeout
        return wait_until_result(wrapper, timeout_sec=5, backoff_sec=1)
