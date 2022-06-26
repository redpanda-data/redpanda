# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service
import requests
from time import sleep
import sys
from rptest.util import wait_until


class CrushedException(Exception):
    pass


class ConsistencyViolationException(Exception):
    pass


OUTPUT_LOG = "/opt/remote/var/rw.log"


class RWWorkload(Service):
    logs = {"rw_stdout_stderr": {"path": OUTPUT_LOG, "collect_default": True}}

    def __init__(self, context, redpanda):
        super(RWWorkload, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._is_done = False
        self._node = None

    def is_alive(self, node):
        result = node.account.ssh_output(
            "bash /opt/remote/control/rw_alive.sh")
        result = result.decode("utf-8")
        return "YES" in result

    def is_ready(self):
        try:
            self.remote_ping()
            return True
        except requests.exceptions.ConnectionError:
            return False

    ### Service overrides

    def start_node(self, node, timeout_sec=10):
        node.account.ssh("bash /opt/remote/control/rw_start.sh")
        sleep(1)
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
        node.account.ssh("bash /opt/remote/control/rw_stop.sh")
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

    #########################################

    def remote_ping(self):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/ping")
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def remote_start(self, connection, topic, partitions):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/start",
                          json={
                              "brokers": connection,
                              "topic": topic,
                              "partitions": partitions
                          })
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def remote_stop(self):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/stop")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def remote_wait(self):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/wait")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )

    def remote_info(self):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/info")
        if r.status_code != 200:
            raise Exception(
                f"unexpected status code: {r.status_code} content: {r.content}"
            )
        return r.json()

    def ensure_progress(self, delta, timeout_sec):
        if not self.is_alive(self._node):
            self.raise_on_violation(self._node)
            raise CrushedException(
                "rw_workload is crushed (it often happens on the consistency violation)"
            )

        old_state = self.remote_info()
        min_reads = old_state["min_reads"]
        min_writes = old_state["min_writes"]

        def check_writes():
            new_state = self.remote_info()
            if new_state["min_writes"] - min_writes < delta:
                return False
            return True

        def check_reads():
            new_state = self.remote_info()
            if new_state["min_reads"] - min_reads < delta:
                return False
            return True

        wait_until(
            check_writes,
            timeout_sec,
            2,
            err_msg=f"writes got stuck: no {delta} progress in {timeout_sec}s")
        wait_until(
            check_reads,
            timeout_sec,
            2,
            err_msg=f"reads got stuck: no {delta} progress in {timeout_sec}s")

    def has_cleared(self, threshold, timeout_sec):
        if not self.is_alive(self._node):
            self.raise_on_violation(self._node)
            raise CrushedException(
                "rw_workload is crushed (it often happens on the consistency violation)"
            )

        def check():
            new_state = self.remote_info()
            if new_state["min_reads"] < threshold:
                return False
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
