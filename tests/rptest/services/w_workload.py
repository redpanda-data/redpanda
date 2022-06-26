# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from threading import Event
import requests
from time import sleep
import sys
from rptest.util import wait_until


class WWorkload(Service):

    logs = {
        "w_stdout_stderr": {
            "path": "/opt/remote/var/w.log",
            "collect_default": True
        }
    }

    def __init__(self, context, redpanda):
        super(WWorkload, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._stopping = Event()
        self._is_done = False
        self._node = None

    def is_alive(self, node):
        result = node.account.ssh_output("bash /opt/remote/control/w_alive.sh")
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
        node.account.ssh("bash /opt/remote/control/w_start.sh")
        sleep(1)
        wait_until(
            lambda: self.is_alive(node),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"w workload {node.account.hostname} failed to start within {timeout_sec} sec",
            retry_on_exc=False)
        self._node = node
        wait_until(
            lambda: self.is_ready(),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"w workload {node.account.hostname} failed to become ready within {timeout_sec} sec",
            retry_on_exc=False)

    def stop_node(self, node):
        node.account.ssh("bash /opt/remote/control/w_stop.sh")

    def clean_node(self, node):
        pass

    def wait_node(self, node, timeout_sec=sys.maxsize):
        wait_until(
            lambda: not (self.is_alive(node)),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=
            f"w workload {node.account.hostname} failed to stop within {timeout_sec} sec",
            retry_on_exc=False)
        return True

    #########################################

    def remote_ping(self):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/ping")
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def remote_wait(self, name):
        ip = self._node.account.hostname
        r = requests.get(f"http://{ip}:8080/wait", json={"name": name})
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def remote_start(self, name, connection, topic, count, keep_history=False):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/start",
                          json={
                              "name": name,
                              "connection": connection,
                              "topic": topic,
                              "count": count,
                              "keep_history": keep_history
                          })
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")

    def remote_validate(self, name, timeout_s):
        ip = self._node.account.hostname
        r = requests.post(f"http://{ip}:8080/validate",
                          json={
                              "name": name,
                              "timeout_s": timeout_s
                          })
        if r.status_code != 200:
            raise Exception(f"unexpected status code: {r.status_code}")
