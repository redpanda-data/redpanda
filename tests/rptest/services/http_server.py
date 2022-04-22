# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import os
import signal

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.background_thread import BackgroundThreadService
from rptest.util import inject_remote_script


class HttpServer(BackgroundThreadService):
    """
    Service wrapping simple HTTP server that logs requests to stdout
    """

    LOG_DIR = "/tmp/simple_http_server"
    STDOUT_CAPTURE = os.path.join(LOG_DIR, "simple_http_server.stdout")

    logs = {
        "verifiable_consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True
        },
    }

    def __init__(self, context, port=8080, stop_timeout_sec=2):
        super(HttpServer, self).__init__(context, 1)
        self.port = port
        self.stop_timeout_sec = stop_timeout_sec
        self.requests = []
        # server is running on single node
        self.url = f"http://{self.nodes[0].account.hostname}:{self.port}"
        self.remote_script_path = None

    def _worker(self, idx, node):
        node.account.ssh(f"mkdir -p {HttpServer.LOG_DIR}", allow_fail=False)

        self.remote_script_path = inject_remote_script(
            node, "simple_http_server.py")
        cmd = f"python3 {self.remote_script_path} --port {self.port}"
        cmd += f" | tee -a {HttpServer.STDOUT_CAPTURE} &"

        self.logger.debug(f"Starting HTTP server {self.url}")
        for line in node.account.ssh_capture(cmd):
            request = self.try_parse_json(node, line.strip())
            if request is not None:
                self.logger.debug(f"received request: {request}")
                self.requests.append(request)

    def pids(self, node):
        try:
            cmd = "ps ax | grep simple_http_server.py | grep -v grep | awk '{print $1}'"
            pid_arr = [
                pid for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def try_parse_json(self, node, string):
        try:
            return json.loads(string)
        except ValueError:
            self.logger.debug(
                f"{str(node.account)}: Could not parse as json: {str(string)}")
            return None

    def stop_all(self):
        for node in self.nodes:
            self.stop_node(node)

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        sig = signal.SIGTERM
        if not clean_shutdown:
            sig = signal.SIGKILL
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(node, clean_shutdown=clean_shutdown)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, f"Node {str(node.account)}: did not stop within the specified timeout of {self.stop_timeout_sec} seconds"

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh(f"rm -rf {self.LOG_DIR}", allow_fail=False)
        if self.remote_script_path:
            node.account.ssh(f"rm -rf {self.remote_script_path}")
