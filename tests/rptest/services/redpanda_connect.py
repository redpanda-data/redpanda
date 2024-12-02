# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

import os
import typing

from ducktape.services.service import Service
from ducktape.tests.test import TestContext

import requests
from rptest.services.redpanda import RedpandaService
from ducktape.utils.util import wait_until

from ducktape.cluster.cluster import ClusterNode


class RedpandaConnectService(Service):
    """
    Redpanda Connect service managed by RPK
    """
    PERSISTENT_ROOT = "/var/lib/redpanda_connect/"
    RPK_BIN = "rpk"
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "connect.log")
    logs = {
        "redpanda_connect_log": {
            "path": LOG_FILE,
            "collect_default": True
        }
    }

    redpanda: RedpandaService
    _pid: typing.Optional[int]
    logging_config = """
logger:
    level: TRACE
    format: logfmt
    add_timestamp: true
    static_fields:
        '@service': redpanda-connect

"""

    def __init__(self, context: TestContext, redpanda: RedpandaService):
        super().__init__(context, num_nodes=1)
        self.redpanda = redpanda
        self._url = None

    def _rpk_binary(self):
        # NOTE: since this runs on separate nodes from the service, the binary
        # path used by each node may differ from that returned by
        # redpanda.find_binary(), e.g. if using a RedpandaInstaller.
        rp_install_path_root = self.context.globals.get(
            "rp_install_path_root", "")
        return f"{rp_install_path_root}/bin/rpk"

    def clean_node(self, node):
        self.logger.debug(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process(self.RPK_BIN, clean_shutdown=False)

        if node.account.exists(self.PERSISTENT_ROOT):
            node.account.remove(self.PERSISTENT_ROOT)
        self.logger.info("Uninstalling redpanda-connect from %s",
                         node.account.hostname)

        self._execute_cmd(node, ["uninstall"])

    def _execute_cmd(self, node: ClusterNode, cmd: list):
        return node.account.ssh_output(self._connect_command(cmd))

    def _connect_command(self, cmd: list):
        return " ".join([self._rpk_binary(), "connect"] + cmd)

    def start_node(self, node):
        node.account.mkdirs(self.PERSISTENT_ROOT)
        self.logger.info("Installing redpanda-connect on %s",
                         node.account.hostname)
        self._execute_cmd(node, ["install"])
        cfg_path = os.path.join(self.PERSISTENT_ROOT, "config.yaml")
        node.account.create_file(cfg_path, self.logging_config)

        node.account.ssh(
            f"{self._connect_command(['streams', '-o', cfg_path])}  1>> {self.LOG_FILE} 2>> {self.LOG_FILE} &"
        )
        self.url = f"http://{node.account.hostname}:4195"

        def _ready():
            r = requests.get(f"http://{node.account.hostname}:4195/ready",
                             timeout=5)
            return r.status_code == 200

        wait_until(_ready,
                   timeout_sec=30,
                   backoff_sec=0.5,
                   err_msg="Redpanda Connect failed to start",
                   retry_on_exc=True)

    def start_stream(self, name: str, config: dict):
        """Starts a stream with the given name and config. 
           For more information visit:

           https://docs.redpanda.com/redpanda-connect

        Args:
            name (str): stream name
            config (dict): stream configuration
        """
        self.logger.debug(
            f"Starting stream {name} with config {json.dumps(config)}")
        self._request("POST", f"streams/{name}", json=config)

    def remove_stream(self, name: str):
        self._request("DELETE", f"streams/{name}")

    def wait_for_stream_to_finish(self,
                                  name: str,
                                  timeout_sec=60,
                                  remove=True):
        """
        Waits for all streams to finish and then removes the stream
        """
        def _finished():
            streams = self._request("GET", f"streams").json()
            return name not in streams or streams[name]["active"] == False

        wait_until(_finished,
                   timeout_sec=timeout_sec,
                   backoff_sec=0.5,
                   err_msg=f"Timeout waiting for {name} stream to finish",
                   retry_on_exc=True)

        if remove:
            self.remove_stream(name)

    def _request(self, method, endpoint, **kwargs):
        self.logger.debug(f"Executing request {method} {self.url}/{endpoint}")
        result = requests.request(method, f"{self.url}/{endpoint}", **kwargs)
        self.logger.debug(
            f"Response from {method} {self.url}/{endpoint} - {result.status_code} {result.text}"
        )
        return result

    def wait_node(self, node, timeout_sec=60):
        """
        Waits for all streams to finish
        """
        def _all_streams_finished():
            streams = self._request("GET", f"streams").json()
            return all(s['active'] == False for id, s in streams.items())

        wait_until(_all_streams_finished,
                   timeout_sec=timeout_sec,
                   backoff_sec=0.5,
                   err_msg="Redpanda Connect not stopped",
                   retry_on_exc=True)

        return True

    def stop_node(self, node):
        node.account.kill_process("rpk.managed-connect", clean_shutdown=True)
