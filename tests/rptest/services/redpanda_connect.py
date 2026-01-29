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

import requests
from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from prometheus_client.parser import text_string_to_metric_families

from rptest.services.redpanda import RedpandaService


class RedpandaConnectService(Service):
    """
    Redpanda Connect service managed by RPK
    """

    PERSISTENT_ROOT = "/var/lib/redpanda_connect/"
    RPK_BIN = "rpk"
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "connect.log")
    logs = {"redpanda_connect_log": {"path": LOG_FILE, "collect_default": True}}

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
        rp_install_path_root = self.context.globals.get("rp_install_path_root", "")
        return f"{rp_install_path_root}/bin/rpk"

    def clean_node(self, node):
        self.logger.debug(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process(self.RPK_BIN, clean_shutdown=False)

        if node.account.exists(self.PERSISTENT_ROOT):
            node.account.remove(self.PERSISTENT_ROOT)
        self.logger.info("Uninstalling redpanda-connect from %s", node.account.hostname)

        self._execute_cmd(node, ["uninstall"])

    def _execute_cmd(self, node: ClusterNode, cmd: list):
        return node.account.ssh_output(self._connect_command(cmd))

    def _connect_command(self, cmd: list):
        return " ".join([self._rpk_binary(), "connect"] + cmd)

    def start_node(self, node):
        node.account.mkdirs(self.PERSISTENT_ROOT)
        self.logger.info("Installing redpanda-connect on %s", node.account.hostname)
        self._execute_cmd(node, ["install"])
        cfg_path = os.path.join(self.PERSISTENT_ROOT, "config.yaml")
        node.account.create_file(cfg_path, self.logging_config)

        node.account.ssh(
            f"{self._connect_command(['streams', '-o', cfg_path])}  1>> {self.LOG_FILE} 2>> {self.LOG_FILE} &"
        )
        self.url = f"http://{node.account.hostname}:4195"

        def _ready():
            r = requests.get(f"http://{node.account.hostname}:4195/ready", timeout=5)
            return r.status_code == 200

        wait_until(
            _ready,
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg="Redpanda Connect failed to start",
            retry_on_exc=True,
        )

    def start_stream(self, name: str, config: dict):
        """Starts a stream with the given name and config.
           For more information visit:

           https://docs.redpanda.com/redpanda-connect

        Args:
            name (str): stream name
            config (dict): stream configuration
        """
        self.logger.debug(f"Starting stream {name} with config {json.dumps(config)}")
        self._request("POST", f"streams/{name}", json=config)

    def remove_stream(self, name: str, operation_timeout_sec=30):
        self._request(
            "DELETE", f"streams/{name}", params={"timeout": f"{operation_timeout_sec}s"}
        )

    def stream_metrics(self, name: str):
        metrics_resp = self._request("GET", "metrics")
        assert metrics_resp.status_code == 200
        families = text_string_to_metric_families(metrics_resp.text)
        metrics = dict()
        for family in families:
            for sample in family.samples:
                if sample.labels.get("stream") == name:
                    family_metrics = metrics.get(family.name, [])
                    family_metrics.append(sample)
                    metrics[family.name] = family_metrics
        return metrics

    def total_records_sent(self, name: str):
        samples = self.stream_metrics(name=name)["output_sent"]
        result = next((s.value for s in samples if s.name == "output_sent_total"), None)
        assert result is not None, f"Unable to probe metrics for stream {name}"
        return result

    def stop_stream(self, name: str, should_finish: bool | None = True, timeout_sec=60):
        """
        Optionally makse waits for the stream to finish and then removes the stream

        should_finish semantics:
            - True: wait for the stream to finish
            - False: make sure the stream has NOT finished when removed
            - None: just remove
        """

        def _finished():
            streams = self._request("GET", "streams").json()
            return name not in streams or streams[name]["active"] == False

        if should_finish == False:
            assert not _finished()
        if should_finish:
            wait_until(
                _finished,
                timeout_sec=timeout_sec,
                backoff_sec=0.5,
                err_msg=f"Timeout waiting for {name} stream to finish",
                retry_on_exc=True,
            )

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
            streams = self._request("GET", "streams").json()
            return all(s["active"] == False for id, s in streams.items())

        wait_until(
            _all_streams_finished,
            timeout_sec=timeout_sec,
            backoff_sec=0.5,
            err_msg="Redpanda Connect not stopped",
            retry_on_exc=True,
        )

        return True

    def stop_node(self, node):
        node.account.kill_process("rpk.managed-connect", clean_shutdown=True)
