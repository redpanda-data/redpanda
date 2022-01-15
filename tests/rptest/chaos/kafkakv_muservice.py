# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import sys
import traceback

from rptest.chaos.muservice import MuService


class KafkaKVMuService(MuService):
    PERSISTENT_ROOT = "/mnt/kafkakv"
    LOG_OK = os.path.join(PERSISTENT_ROOT, "kafkakv_ok.log")
    LOG_ERR = os.path.join(PERSISTENT_ROOT, "kafkakv_err.log")
    LOG_OK_1 = os.path.join(PERSISTENT_ROOT, "kafkakv_ok.log.1")
    LOG_ERR_1 = os.path.join(PERSISTENT_ROOT, "kafkakv_err.log.1")
    KAFKAKV_SCRIPT = "/opt/v/src/consistency-testing/chaostest/control/kafkakv.py"
    KAFKAKV_PORT = 8888
    KAFKAKV_CONCURRENCY = 36
    TOPIC = "topic1"
    STDOUT = "/dev/null"

    READY_TIMEOUT_SEC = 10
    V_DEV_MOUNT = "/opt/v"

    logs = {
        "kafkakv_ok_0": {
            "path": LOG_OK,
            "collect_default": True
        },
        "kafkakv_ok_1": {
            "path": LOG_OK_1,
            "collect_default": True
        },
        "kafkakv_err_0": {
            "path": LOG_ERR,
            "collect_default": True
        },
        "kafkakv_err_1": {
            "path": LOG_ERR_1,
            "collect_default": True
        },
    }

    def __init__(self, redpanda_mu):
        super(KafkaKVMuService, self).__init__()
        self.redpanda_mu = redpanda_mu
        self.endpoints = []

    def start(self, service, nodes):
        for node in nodes:
            node.account.mkdirs(KafkaKVMuService.PERSISTENT_ROOT)

            cmd = f"nohup python3 {KafkaKVMuService.KAFKAKV_SCRIPT} "
            cmd += f"--port {KafkaKVMuService.KAFKAKV_PORT} "
            cmd += f"--topic {KafkaKVMuService.TOPIC} "
            cmd += f"--inflight-limit {KafkaKVMuService.KAFKAKV_CONCURRENCY} "
            cmd += f"--err {KafkaKVMuService.LOG_ERR} "
            cmd += f"--log {KafkaKVMuService.LOG_OK} "
            for broker in self.redpanda_mu.brokers:
                cmd += f"--broker {broker} "
            cmd += f"> {KafkaKVMuService.STDOUT} 2>&1 &"

            service.logger.info(
                "Starting iofaults service on {} with command: {}".format(
                    node.account, cmd))

            with node.account.monitor_log(KafkaKVMuService.LOG_OK) as mon:
                node.account.ssh(cmd)
                mon.wait_until(
                    "Successfully started iofaults!",
                    timeout_sec=KafkaKVMuService.READY_TIMEOUT_SEC,
                    backoff_sec=0.5,
                    err_msg="kafkakv didn't finish startup in {} seconds".
                    format(KafkaKVMuService.READY_TIMEOUT_SEC))

            self.endpoints.append(
                f"{node.account.hostname}:{KafkaKVMuService.KAFKAKV_PORT}")

    def stop_node(self, service, node):
        try:
            node.account.ssh_output(
                "ps aux | egrep [k]afkakv.py | awk '{print $2}' | xargs -r kill -9",
                allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            service.logger.error(
                "Failed to kill kafkakv type: {} value: {} stacktrace: {}".
                format(str(e), str(v), stacktrace))
            raise

    def clean_node(self, service, node):
        self.stop_node(service, node)
        node.account.ssh_output(
            f"sudo rm -rf {KafkaKVMuService.PERSISTENT_ROOT}",
            allow_fail=False)
