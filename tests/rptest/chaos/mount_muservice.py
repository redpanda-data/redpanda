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


class MountMuService(MuService):
    PERSISTENT_ROOT = "/mnt/iofaults"
    MOUNTPOINT_DIR = "/mnt/iofaults/mount"
    BACKEND_DIR = "/mnt/iofaults/back"
    LOG = os.path.join(PERSISTENT_ROOT, "iofaults.log")
    IOFAULT_SCRIPT = "/opt/v/src/consistency-testing/iofaults/iofaults.py"
    IOFAULT_PORT = 9093

    READY_TIMEOUT_SEC = 10
    V_DEV_MOUNT = "/opt/v"

    logs = {
        "iofaults_start_stdout_stderr": {
            "path": LOG,
            "collect_default": True
        },
    }

    def __init__(self):
        super(MountMuService, self).__init__()

    def start(self, service, nodes):
        for node in nodes:
            node.account.mkdirs(MountMuService.PERSISTENT_ROOT)
            node.account.mkdirs(MountMuService.MOUNTPOINT_DIR)
            node.account.mkdirs(MountMuService.BACKEND_DIR)

            cmd = f"nohup python3 {MountMuService.IOFAULT_SCRIPT} "
            cmd += f"{MountMuService.IOFAULT_PORT} "
            cmd += f"{MountMuService.MOUNTPOINT_DIR} "
            cmd += f"{MountMuService.BACKEND_DIR} "
            cmd += f">> {MountMuService.LOG} 2>&1 &"

            service.logger.info(
                "Starting iofaults service on {} with command: {}".format(
                    node.account, cmd))

            with node.account.monitor_log(MountMuService.LOG) as mon:
                node.account.ssh(cmd)
                mon.wait_until(
                    "Successfully started iofaults!",
                    timeout_sec=MountMuService.READY_TIMEOUT_SEC,
                    backoff_sec=0.5,
                    err_msg="iofaults didn't finish startup in {} seconds".
                    format(MountMuService.READY_TIMEOUT_SEC))

    def stop_node(self, service, node):
        try:
            node.account.ssh_output(
                "ps aux | egrep [io]faults.py | awk '{print $2}' | xargs -r kill -9",
                allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            service.logger.error(
                "Failed to kill iofaults type: {} value: {} stacktrace: {}".
                format(str(e), str(v), stacktrace))
            raise

    def clean_node(self, service, node):
        self.stop_node(service, node)

        try:
            node.account.ssh_output(
                f"sudo umount {MountMuService.MOUNTPOINT_DIR} || true",
                allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            service.logger.error(
                "Failed to umount: {} value: {} stacktrace: {}".format(
                    str(e), str(v), stacktrace))
            raise

        node.account.ssh_output(
            f"sudo rm -rf {MountMuService.PERSISTENT_ROOT}", allow_fail=False)
