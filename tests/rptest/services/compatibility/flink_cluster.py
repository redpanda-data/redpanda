# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
import time

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError
import rptest.services.compatibility.flink_examples as FlinkExamples


# Using a Ducktape Service instead of BackgroundThreadService
# because the Flink cluster itself is run like a linux service
class FlinkCluster(Service):
    """
    The service that runs a Flink cluster on a single ducktape node
    """
    def __init__(self, context):
        super(FlinkCluster, self).__init__(context, num_nodes=1)

        self._node = None

    def is_start_line(self, line):
        return "Starting cluster." in line or "Starting standalonesession daemon on host" in line or "Starting taskexecutor daemon on host" in line

    def start_node(self, node):
        cmd = f"{FlinkExamples.FLINK_BIN}/start-cluster.sh"
        output_iter = node.account.ssh_capture(cmd, timeout_sec=10)
        for line in output_iter:
            line = line.strip()
            self.logger.debug(f"Starting flink cluster: {line}")
            assert self.is_start_line(line)

        self._node = node

    def stop_node(self, node):
        cmd = f"{FlinkExamples.FLINK_BIN}/stop-cluster.sh"
        output_iter = node.account.ssh_capture(cmd, timeout_sec=10)
        for line in output_iter:
            line = line.strip()
            self.logger.debug(f"Stopping flink cluster: {line}")

    def clean_node(self, node):
        node.account.remove(f"{FlinkExamples.FLINK_DIR}/log/*",
                            allow_fail=True)
        # Remove all files in log
        self.logger.debug(f"Cleaned flink log")

    def run_flink_job(self, flink_job, timeout=60):
        assert self._node

        output_iter = self._node.account.ssh_capture(flink_job.cmd(),
                                                     allow_fail=True,
                                                     timeout_sec=10)

        def check_job():
            line = next(output_iter)
            line = line.strip()
            self.logger.debug(f"Running flink job: {line}")
            flink_job.condition(line)
            return flink_job.condition_met()

        wait_until(check_job,
                   timeout_sec=timeout,
                   backoff_sec=5,
                   err_msg=f"failed to run flink job {flink_job.name}")
