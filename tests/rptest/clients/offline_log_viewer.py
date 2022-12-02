# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json


class OfflineLogViewer:
    """
    Wrap tools/offline_log_viewer for use in tests: this is for tests that
    want to peek at the structures, but also for validating the tool itself.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def _cmd(self, suffix):
        viewer_path = "python3 /opt/scripts/offline_log_viewer/viewer.py"
        return f"{viewer_path} --path {self._redpanda.DATA_DIR} {suffix}"

    def read_kvstore(self, node):
        cmd = self._cmd("--type kvstore")
        kvstore_json = node.account.ssh_output(cmd, combine_stderr=False)
        return json.loads(kvstore_json)

    def read_controller(self, node):
        cmd = self._cmd("--type controller")
        controller_json = node.account.ssh_output(cmd, combine_stderr=False)
        try:
            return json.loads(controller_json)
        except json.decoder.JSONDecodeError:
            # Log the bad output before re-raising
            self._redpanda.logger.error(
                f"Invalid JSON output: {controller_json}")
            import time
            time.sleep(3600)
            raise
