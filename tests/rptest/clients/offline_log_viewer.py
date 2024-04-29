# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import subprocess
import uuid


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

    def _json_cmd(self, node, suffix):
        cmd = self._cmd(suffix=suffix)
        json_out = node.account.ssh_output(cmd, combine_stderr=False)
        try:
            return json.loads(json_out)
        except json.decoder.JSONDecodeError:
            # Log the bad output before re-raising
            self._redpanda.logger.error(f"Invalid JSON output: {json_out}")
            raise

    def read_kafka_records(self, node, topic):
        return self._json_cmd(node, f"--type kafka_records --topic {topic}")

    def read_controller(self, node):
        return self._json_cmd(node, "--type controller")

    def has_controller_snapshot(self, node):
        return node.account.exists(
            f"{self._redpanda.DATA_DIR}/redpanda/controller/0_0/snapshot")

    def read_controller_snapshot(self, node):
        return self._json_cmd(node, "--type controller_snapshot")

    def read_consumer_offsets(self, node):
        return self._json_cmd(node, "--type consumer_offsets")

    def read_bin_topic_manifest(self, bin_data, return_legacy_format=True):
        """
        copy bin_data to a local file, invoke the tool with it and 
        return a dict with the decoded result. 
        return_legacy_format=True produces an output like the original 
        topic_manifest.json
        """
        path = f"/tmp/{str(uuid.uuid4())}.topic_manifest.bin"
        with open(path, "wb") as f:
            f.write(bin_data)

        cmd = f"python3 /opt/scripts/offline_log_viewer/viewer.py --type { 'topic_manifest_legacy' if return_legacy_format else 'topic_manifest' } --path {path}"
        json_out = subprocess.Popen(cmd, shell=True,
                                    stdout=subprocess.PIPE).stdout.read()
        try:
            return json.loads(json_out)
        except json.decoder.JSONDecodeError:
            # Log the bad output before re-raising
            self._redpanda.logger.error(f"Invalid JSON output: {json_out}")
            raise
