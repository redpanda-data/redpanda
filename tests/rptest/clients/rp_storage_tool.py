# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import tempfile
import subprocess
import json


class RpStorageTool:
    def __init__(self, logger):
        self.logger = logger

    def _decode(self, binary_data, command):
        # Decode to JSON using external tool
        with tempfile.NamedTemporaryFile(mode='wb') as tmp:
            tmp.write(binary_data)
            tmp.flush()
            cmd = ["rp-storage-tool", command, f"--path={tmp.name}"]
            self.logger.debug(
                f"Decoding {len(binary_data)} byte binary: {cmd}")
            p = subprocess.run(cmd, capture_output=True)
            if p.returncode != 0:
                self.logger.error(p.stderr)
                self.logger.error(f"Binary at: {tmp.name}")
                raise RuntimeError(f"{command} failed: {p.stderr}")
            else:
                json_bytes = p.stdout
                try:
                    return json.loads(json_bytes)
                except Exception as e:
                    self.logger.error(
                        f"Error loading JSON decoded with {command}: {e}")
                    self.logger.error(f"Decoded json: {json_bytes}")
                    raise

    def decode_partition_manifest(self, binary_data) -> dict:
        return self._decode(binary_data, "decode-partition-manifest")

    def decode_lifecycle_marker(self, binary_data) -> dict:
        return self._decode(binary_data, "decode-lifecycle-marker")
