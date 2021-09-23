# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import time
import json


class KafkaCat:
    """
    Wrapper around the kcat utility.

    This tools is useful because it offers a JSON output format.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def metadata(self):
        return self._cmd(["-L"])

    def consume_one(self, topic, partition, offset):
        return self._cmd([
            "-C", "-e", "-t", f"{topic}", "-p", f"{partition}", "-o",
            f"{offset}", "-c1"
        ])

    def _cmd(self, cmd):
        for retry in reversed(range(10)):
            try:
                res = subprocess.check_output(
                    ["kcat", "-b",
                     self._redpanda.brokers(), "-J"] + cmd)
                res = json.loads(res)
                self._redpanda.logger.debug(json.dumps(res, indent=2))
                return res
            except subprocess.CalledProcessError as e:
                if retry == 0:
                    raise
                self._redpanda.logger.debug(
                    "kcat retrying after exit code {}: {}".format(
                        e.returncode, e.output))
                time.sleep(2)
