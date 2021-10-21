# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import time


class KCL:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def list_topics(self):
        return self._cmd(['topic', 'list'])

    def list_groups(self):
        return self._cmd(["group", "list"])

    def produce(self, topic, msg):
        return self._cmd(["produce", topic], input=msg)

    def consume(self,
                topic,
                n=None,
                group=None,
                regex=False,
                fetch_max_bytes=None):
        cmd = ["consume"]
        if group is not None:
            cmd += ["-g", group]
        if n is not None:
            cmd.append(f"-n{n}")
        if regex:
            cmd.append("-r")
        if fetch_max_bytes is not None:
            cmd += ["--fetch-max-bytes", str(fetch_max_bytes)]
        cmd.append(topic)
        return self._cmd(cmd)

    def _cmd(self, cmd, input=None):
        brokers = self._redpanda.brokers()
        cmd = ["kcl", "-X", f"seed_brokers={brokers}", "--no-config-file"
               ] + cmd
        for retry in reversed(range(5)):
            try:
                res = subprocess.check_output(cmd, text=True, input=input)
                self._redpanda.logger.debug(res)
                return res
            except subprocess.CalledProcessError as e:
                if retry == 0:
                    raise
                self._redpanda.logger.debug(
                    "kcl retrying after exit code {}: {}".format(
                        e.returncode, e.output))
                time.sleep(1)
