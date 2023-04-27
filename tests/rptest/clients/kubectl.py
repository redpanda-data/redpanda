# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess


class KubectlTool:
    """
    Wrapper around kubectl.
    """
    def __init__(self, redpanda, namespace='redpanda'):
        self._redpanda = redpanda
        self._namespace = namespace

    def exec(self, remote_cmd):
        cmd = [
            'kubectl', 'exec', '-n', self._namespace, 'redpanda-0', '--',
            'bash', '-c'
        ] + [remote_cmd]
        try:
            res = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            # log but ignore for now
            self._redpanda.logger.info("kubectl error {}: {}".format(
                e.returncode, e.output))
        return res

    def exists(self, remote_path):
        cmd = [
            'kubectl', 'exec', '-n', self._namespace, 'redpanda-0', '--',
            'stat'
        ] + [remote_path]
        try:
            subprocess.check_output(cmd)
            return True
        except subprocess.CalledProcessError as e:
            return False
