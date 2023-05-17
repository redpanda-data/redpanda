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
    def __init__(self,
                 redpanda,
                 cmd_prefix=[],
                 namespace='redpanda',
                 cluster_id=''):
        self._redpanda = redpanda
        self._cmd_prefix = cmd_prefix
        self._namespace = namespace
        self._cluster_id = cluster_id

    def exec(self, remote_cmd):
        cmd = self._cmd_prefix + [
            'kubectl', 'exec', '-n', self._namespace,
            f'rp-{self._cluster_id}-0', '--', 'bash', '-c'
        ] + ['"' + remote_cmd + '"']
        try:
            self._redpanda.logger.info(cmd)
            res = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            self._redpanda.logger.info("kubectl error {}: {}".format(
                e.returncode, e.output))
            exit(1)
        return res

    def exists(self, remote_path):
        cmd = self._cmd_prefix + [
            'kubectl', 'exec', '-n', self._namespace,
            f'rp-{self._cluster_id}-0', '--', 'stat'
        ] + [remote_path]
        try:
            subprocess.check_output(cmd)
            return True
        except subprocess.CalledProcessError as e:
            return False
