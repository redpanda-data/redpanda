# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess


class HelmTool:
    """
    Wrapper around helm.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def install(self):
        cmd = [
            'helm', 'install', 'redpanda', 'redpanda/redpanda', '--namespace',
            'redpanda', '--create-namespace', '--set',
            'external.domain=customredpandadomain.local', '--set',
            'statefulset.initContainers.setDataDirOwnership.enabled=true',
            '--wait'
        ]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            # log but ignore for now
            self._redpanda.logger.info("helm error {}: {}".format(
                e.returncode, e.output))

    def uninstall(self):
        cmd = [
            'helm', 'uninstall', '--namespace', 'redpanda', 'redpanda',
            '--wait'
        ]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            # log but ignore for now
            self._redpanda.logger.info("helm error {}: {}".format(
                e.returncode, e.output))
