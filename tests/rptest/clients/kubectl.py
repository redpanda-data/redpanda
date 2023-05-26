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
    Wrapper around kubectl for operating on a redpanda cluster.
    """

    KUBECTL_VERSION = '1.24.10'

    def __init__(self,
                 redpanda,
                 cmd_prefix=[],
                 namespace='redpanda',
                 cluster_id=''):
        self._redpanda = redpanda
        self._cmd_prefix = cmd_prefix
        self._namespace = namespace
        self._cluster_id = cluster_id
        self._kubectl_installed = False

    def _install(self):
        if not self._kubectl_installed:
            download_cmd = self._cmd_prefix + [
                'wget', '-q',
                f'https://dl.k8s.io/release/v{self.KUBECTL_VERSION}/bin/linux/amd64/kubectl',
                '-O', '/tmp/kubectl'
            ]
            install_cmd = self._cmd_prefix + [
                'sudo', 'install', '-m', '0755', '/tmp/kubectl',
                '/usr/local/bin/kubectl'
            ]
            cleanup_cmd = self._cmd_prefix + ['rm', '-f', '/tmp/kubectl']
            config_cmd = self._cmd_prefix + [
                'awscli2', 'eks', 'update-kubeconfig', '--name',
                f'redpanda-{self._cluster_id}', '--region', 'us-west-2'
            ]
            try:
                self._redpanda.logger.info(download_cmd)
                res = subprocess.check_output(download_cmd)
                self._redpanda.logger.info(install_cmd)
                res = subprocess.check_output(install_cmd)
                self._redpanda.logger.info(cleanup_cmd)
                res = subprocess.check_output(cleanup_cmd)
                self._redpanda.logger.info(config_cmd)
                res = subprocess.check_output(config_cmd)
            except subprocess.CalledProcessError as e:
                self._redpanda.logger.info("kubectl error {}: {}".format(
                    e.returncode, e.output))
                exit(1)
            self._kubectl_installed = True
        return

    def exec(self, remote_cmd):
        self._install()
        cmd = self._cmd_prefix + [
            'kubectl', 'exec', '-n', self._namespace, '-c', 'redpanda',
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
        self._install()
        cmd = self._cmd_prefix + [
            'kubectl', 'exec', '-n', self._namespace, '-c', 'redpanda',
            f'rp-{self._cluster_id}-0', '--', 'stat'
        ] + [remote_path]
        try:
            subprocess.check_output(cmd)
            return True
        except subprocess.CalledProcessError as e:
            return False
