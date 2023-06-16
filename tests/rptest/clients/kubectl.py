# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import subprocess


class KubectlTool:
    """
    Wrapper around kubectl for operating on a redpanda cluster.
    """

    KUBECTL_VERSION = '1.24.10'

    def __init__(self,
                 redpanda,
                 *,
                 remote_uri=None,
                 namespace='redpanda',
                 cluster_id=''):
        self._redpanda = redpanda
        self._remote_uri = remote_uri
        self._namespace = namespace
        self._cluster_id = cluster_id
        self._kubectl_installed = False
        self._privileged_pod_installed = False

    def _install(self):
        '''Installs kubectl on a remote target host
        '''
        if not self._kubectl_installed and self._remote_uri is not None:
            download_cmd = [
                'tsh', 'ssh', self._remote_uri, 'wget', '-q',
                f'https://dl.k8s.io/release/v{self.KUBECTL_VERSION}/bin/linux/amd64/kubectl',
                '-O', '/tmp/kubectl'
            ]
            install_cmd = [
                'tsh', 'ssh', self._remote_uri, 'sudo', 'install', '-m',
                '0755', '/tmp/kubectl', '/usr/local/bin/kubectl'
            ]
            cleanup_cmd = [
                'tsh', 'ssh', self._remote_uri, 'rm', '-f', '/tmp/kubectl'
            ]
            config_cmd = [
                'tsh', 'ssh', self._remote_uri, 'awscli2', 'eks',
                'update-kubeconfig', '--name', f'redpanda-{self._cluster_id}',
                '--region', 'us-west-2'
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
                self._redpanda.logger.info("CalledProcessError {}: {}".format(
                    e.returncode, e.output))
                exit(1)
            self._kubectl_installed = True
        return

    def exec(self, remote_cmd):
        self._install()
        prefix = []
        if self._remote_uri:
            prefix = ['tsh', 'ssh', self._remote_uri]
        cmd = prefix + [
            'kubectl', 'exec', '-n', self._namespace, '-c', 'redpanda',
            f'rp-{self._cluster_id}-0', '--', 'bash', '-c'
        ] + ['"' + remote_cmd + '"']
        try:
            self._redpanda.logger.info(cmd)
            res = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            self._redpanda.logger.info("CalledProcessError {}: {}".format(
                e.returncode, e.output))
            exit(1)
        return res

    def exists(self, remote_path):
        self._install()
        prefix = []
        if self._remote_uri:
            prefix = ['tsh', 'ssh', self._remote_uri]
        cmd = prefix + [
            'kubectl', 'exec', '-n', self._namespace, '-c', 'redpanda',
            f'rp-{self._cluster_id}-0', '--', 'stat'
        ] + [remote_path]
        try:
            subprocess.check_output(cmd)
            return True
        except subprocess.CalledProcessError as e:
            return False

    def _get_privileged_pod(self):
        # kubectl get pod -l name=privileged-pod --no-headers -o custom-columns=NODE:.spec.nodeName,NAME:.metadata.name
        # ip-10-1-1-26.us-west-2.compute.internal    everything-allowed-exec-pod-bkj4m
        # ip-10-1-1-139.us-west-2.compute.internal   everything-allowed-exec-pod-jxk9j
        # ip-10-1-1-101.us-west-2.compute.internal   everything-allowed-exec-pod-pl8sc
        cmd = [
            'tsh',
            'ssh',
            self._remote_uri,
            'kubectl',
            'get',
            'pod',
            '-l',
            'name=privileged-pod',
            '--no-headers',
            '-o',
            'custom-columns=NODE:.spec.nodeName,NAME:.metadata.name',
        ]
        self._redpanda.logger.info(cmd)
        res = subprocess.run(cmd, capture_output=True, text=True)
        ip_to_priv_pods = {}
        for line in res.stdout.splitlines():
            s = line.split()
            ip_to_priv_pods[s[0]] = s[1]
        self._redpanda.logger.info(ip_to_priv_pods)

        # kubectl -n redpanda get pod -l app.kubernetes.io/name=redpanda --no-headers -o custom-columns=NODE:.spec.nodeName,NAME:.metadata.name
        # ip-10-1-1-139.us-west-2.compute.internal   rp-ci0motok30vsi89l501g-0
        # ip-10-1-1-101.us-west-2.compute.internal   rp-ci0motok30vsi89l501g-1
        # ip-10-1-1-26.us-west-2.compute.internal    rp-ci0motok30vsi89l501g-2
        cmd = [
            'tsh',
            'ssh',
            self._remote_uri,
            'kubectl',
            '-n',
            'redpanda',
            'get',
            'pod',
            '-l',
            'app.kubernetes.io/name=redpanda',
            '--no-headers',
            '-o',
            'custom-columns=NODE:.spec.nodeName,NAME:.metadata.name',
        ]
        self._redpanda.logger.info(cmd)
        res = subprocess.run(cmd, capture_output=True, text=True)
        ip_to_redpanda_pods = {}
        for line in res.stdout.splitlines():
            s = line.split()
            ip_to_redpanda_pods[s[0]] = s[1]
        self._redpanda.logger.info(ip_to_redpanda_pods)

        redpanda_to_priv_pods = {}
        for ip, redpanda_pod in ip_to_redpanda_pods.items():
            redpanda_to_priv_pods[redpanda_pod] = ip_to_priv_pods[ip]

        self._redpanda.logger.info(redpanda_to_priv_pods)
        redpanda_pod = f'rp-{self._cluster_id}-0'
        return redpanda_to_priv_pods[redpanda_pod]

    def _setup_privileged_pod(self):
        if not self._privileged_pod_installed and self._remote_uri is not None:
            filename = 'everything-allowed-exec-pod.yml'
            filename_path = os.path.join(os.path.dirname(__file__),
                                         'everything-allowed-exec-pod.yml')
            self._redpanda.logger.info(filename_path)
            setup_cmd = ['tsh', 'scp', filename_path, f'{self._remote_uri}:']
            apply_cmd = [
                'tsh', 'ssh', self._remote_uri, 'kubectl', 'apply', '-f',
                filename
            ]
            try:
                self._redpanda.logger.info(setup_cmd)
                res = subprocess.check_output(setup_cmd)
                self._redpanda.logger.info(apply_cmd)
                res = subprocess.check_output(apply_cmd)
            except subprocess.CalledProcessError as e:
                self._redpanda.logger.info("CalledProcessError {}: {}".format(
                    e.returncode, e.output))
                exit(1)
            self._privileged_pod_installed = True

    def exec_privileged(self, remote_cmd):
        self._setup_privileged_pod()
        priv_pod = self._get_privileged_pod()
        prefix = []
        if self._remote_uri:
            prefix = ['tsh', 'ssh', self._remote_uri]
        cmd = prefix + ['kubectl', 'exec', priv_pod, '--', 'bash', '-c'
                        ] + ['"' + remote_cmd + '"']
        self._redpanda.logger.debug(cmd)
        res = subprocess.run(cmd, capture_output=True, text=True)
        self._redpanda.logger.debug(res.stdout)
        return res
