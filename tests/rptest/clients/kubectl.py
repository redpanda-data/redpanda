# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
from logging import Logger
import os
import subprocess
from typing import Any, Union, Generator

SUPPORTED_PROVIDERS = ['aws', 'gcp']


def is_redpanda_pod(pod_obj: dict[str, Any], cluster_id: str) -> bool:
    """Returns true if the pod API object name matches the Redpanda pattern."""
    return pod_obj['metadata']['name'].startswith(f'rp-{cluster_id}')


class KubectlTool:
    """
    Wrapper around kubectl for operating on a redpanda cluster.
    """

    TELEPORT_DATA_DIR = '/tmp/tbot-data'
    TELEPORT_DEST_DIR = '/tmp/machine-id'
    TELEPORT_IDENT_FILE = f'{TELEPORT_DEST_DIR}/identity'

    def __init__(
        self,
        redpanda,
        *,
        remote_uri=None,
        namespace='redpanda',
        cluster_id='',
        cluster_privider='aws',
        cluster_region='us-west-2',
        tp_proxy=None,
        tp_token=None,
    ):
        self._redpanda = redpanda
        self._remote_uri = remote_uri
        self._namespace = namespace
        self._cluster_id = cluster_id

        self._provider = cluster_privider.lower()
        if self._provider not in SUPPORTED_PROVIDERS:
            raise RuntimeError("KubectlTool does not yet support "
                               f"'{self._provider}' cloud provider")

        self._region = cluster_region
        self._tp_proxy = tp_proxy
        self._tp_token = tp_token
        self._kubectl_installed = False
        self._privileged_pod_installed = False
        self._setup_tbot()

    def _ssh_prefix(self):
        '''Generate the ssh prefix of a cmd.

        Example output of the 4 types of ssh prefixes:
         0. # nothing if there is no _remote_uri to run a remote command onto
         1. ssh target.example.com # for simple ssh
         2. tsh ssh --proxy=proxy.example.com target.example.com # for local dev that will use github auth by default
         3. tsh ssh --proxy=proxy.example.com --identity=/tmp/machine-id/identity target.example.com # for headless assuming tbot start
        '''
        if self._remote_uri is None:
            return []
        if self._tp_proxy is None:
            return [
                'ssh',
                self._remote_uri,
            ]
        if self._tp_token is None:
            return [
                'tsh',
                'ssh',
                f'--proxy={self._tp_proxy}',
                '--auth=okta',
                self._remote_uri,
            ]
        return [
            'tsh',
            'ssh',
            f'--proxy={self._tp_proxy}',
            '--auth=okta',
            f'--identity={self.TELEPORT_IDENT_FILE}',
            self._remote_uri,
        ]

    def _scp_cmd(self, src, dest):
        '''Generate the scp cmd.

        Example output of the 4 types of ssh prefixes:
         0. # nothing if there is no _remote_uri to run a remote command onto
         1. scp src dest # for simple scp passwordless
         2. tsh scp --proxy=proxy.example.com src dest # for local dev that will use github auth by default
         3. tsh scp --proxy=proxy.example.com --identity=/tmp/machine-id/identity src dest # for headless assuming tbot start
        '''
        if self._remote_uri is None:
            # do not copy anything if kubectl is on local machine
            return []
        if self._tp_proxy is None:
            return ['scp', src, dest]
        if self._tp_token is None:
            return [
                'tsh', 'scp', f'--proxy={self._tp_proxy}', '--auth=okta', src,
                dest
            ]
        return [
            'tsh', 'scp', f'--proxy={self._tp_proxy}', '--auth=okta',
            f'--identity={self.TELEPORT_IDENT_FILE}', src, dest
        ]

    def _aws_config_cmd(self):
        return [
            'awscli2', 'eks', 'update-kubeconfig', '--name',
            f'redpanda-{self._cluster_id}', '--region', self._region
        ]

    def _gcp_config_cmd(self):
        return [
            'gcloud', 'container', 'clusters', 'get-credentials',
            f'redpanda-{self._cluster_id}', '--region', self._region
        ]

    def _install(self):
        '''Installs kubectl on a remote target host
        '''
        if not self._kubectl_installed and self._remote_uri is not None:
            ssh_prefix = self._ssh_prefix()
            bg_cmd = ssh_prefix + ['./breakglass-tools.sh']

            if self._provider == 'aws':
                config_cmd = ssh_prefix + self._aws_config_cmd()
            elif self._provider == 'gcp':
                config_cmd = ssh_prefix + self._gcp_config_cmd()

            self._redpanda.logger.info(bg_cmd)
            res = subprocess.check_output(bg_cmd)
            self._redpanda.logger.info(config_cmd)
            res = subprocess.check_output(config_cmd)
            self._kubectl_installed = True
        return

    @property
    def logger(self) -> Logger:
        return self._redpanda.logger

    def _cmd(self, cmd):
        # Log and run
        ssh_prefix = self._ssh_prefix()
        remote_cmd = ssh_prefix + cmd
        self._redpanda.logger.info(remote_cmd)
        try:
            return subprocess.check_output(remote_cmd, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            self.logger.info(
                f'Command failed (rc={e.returncode}).\n' +
                f'--------- stdout -----------\n{e.stdout.decode()}' +
                f'--------- stderr -----------\n{e.stderr.decode()}')
            raise

    def cmd(self, kcmd: list[str] | str):
        """Execute a kubectl command on the agent node.
        """
        # prepare
        self._install()
        _kubectl = ['kubectl']

        # Make it universal for str/list
        _kcmd = kcmd if isinstance(kcmd, list) else kcmd.split()
        # Format command
        cmd = _kubectl + _kcmd
        return self._cmd(cmd)

    def exec(self, remote_cmd, pod_name=None) -> str:
        """Execute a command inside of a redpanda pod container.

        :param remote_cmd: string of bash command to run inside of pod container
        :param pod_name: name of the pod, e.g. 'rp-clo88krkqkrfamptsst0-5', defaults to pod 0
        """

        self._install()
        if pod_name is None:
            pod_name = f'rp-{self._cluster_id}-0'
        cmd = [
            'kubectl', 'exec', pod_name, f'-n={self._namespace}',
            '-c=redpanda', '--', 'bash', '-c'
        ] + ['"' + remote_cmd + '"']
        return self._ssh_cmd(cmd)  # type: ignore

    def exists(self, remote_path):
        self._install()
        ssh_prefix = self._ssh_prefix()
        cmd = ssh_prefix + [
            'kubectl', 'exec', '-n', self._namespace, '-c', 'redpanda',
            f'rp-{self._cluster_id}-0', '--', 'stat'
        ] + [remote_path]
        try:
            subprocess.check_output(cmd)
            return True
        except subprocess.CalledProcessError as e:
            return False

    def _get_privileged_pod(self, pod_name=None):
        # kubectl get pod -l name=privileged-pod --no-headers -o custom-columns=NODE:.spec.nodeName,NAME:.metadata.name
        # ip-10-1-1-26.us-west-2.compute.internal    everything-allowed-exec-pod-bkj4m
        # ip-10-1-1-139.us-west-2.compute.internal   everything-allowed-exec-pod-jxk9j
        # ip-10-1-1-101.us-west-2.compute.internal   everything-allowed-exec-pod-pl8sc
        ssh_prefix = self._ssh_prefix()
        cmd = ssh_prefix + [
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
        cmd = ssh_prefix + [
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
        if pod_name is None:
            pod_name = f'rp-{self._cluster_id}-0'
        return redpanda_to_priv_pods[pod_name]

    def _setup_tbot(self):
        if self._tp_proxy is None or self._tp_token is None:
            self._redpanda.logger.info(
                'skipping tbot start to generate identity')
            return None
        # Select method to join
        _method = "iam"
        if self._provider == 'gcp':
            _method = "gcp"
        self._redpanda.logger.info('cleaning teleport data dir')
        subprocess.check_output(['rm', '-f', '-r', self.TELEPORT_DATA_DIR])
        self._redpanda.logger.info('starting tbot to generate identity')
        cmd = [
            'tbot', 'start', f'--data-dir={self.TELEPORT_DATA_DIR}',
            f'--destination-dir={self.TELEPORT_DEST_DIR}',
            f'--auth-server={self._tp_proxy}', f'--join-method={_method}',
            f'--token={self._tp_token}', '--certificate-ttl=6h',
            '--renewal-interval=6h', '--oneshot'
        ]
        return subprocess.check_output(cmd)

    def _setup_privileged_pod(self):
        if not self._privileged_pod_installed and self._remote_uri is not None:
            filename = 'everything-allowed-exec-pod.yml'
            filename_path = os.path.join(os.path.dirname(__file__),
                                         'everything-allowed-exec-pod.yml')
            self._redpanda.logger.info(filename_path)
            setup_cmd = self._scp_cmd(filename_path, f'{self._remote_uri}:')
            ssh_prefix = self._ssh_prefix()
            apply_cmd = ssh_prefix + ['kubectl', 'apply', '-f', filename]
            if len(setup_cmd) > 0:
                self._redpanda.logger.info(setup_cmd)
                res = subprocess.check_output(setup_cmd)
            self._redpanda.logger.info(apply_cmd)
            res = subprocess.check_output(apply_cmd)
            self._privileged_pod_installed = True

    def exec_privileged(self, remote_cmd, pod_name=None):
        self._setup_privileged_pod()
        priv_pod = self._get_privileged_pod(pod_name)
        ssh_prefix = self._ssh_prefix()
        cmd = ssh_prefix + ['kubectl', 'exec', priv_pod, '--', 'bash', '-c'
                            ] + ['"' + remote_cmd + '"']
        self._redpanda.logger.debug(cmd)
        res = subprocess.run(cmd, capture_output=True, text=True)
        self._redpanda.logger.debug(res.stdout)
        return res


class KubeNodeShell():
    def __init__(self, kubectl: KubectlTool, node_name: str) -> None:
        self.kubectl = kubectl
        self.node_name = node_name
        # It is bad, but it works
        self.logger = self.kubectl._redpanda.logger
        self.current_context = self.kubectl.cmd(
            f"config current-context").decode().strip()
        # Make sure that name is not longer that 63 chars
        # The Pod "gke-redpanda-co9uuq78jo-redpanda-6a66-fcfacc41-65mz-priviledged-shell" is invalid: metadata.labels:
        # Invalid value: "gke-redpanda-co9uuq78jo-redpanda-6a66-fcfacc41-65mz-priviledged-shell": must be no more than 63 characters
        self.pod_name = f"{node_name}-pshell"
        if len(self.pod_name) > 63:
            # Assume that our added chars broke the limit
            # Cut them to fit
            self.pod_name = self.pod_name[:63]

        # In case of concurrent tests, just reuse existing pod
        self.pod_reused = True if self._is_shell_running() else False

    def _is_shell_running(self):
        # Check if such pod exists
        try:
            _out = self.kubectl.cmd(f"get pods -A | grep {self.pod_name}")
            return len(_out) > 0
        except:
            # Above command fails only when pod is not found
            return False

    def _build_overrides(self):
        return {
            "spec": {
                "nodeName":
                self.node_name,
                "hostPID":
                True,
                "hostNetwork":
                True,
                "containers": [{
                    "securityContext": {
                        "privileged": True
                    },
                    "image":
                    "docker.io/library/alpine",
                    "name":
                    "nsenter",
                    "stdin":
                    True,
                    "stdinOnce":
                    True,
                    "tty":
                    True,
                    "command": [
                        "nsenter", "--target", "1", "--mount", "--uts",
                        "--ipc", "--net", "--pid", "bash", "-l"
                    ],
                    "resources": {},
                    "volumeMounts": []
                }],
                "tolerations": [{
                    "key": "CriticalAddonsOnly",
                    "operator": "Exists"
                }, {
                    "effect": "NoExecute",
                    "operator": "Exists"
                }],
                "volumes": []
            }
        }

    def __enter__(self):
        if not self.pod_reused:
            # Init node shell
            overrides = self._build_overrides()
            _out = self.kubectl.cmd([
                f"--context={self.current_context}",
                "run",
                "--image docker.io/library/alpine",
                "--restart=Never",
                f"--overrides='{json.dumps(overrides)}'",
                # "--pod-running-timeout=1m",
                f"{self.pod_name}"
            ])
            self.logger.debug(f"Response: {_out.decode()}")
        return self

    def __call__(self, cmd: list[str] | str) -> list:
        self.logger.info(f"Running command inside node '{self.node_name}'")
        # Prefix for running inside proper pod
        _kcmd = ["exec", self.pod_name, "--"]
        # Universal for list and str
        _cmd = cmd if isinstance(cmd, list) else cmd.split()
        _kcmd += _cmd
        # exception handler is inside subclass
        _out = self.kubectl.cmd(_kcmd)
        return _out.decode().splitlines()

    def __exit__(self, *args, **kwargs):
        # If this instance created this pod, delete it
        if not self.pod_reused:
            try:
                self.kubectl.cmd(f"delete pod {self.pod_name}")
            except Exception as e:
                self.logger.warning("Failed to delete node shell pod "
                                    f"'{self.pod_name}': {e}")
        return
