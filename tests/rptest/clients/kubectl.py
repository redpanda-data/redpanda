# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import os
import subprocess
import time
from logging import Logger
from typing import Any, Generator, Union, overload, Literal

SUPPORTED_PROVIDERS = ["aws", "gcp", "azure"]

# Default retry settings for transient tsh/ssh failures
TSH_RETRY_COUNT = 3
TSH_RETRY_BACKOFF_SEC = 5


def run_with_retries(
    cmd: list[str],
    retries: int = TSH_RETRY_COUNT,
    backoff_sec: float = TSH_RETRY_BACKOFF_SEC,
    logger=None,
    **kwargs,
) -> bytes:
    """Run a subprocess command with retries on CalledProcessError.

    Intended for tsh/ssh commands that may fail transiently due to
    connectivity or auth issues. Makes ``retries`` total attempts with
    exponential backoff (backoff_sec, 2*backoff_sec, ...) between them.
    """
    if retries < 1:
        raise ValueError(f"retries must be >= 1, got {retries}")

    # Capture stderr so that error output is available for logging.
    kwargs.setdefault("stderr", subprocess.STDOUT)

    last_err: subprocess.CalledProcessError | None = None
    for attempt in range(1, retries + 1):
        try:
            return subprocess.check_output(cmd, **kwargs)
        except subprocess.CalledProcessError as e:
            last_err = e
            output = ""
            if e.output:
                output = e.output.decode("utf-8", errors="replace")[:300]
            if attempt < retries:
                if logger:
                    logger.warning(
                        f"Command failed (attempt {attempt}/{retries}, "
                        f"rc={e.returncode}): {' '.join(cmd)}"
                        + (f"\n{output}" if output else "")
                    )
                time.sleep(backoff_sec * (2 ** (attempt - 1)))
    if logger and last_err:
        output = ""
        if last_err.output:
            output = last_err.output.decode("utf-8", errors="replace")[:300]
        logger.error(
            f"Command failed after {retries} attempts: {' '.join(cmd)}"
            + (f"\n{output}" if output else "")
        )
    raise last_err  # type: ignore[misc]


def is_redpanda_pod(pod_obj: dict[str, Any], cluster_id: str) -> bool:
    """Returns true if the pod looks like a Redpanda broker pod"""

    # Azure behaves this way.  This is also the 'new' way going forward (circa 2024/7)
    # We look for pods whose metadata indicates that it is part of a statefulset, and that
    # the pod names are generated with the template "redpanda-broker-...".
    # False positives are quite unlikely with this criterion:
    # Scenario would be:
    # - Another Statefulset
    # - That reuses the SAME generateName (bad!)
    try:
        if pod_obj["metadata"]["generateName"] == "redpanda-broker-":
            if (
                pod_obj["metadata"]["labels"]["app.kubernetes.io/component"]
                == "redpanda-statefulset"
            ):
                return True
    except KeyError:
        pass

    # Other providers like AWS / GCP behave this way ("the old way")
    return pod_obj["metadata"]["name"].startswith(f"rp-{cluster_id}")


class KubectlTool:
    """
    Wrapper around kubectl for operating on a redpanda cluster.
    """

    TELEPORT_DATA_DIR = "/tmp/tbot-data"
    TELEPORT_DEST_DIR = "/tmp/machine-id"
    TELEPORT_IDENT_FILE = f"{TELEPORT_DEST_DIR}/identity"

    def __init__(
        self,
        redpanda: Any,
        *,
        remote_uri: str,
        namespace: str = "redpanda",
        cluster_id: str = "",
        cluster_provider: str = "aws",
        tp_proxy: str | None = None,
        tp_token: str | None = None,
    ):
        self._redpanda = redpanda
        self._remote_uri = remote_uri
        self._namespace = namespace
        self._cluster_id = cluster_id

        self._provider = cluster_provider.lower()
        if self._provider not in SUPPORTED_PROVIDERS:
            raise RuntimeError(
                f"KubectlTool does not yet support '{self._provider}' cloud provider"
            )

        self._tp_proxy = tp_proxy
        self._tp_token = tp_token
        self._setup_tbot()
        self._install_kubectl()
        self._setup_privileged_pod()

    def _ssh_prefix(self):
        """Generate the ssh prefix of a cmd.

        Example output of the 3 types of ssh prefixes:
         1. ssh target.example.com # for simple ssh
         2. tsh ssh --proxy=proxy.example.com target.example.com # for local dev that will use github auth by default
         3. tsh ssh --proxy=proxy.example.com --identity=/tmp/machine-id/identity target.example.com # for headless assuming tbot start
        """
        if self._tp_proxy is None:
            return [
                "ssh",
                self._remote_uri,
            ]
        if self._tp_token is None:
            return [
                "tsh",
                "ssh",
                f"--proxy={self._tp_proxy}",
                "--auth=okta",
                self._remote_uri,
            ]
        return [
            "tsh",
            "ssh",
            f"--proxy={self._tp_proxy}",
            "--auth=okta",
            f"--identity={self.TELEPORT_IDENT_FILE}",
            self._remote_uri,
        ]

    def _scp_cmd(self, src: str, dest: str):
        """Generate the scp cmd.

        Example output of the 3 types of scp commands:
         1. scp src dest # for simple scp passwordless
         2. tsh scp --proxy=proxy.example.com src dest # for local dev that will use github auth by default
         3. tsh scp --proxy=proxy.example.com --identity=/tmp/machine-id/identity src dest # for headless assuming tbot start
        """
        if self._tp_proxy is None:
            return ["scp", src, dest]
        if self._tp_token is None:
            return ["tsh", "scp", f"--proxy={self._tp_proxy}", "--auth=okta", src, dest]
        return [
            "tsh",
            "scp",
            f"--proxy={self._tp_proxy}",
            "--auth=okta",
            f"--identity={self.TELEPORT_IDENT_FILE}",
            src,
            dest,
        ]

    def _install_kubectl(self):
        """Installs kubectl on a remote target host"""
        breakglass_cmd = ["./breakglass-tools.sh"]
        if self._provider == "azure":
            # for azure, we manually override the path here to ensure
            # that azure-cli installed as a snap gets found (workaround)
            p = ["env", "PATH=/usr/local/bin:/usr/bin:/bin:/snap/bin"]
            breakglass_cmd = p + breakglass_cmd
        self._ssh_cmd(breakglass_cmd)

        # Install EKS token caching wrapper for AWS to avoid calling
        # 'aws eks get-token' on every kubectl invocation
        if self._provider == "aws":
            self._install_eks_token_cache_wrapper()

    def _install_eks_token_cache_wrapper(self):
        """Install a caching wrapper for aws eks get-token on the remote host.

        This wrapper caches tokens to avoid repeated AWS API calls on every
        kubectl invocation. Tokens are cached until 10 seconds before expiry.
        """
        wrapper_remote_path = "~/.kube/aws-eks-token-cache.sh"
        wrapper_local_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "remote_scripts",
            "cloud",
            "aws_eks_token_cache.sh",
        )

        try:
            scp_cmd = self._scp_cmd(
                wrapper_local_path, f"{self._remote_uri}:{wrapper_remote_path}"
            )
            self._redpanda.logger.debug(f"Copying EKS cache wrapper: {scp_cmd}")
            subprocess.check_output(scp_cmd)

            self._ssh_cmd(["chmod", "+x", wrapper_remote_path])

            # Update kubeconfig to use the wrapper instead of aws
            update_cmd = [
                "sed",
                "-i",
                f"'s#command: aws$#command: {wrapper_remote_path}#'",
                "~/.kube/config",
            ]
            self._ssh_cmd(update_cmd)

            self._redpanda.logger.info("Installed EKS token caching wrapper")
        except subprocess.CalledProcessError as e:
            self._redpanda.logger.warning(
                f"Failed to install EKS token caching wrapper: {e}"
            )

    @property
    def logger(self) -> Logger:
        return self._redpanda.logger

    def _local_captured(self, cmd: list[str]):
        """Runs kubectl subcommands on a Cloud Agent
        with streaming stdout and stderr to output

        Args:
            cmd (list[str]): kubectl commands to run

        Raises:
            CalledProcessError: when return code is non-zero

        Yields:
            Generator[bytes]: Generator of output line by line
        """
        self._redpanda.logger.info(cmd)
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )

        for line in process.stdout:  # type: ignore
            yield line

        process.wait()
        if process.returncode:
            raise subprocess.CalledProcessError(
                process.returncode, cmd, None, "stderr piped to stdout"
            )

    def _local_cmd(self, cmd: list[str], timeout: int = 900) -> str:
        """Run the given command locally and return stdout.

        throws CalledProcessError on non-zero exit code
        throws TimeoutExpired on timeout
        """

        def _prepare_output(sout: str, serr: str) -> str:
            return (
                f"\n--------- stdout -----------\n{sout}"
                f"\n--------- stderr -----------\n{serr}\n"
            )

        self._redpanda.logger.info(cmd)
        try:
            res = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout, check=True
            )
        except subprocess.CalledProcessError as e:
            self.logger.info(
                f"Command failed (rc={e.returncode}): "
                f"'{' '.join(cmd)}'\n"
                f"{_prepare_output(e.stdout, e.stderr)}"
            )
            raise
        self.logger.debug(_prepare_output(res.stdout, res.stderr))
        return res.stdout

    @property
    def _redpanda_operator_v2(self) -> bool:
        return self._provider == "azure"

    def _redpanda_broker_pod_name(self) -> str:
        if self._redpanda_operator_v2:
            return "redpanda-broker-0"
        else:
            return f"rp-{self._cluster_id}-blue-a-0"

    @overload
    def _ssh_cmd(self, cmd: list[str], capture: Literal[False] = False) -> str: ...

    @overload
    def _ssh_cmd(
        self, cmd: list[str], capture: Literal[True]
    ) -> Generator[bytes, Any, None]: ...

    def _ssh_cmd(
        self, cmd: list[str], capture: bool = False
    ) -> Union[str, Generator[bytes, Any, None]]:
        """Execute a command on a the remote node using ssh/tsh as appropriate."""
        local_cmd = self._ssh_prefix() + cmd
        if capture:
            return self._local_captured(local_cmd)
        else:
            return self._local_cmd(local_cmd)

    @overload
    def cmd(self, kcmd: list[str] | str, capture: Literal[False] = False) -> str: ...

    @overload
    def cmd(
        self, kcmd: list[str] | str, capture: Literal[True]
    ) -> Generator[bytes, Any, None]: ...

    def cmd(self, kcmd: list[str] | str, capture=False):
        """Execute a kubectl command on the agent node.
        Capture mode streams data from process stdout via Generator
        Non-capture more returns whole output as a list

        Args:
            kcmd (list[str] | str): command to run on agent with kubectl
            capture (bool, optional): Whether return whole result or
                                      iterate line by line. Defaults to False.

        Returns:
            list[str / bytes]: Return is either a whole lines list
                or a Generator with lines as items
        """
        _kubectl = ["kubectl"]

        # Make it universal for str/list
        _kcmd = kcmd if isinstance(kcmd, list) else kcmd.split()
        # Format command
        cmd = _kubectl + _kcmd
        return self._ssh_cmd(cmd, capture=capture)

    def exec(self, remote_cmd: str, pod_name: str | None = None) -> str:
        """Execute a command inside of a redpanda pod container.

        :param remote_cmd: string of bash command to run inside of pod container
        :param pod_name: name of the pod, e.g. 'rp-clo88krkqkrfamptsst0-5', defaults to pod 0
        """

        if pod_name is None:
            pod_name = self._redpanda_broker_pod_name()
        cmd = [
            "kubectl",
            "exec",
            pod_name,
            f"-n={self._namespace}",
            "-c=redpanda",
            "--",
            "bash",
            "-c",
        ] + ['"' + remote_cmd + '"']
        return self._ssh_cmd(cmd)

    def exists(self, remote_path: str, pod_name: str | None = None) -> bool:
        if pod_name is None:
            pod_name = self._redpanda_broker_pod_name()
        try:
            self._ssh_cmd(
                [
                    "kubectl",
                    "exec",
                    "-n",
                    self._namespace,
                    "-c",
                    "redpanda",
                    pod_name,
                    "--",
                    "stat",
                    remote_path,
                ]
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def _get_privileged_pod(self, pod_name=None):
        # kubectl get pod -l name=privileged-pod --no-headers -o custom-columns=NODE:.spec.nodeName,NAME:.metadata.name
        # ip-10-1-1-26.us-west-2.compute.internal    everything-allowed-exec-pod-bkj4m
        # ip-10-1-1-139.us-west-2.compute.internal   everything-allowed-exec-pod-jxk9j
        # ip-10-1-1-101.us-west-2.compute.internal   everything-allowed-exec-pod-pl8sc
        ssh_prefix = self._ssh_prefix()
        cmd = ssh_prefix + [
            "kubectl",
            "get",
            "pod",
            "-l",
            "name=privileged-pod",
            "--no-headers",
            "-o",
            "custom-columns=NODE:.spec.nodeName,NAME:.metadata.name",
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
            "kubectl",
            "-n",
            "redpanda",
            "get",
            "pod",
            "-l",
            "app.kubernetes.io/name=redpanda",
            "--no-headers",
            "-o",
            "custom-columns=NODE:.spec.nodeName,NAME:.metadata.name",
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
            pod_name = self._redpanda_broker_pod_name()
        return redpanda_to_priv_pods[pod_name]

    def _setup_tbot(self):
        if self._tp_proxy is None or self._tp_token is None:
            self._redpanda.logger.info("skipping tbot start to generate identity")
            return None
        # Select method to join
        _method = "iam"
        if self._provider == "gcp":
            _method = "gcp"
        elif self._provider == "azure":
            _method = "azure"
        self._redpanda.logger.info("cleaning teleport data dir")
        self._redpanda.logger.info(f"Selected join method: {_method}")
        subprocess.check_output(["rm", "-f", "-r", self.TELEPORT_DATA_DIR])
        self._redpanda.logger.info("starting tbot to generate identity")
        cmd = [
            "tbot",
            "start",
            f"--data-dir={self.TELEPORT_DATA_DIR}",
            f"--destination-dir={self.TELEPORT_DEST_DIR}",
            f"--auth-server={self._tp_proxy}",
            f"--join-method={_method}",
            f"--token={self._tp_token}",
            "--certificate-ttl=6h",
            "--renewal-interval=6h",
            "--oneshot",
        ]
        # Log the full command to be executed
        self._redpanda.logger.debug(f"Running tbot command: {cmd}")

        try:
            self._local_cmd(cmd)
        except subprocess.CalledProcessError:
            self._redpanda.logger.debug(f"Contents of {self.TELEPORT_DATA_DIR}:")
            subprocess.call(["ls", "-la", self.TELEPORT_DATA_DIR])
            self._redpanda.logger.debug(f"Contents of {self.TELEPORT_DEST_DIR}:")
            subprocess.call(["ls", "-la", self.TELEPORT_DEST_DIR])
            raise

    def _setup_privileged_pod(self):
        filename = "everything-allowed-exec-pod.yml"
        filename_path = os.path.join(
            os.path.dirname(__file__), "everything-allowed-exec-pod.yml"
        )
        self._redpanda.logger.info(filename_path)
        setup_cmd = self._scp_cmd(filename_path, f"{self._remote_uri}:")
        self._redpanda.logger.info(setup_cmd)
        run_with_retries(setup_cmd, logger=self._redpanda.logger)
        apply_cmd = ["kubectl", "apply", "-f", filename]
        self._ssh_cmd(apply_cmd)

    def exec_privileged(self, remote_cmd, pod_name=None):
        priv_pod = self._get_privileged_pod(pod_name)
        ssh_prefix = self._ssh_prefix()
        cmd = (
            ssh_prefix
            + ["kubectl", "exec", priv_pod, "--", "bash", "-c"]
            + ['"' + remote_cmd + '"']
        )
        self._redpanda.logger.debug(cmd)
        res = subprocess.run(cmd, capture_output=True, text=True)
        self._redpanda.logger.debug(res.stdout)
        return res


class KubeNodeShell:
    def __init__(
        self,
        kubectl: KubectlTool,
        node_name: str,
        namespace: str = "redpanda",
        clean=False,
    ) -> None:
        self.kubectl = kubectl
        self.node_name = node_name
        # It is bad, but it works
        self.logger = self.kubectl._redpanda.logger
        self.namespace = namespace
        self.current_context = self.kubectl.cmd("config current-context").strip()
        # Make sure that name is not longer that 63 chars
        # The Pod "gke-redpanda-co9uuq78jo-redpanda-6a66-fcfacc41-65mz-priviledged-shell" is invalid: metadata.labels:
        # Invalid value: "gke-redpanda-co9uuq78jo-redpanda-6a66-fcfacc41-65mz-priviledged-shell": must be no more than 63 characters
        self.pod_name = f"{node_name}-pshell"
        if len(self.pod_name) > 63:
            # Assume that our added chars broke the limit
            # Cut them to fit
            self.pod_name = self.pod_name[:63]

        # Set cleaning flag on exit
        self.clean = clean

    def _is_shell_running(self):
        # Check if such pod exists
        try:
            _out = self.kubectl.cmd(f"get pods -A | grep {self.pod_name}")
            return len(_out) > 0
        except Exception:
            # Above command fails only when pod is not found
            return False

    def _build_overrides(self):
        return {
            "spec": {
                "nodeName": self.node_name,
                "hostPID": True,
                "hostNetwork": True,
                "containers": [
                    {
                        "securityContext": {"privileged": True},
                        "image": "docker.io/library/alpine",
                        "name": "nsenter",
                        "stdin": True,
                        "stdinOnce": True,
                        "tty": True,
                        "command": [
                            "nsenter",
                            "--target",
                            "1",
                            "--mount",
                            "--uts",
                            "--ipc",
                            "--net",
                            "--pid",
                            "bash",
                            "-l",
                        ],
                        "resources": {},
                        "volumeMounts": [],
                    }
                ],
                "tolerations": [
                    {"key": "CriticalAddonsOnly", "operator": "Exists"},
                    {"effect": "NoExecute", "operator": "Exists"},
                ],
                "volumes": [],
            }
        }

    def initialize_nodeshell(self):
        if not self._is_shell_running():
            # Init node shell
            overrides = self._build_overrides()
            _out = self.kubectl.cmd(
                [
                    f"--context={self.current_context}",
                    f"--namespace={self.namespace}",
                    "run",
                    "--image docker.io/library/alpine",
                    "--restart=Never",
                    f"--overrides='{json.dumps(overrides)}'",
                    # Ensure the pod is running before this method returns.
                    "--pod-running-timeout=1m",
                    f"{self.pod_name}",
                ]
            )
            self.logger.debug(f"Response: {_out}")
        return self

    def destroy_nodeshell(self):
        if self._is_shell_running():
            try:
                self.kubectl.cmd(f"-n {self.namespace} delete pod {self.pod_name}")
            except Exception as e:
                self.logger.warning(
                    f"Failed to delete node shell pod '{self.pod_name}': {e}"
                )
        return

    def __enter__(self):
        return self.initialize_nodeshell()

    def __exit__(self, *args, **kwargs):
        if self.clean:
            self.destroy_nodeshell()
        return

    def __call__(self, cmd: list[str] | str, capture: bool = False):
        self.logger.info(f"Running command inside node '{self.node_name}'")
        # Prefix for running inside proper pod
        _kcmd = ["-n", f"{self.namespace}", "exec", self.pod_name, "--"]
        # Universal for list and str
        _cmd = cmd if isinstance(cmd, list) else cmd.split()
        _kcmd += _cmd
        # exception handler is inside subclass
        if capture:
            return self.kubectl.cmd(_kcmd, capture=capture)
        else:
            return self.kubectl.cmd(_kcmd, capture=capture).splitlines()
