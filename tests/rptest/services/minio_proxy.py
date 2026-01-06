# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import signal

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.background_thread import BackgroundThreadService

from rptest.util import inject_remote_script


class MinioProxy(BackgroundThreadService):
    """
    Service that proxies HTTP requests to MinIO with configurable delay.

    This service runs on a dedicated node and forwards all S3/MinIO API
    requests to the actual MinIO backend, optionally adding a delay to
    each request. This is useful for testing tiered storage behavior
    under slow network conditions.
    """

    LOG_DIR = "/tmp/minio_proxy"
    STDOUT_CAPTURE = os.path.join(LOG_DIR, "minio_proxy.stdout")

    logs = {
        "minio_proxy_stdout": {"path": STDOUT_CAPTURE, "collect_default": True},
    }

    def __init__(
        self,
        context,
        port=9000,
        backend_host="minio-s3",
        backend_port=9000,
        delay_ms=0,
        access_key="panda-user",
        secret_key="panda-secret",
        stop_timeout_sec=5,
    ):
        """
        Initialize the MinIO proxy service.

        Args:
            context: Test context
            port: Port to listen on for proxy requests
            backend_host: Hostname of the actual MinIO backend
            backend_port: Port of the actual MinIO backend
            delay_ms: Delay in milliseconds to add to each request
            access_key: AWS/MinIO access key for signature recalculation
            secret_key: AWS/MinIO secret key for signature recalculation
            stop_timeout_sec: Timeout for stopping the service
        """
        super(MinioProxy, self).__init__(context, 1)
        self.port = port
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.delay_ms = delay_ms
        self.access_key = access_key
        self.secret_key = secret_key
        self.stop_timeout_sec = stop_timeout_sec
        self.remote_script_path = None

    @property
    def hostname(self):
        """Return the hostname where the proxy is running."""
        return self.nodes[0].account.hostname

    @property
    def endpoint(self):
        """Return the proxy endpoint URL."""
        return f"{self.hostname}:{self.port}"

    def set_delay(self, delay_ms):
        """
        Update the delay configuration.

        Note: This requires restarting the service to take effect.
        """
        self.delay_ms = delay_ms

    def _worker(self, idx, node):
        """Background worker that runs the proxy."""
        node.account.ssh(f"mkdir -p {MinioProxy.LOG_DIR}", allow_fail=False)

        self.remote_script_path = inject_remote_script(node, "minio_proxy.py")

        cmd = f"python3 {self.remote_script_path}"
        cmd += f" --port {self.port}"
        cmd += f" --backend-host {self.backend_host}"
        cmd += f" --backend-port {self.backend_port}"
        cmd += f" --delay-ms {self.delay_ms}"
        cmd += f" --access-key {self.access_key}"
        cmd += f" --secret-key {self.secret_key}"
        cmd += f" | tee -a {MinioProxy.STDOUT_CAPTURE} &"

        self.logger.info(
            f"Starting MinIO proxy on {self.hostname}:{self.port} "
            f"-> {self.backend_host}:{self.backend_port} "
            f"with {self.delay_ms}ms delay and AWS SigV4 recalculation"
        )

        for line in node.account.ssh_capture(cmd):
            line = line.strip()
            if line:
                self.logger.debug(f"Proxy: {line}")

    def pids(self, node):
        """Get PIDs of running proxy processes."""
        try:
            cmd = "ps ax | grep minio_proxy.py | grep -v grep | awk '{print $1}'"
            pid_arr = [
                pid
                for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def stop_all(self):
        """Stop proxy on all nodes."""
        for node in self.nodes:
            self.stop_node(node)

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        """Kill proxy process on a node."""
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

    def stop_node(self, node, clean_shutdown=True):
        """Stop proxy on a specific node."""
        self.kill_node(node, clean_shutdown=clean_shutdown)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, (
            f"Node {str(node.account)}: MinIO proxy did not stop within "
            f"{self.stop_timeout_sec} seconds"
        )

    def clean_node(self, node):
        """Clean up proxy resources on a node."""
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh(f"rm -rf {MinioProxy.LOG_DIR}", allow_fail=False)
        if self.remote_script_path:
            node.account.ssh(f"rm -rf {self.remote_script_path}")
