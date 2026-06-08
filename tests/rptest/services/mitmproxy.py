# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import shlex

from ducktape.services.service import Service
from ducktape.utils.util import wait_until

MITMPROXY_PORT = 8888
LOG_PATH = "/var/log/mitmproxy.log"
FLOWS_PATH = "/tmp/mitmproxy.flows"
CERT_PEM_PATH = "/tmp/mitmproxy-server.pem"


class MitmproxyService(Service):
    """Runs mitmproxy as a CONNECT-only forward proxy (no TLS
    interception). Exposes proxy_url() that the test configures
    Redpanda to use via the oidc_http_proxy_url cluster config.

    The --ignore-hosts '.*' flag disables mitmproxy's TLS MITM so
    every CONNECT request is tunnelled through transparently to the
    origin. This is what we want for this test because we only care
    about whether Redpanda speaks CONNECT correctly, not about
    inspecting the TLS contents.

    Optional TLS listener: call set_server_cert(cert) before start()
    to make mitmproxy autodetect TLS on its listener and present the
    given cert, so clients connect via https:// before issuing CONNECT
    (the "HTTPS proxy" scheme). This uses mitmproxy's --certs option;
    regular-mode listeners autodetect plaintext HTTP vs TLS on the
    same port.
    """

    logs = {
        "mitmproxy_log": {"path": LOG_PATH, "collect_default": True},
        "mitmproxy_flows": {"path": FLOWS_PATH, "collect_default": True},
    }

    def __init__(self, context):
        super().__init__(context, num_nodes=1)
        self._server_cert = None
        self._proxy_auth = None

    @property
    def node(self):
        return self.nodes[0]

    def set_server_cert(self, cert) -> None:
        """Enable TLS on the listener. `cert` must expose `.crt` and
        `.key` as on-disk paths (e.g. a TLSCertManager-produced
        certificate). Must be called before start().
        """
        self._server_cert = cert

    def set_proxy_auth(self, username: str, password: str) -> None:
        """Require HTTP Basic proxy authentication. Must be called
        before start(). Clients must send Proxy-Authorization: Basic
        base64(user:pass) on the CONNECT request or get a 407.
        """
        self._proxy_auth = f"{username}:{password}"

    def proxy_url(self) -> str:
        scheme = "https" if self._server_cert is not None else "http"
        return f"{scheme}://{self.node.account.hostname}:{MITMPROXY_PORT}"

    def start_node(self, node, **kwargs):
        extra_args = ""
        if self._server_cert is not None:
            # mitmproxy's --certs takes a single PEM containing the
            # cert chain and the private key concatenated. Build it
            # on the node.
            node.account.copy_to(self._server_cert.crt, "/tmp/_mitmproxy-cert.pem")
            node.account.copy_to(self._server_cert.key, "/tmp/_mitmproxy-key.pem")
            node.account.ssh(
                f"cat /tmp/_mitmproxy-cert.pem /tmp/_mitmproxy-key.pem "
                f"> {CERT_PEM_PATH}",
                allow_fail=False,
            )
            extra_args = f"--certs '*={CERT_PEM_PATH}' "

        if self._proxy_auth is not None:
            extra_args += f"--set {shlex.quote('proxyauth=' + self._proxy_auth)} "

        # PYTHONUNBUFFERED + stdbuf -oL: mitmdump writes progress to
        # stdout, and Python's default block-buffering means the file
        # stays empty for the whole test until the process exits.
        # Forcing line-buffered output means assert_proxied_host can
        # read the log mid-test.
        cmd = (
            f"nohup env PYTHONUNBUFFERED=1 stdbuf -oL -eL mitmdump "
            f"--mode regular "
            f"--listen-port {MITMPROXY_PORT} "
            f"--ignore-hosts '.*' "
            f"--save-stream-file {FLOWS_PATH} "
            f"{extra_args}"
            f">{LOG_PATH} 2>&1 &"
        )
        node.account.ssh(cmd)
        wait_until(
            lambda: self._is_listening(node),
            timeout_sec=30,
            err_msg="mitmproxy did not start listening",
        )

    def stop_node(self, node, **kwargs):
        node.account.kill_process("mitmdump", allow_fail=True)

    def safe_stop(self) -> None:
        """Stop the service, logging and swallowing exceptions. For
        use in test tearDowns where a stop failure should not mask
        the original test outcome.
        """
        try:
            self.stop()
        except Exception as e:
            self.logger.warn(f"Failed to stop mitmproxy: {e}")

    def clean_node(self, node, **kwargs):
        node.account.ssh(
            f"rm -f {LOG_PATH} {FLOWS_PATH} {CERT_PEM_PATH} "
            f"/tmp/_mitmproxy-cert.pem /tmp/_mitmproxy-key.pem",
            allow_fail=True,
        )

    def _is_listening(self, node) -> bool:
        # `ss` filter anchored on sport to avoid matching e.g. :18888;
        # -H suppresses the header row so the output is empty iff
        # nothing is listening on MITMPROXY_PORT.
        out = node.account.ssh_output(
            f"ss -lntH 'sport = :{MITMPROXY_PORT}'",
            allow_fail=True,
        )
        return bool(out and out.strip())

    def assert_proxied_host(self, expected_host: str) -> None:
        """Assert that mitmproxy's access log shows a tunneled CONNECT
        to the given host. Call this after the test workload has run.

        mitmproxy 11's regular-mode log records CONNECT tunnels as
        `server connect <host>:<port>` rather than the raw HTTP verb,
        so we accept either that form or the classic `CONNECT <host>`.
        Reads the log with sudo fallback in case mitmdump ran as root.
        """
        cmd = f"sudo cat {LOG_PATH} 2>/dev/null || cat {LOG_PATH}"
        log_contents = self.node.account.ssh_output(cmd, allow_fail=True)
        text = (
            log_contents.decode()
            if isinstance(log_contents, (bytes, bytearray))
            else (log_contents or "")
        )
        if not text.strip():
            raise AssertionError(f"mitmproxy log at {LOG_PATH} is empty or unreadable")
        if (
            f"CONNECT {expected_host}" not in text
            and f"server connect {expected_host}" not in text
        ):
            excerpt = "\n".join(text.splitlines()[-30:])
            raise AssertionError(
                f"no CONNECT tunnel to {expected_host} found in mitmproxy "
                f"log; last 30 lines:\n{excerpt}"
            )
