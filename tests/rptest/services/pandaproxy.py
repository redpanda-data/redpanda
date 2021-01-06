# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import signal

from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until


class PandaProxyService(Service):
    PERSISTENT_ROOT = "/var/lib/pandaproxy"
    CONFIG_FILE = "/etc/pandaproxy/pandaproxy.yaml"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "pandaproxy.log")
    READY_TIMEOUT_SEC = 10

    logs = {
        "pandaproxy_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        },
    }

    def __init__(self, context, redpanda, num_proxies=1):
        super(PandaProxyService, self).__init__(context, num_nodes=num_proxies)
        self._context = context
        self._redpanda = redpanda

        self._rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", "")

        if not os.path.isdir(self._rp_install_path_root):
            raise RuntimeError(
                f"Install path: {self._rp_install_path_root} doesn't exist")

    def start_node(self, node):
        node.account.mkdirs(PandaProxyService.PERSISTENT_ROOT)
        node.account.mkdirs(os.path.dirname(PandaProxyService.CONFIG_FILE))

        platform = self._context.globals.get("platform", "docker-compose")

        if platform == "docker-compose":
            self.write_conf_file(node)

        cmd = "nohup {} ".format(self.find_binary("pandaproxy"))
        cmd += "--pandaproxy-cfg {} ".format(PandaProxyService.CONFIG_FILE)
        cmd += "--default-log-level=trace "
        cmd += ">> {0} 2>&1 &".format(PandaProxyService.STDOUT_STDERR_CAPTURE)

        self.logger.info(
            "Starting Pandaproxy service on {} with command: {}".format(
                node.account, cmd))

        # wait until pandaproxy has finished booting up
        with node.account.monitor_log(
                PandaProxyService.STDOUT_STDERR_CAPTURE) as mon:
            node.account.ssh(cmd)
            mon.wait_until(
                "Started Pandaproxy listening at",
                timeout_sec=PandaProxyService.READY_TIMEOUT_SEC,
                backoff_sec=0.5,
                err_msg="Pandaproxy didn't finish startup in {} seconds".
                format(PandaProxyService.READY_TIMEOUT_SEC))

    def stop_node(self, node):
        pids = self.pids(node)

        for pid in pids:
            node.account.signal(pid, signal.SIGTERM, allow_fail=False)

        timeout_sec = 30
        wait_until(lambda: len(self.pids(node)) == 0,
                   timeout_sec=timeout_sec,
                   err_msg="Pandaproxy node failed to stop in %d seconds" %
                   timeout_sec)

    def clean_node(self, node):
        node.account.kill_process("pandaproxy", clean_shutdown=False)
        node.account.remove(f"{PandaProxyService.PERSISTENT_ROOT}/*")
        node.account.remove(f"{PandaProxyService.CONFIG_FILE}")

    def find_binary(self, name):
        bin_path = f"{self._rp_install_path_root}/pandaproxy/bin/{name}"
        return bin_path

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "ps ax | grep -i pandaproxy | grep -v grep | awk '{print $1}'"
            pid_arr = [
                pid for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def write_conf_file(self, node):
        conf = self.render("pandaproxy.yaml",
                           broker=self._redpanda.nodes[0],
                           root=self._rp_install_path_root)

        self.logger.info("Writing Pandaproxy config file: {}".format(
            PandaProxyService.CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(PandaProxyService.CONFIG_FILE, conf)
