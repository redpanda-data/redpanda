import os
import signal

import yaml

from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until


class PandaProxyService(Service):
    PERSISTENT_ROOT = "/mnt/pandaproxy"
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "pandaproxy.yaml")
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "pandaproxy.log")
    READY_TIMEOUT_SEC = 10
    V_DEV_MOUNT = "/opt/v"

    # ducktape `--globals` for selecting build
    BUILD_TYPE_KEY = "redpanda_build_type"
    DEFAULT_BUILD_TYPE = "release"
    COMPILER_KEY = "redpanda_compiler"
    DEFAULT_COMPILER = "clang"
    PACKAGING_KEY = "redpanda_packaging"
    DEFAULT_PACKAGING = "dir"

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

    def start_node(self, node):
        node.account.mkdirs(PandaProxyService.PERSISTENT_ROOT)
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
        node.account.remove(PandaProxyService.PERSISTENT_ROOT)

    def find_binary(self, name):
        root = self._build_root()
        path = os.path.join(root, "bin", name)
        self.logger.debug("Found binary %s: %s", name, path)
        return path

    def _build_root(self):
        # TODO: figure out how to use python mixin design pattern to factor out
        # the build path calculation to share between services.
        build_type = self._context.globals.get(
            PandaProxyService.BUILD_TYPE_KEY,
            PandaProxyService.DEFAULT_BUILD_TYPE)
        compiler = self._context.globals.get(
            PandaProxyService.COMPILER_KEY, PandaProxyService.DEFAULT_COMPILER)
        packaging = self._context.globals.get(
            PandaProxyService.PACKAGING_KEY,
            PandaProxyService.DEFAULT_PACKAGING)

        if packaging not in {"dir"}:
            raise RuntimeError("Packaging type %s not supported" % packaging)

        return os.path.join(PandaProxyService.V_DEV_MOUNT, "build", build_type,
                            compiler, "dist/local/pandaproxy")

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
                           root=self._build_root())

        self.logger.info("Writing Pandaproxy config file: {}".format(
            PandaProxyService.CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(PandaProxyService.CONFIG_FILE, conf)
