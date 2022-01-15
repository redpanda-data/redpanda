# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import sys
import traceback

from rptest.chaos.muservice import MuService

from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.chaos.kafkakv_muservice import KafkaKVMuService

import logging

chaos_event_log = logging.getLogger("chaos-event")
errors_log = logging.getLogger("errors")


class RedpandaMuServiceServiceProxy:
    def __init__(self, service, redpanda_mu):
        self.logger = service.logger
        self.redpanda_mu = redpanda_mu

    def brokers(self, limit=None):
        return ",".join(self.redpanda_mu.brokers[:limit])


class RedpandaMuService(MuService):
    PERSISTENT_ROOT = "/mnt/iofaults/mount"

    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "redpanda.yaml")
    STDOUT_STDERR_CAPTURE = os.path.join("/mnt/iofaults", "redpanda.log")
    CLUSTER_NAME = "my_cluster"
    READY_TIMEOUT_SEC = 10
    V_DEV_MOUNT = "/opt/v"

    BUILD_DIR_KEY = "redpanda_build_dir"
    DEFAULT_BUILD_DIR = "vbuild"
    BUILD_TYPE_KEY = "redpanda_build_type"
    DEFAULT_BUILD_TYPE = "release"
    COMPILER_KEY = "redpanda_compiler"
    DEFAULT_COMPILER = "clang"
    PACKAGING_KEY = "redpanda_packaging"
    DEFAULT_PACKAGING = "dir"

    logs = {
        "redpanda_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        },
    }

    def __init__(self, log_level='info'):
        super(RedpandaMuService, self).__init__()
        self._log_level = log_level
        self.brokers = []

    def start(self, service, nodes):
        for node in nodes:
            self._start_node(service, node)
            self.brokers.append(f"{node.account.hostname}:9092")

        expected = set(nodes)
        wait_until(lambda: {n
                            for n in nodes
                            if self._registered(service, n)} == expected,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Cluster membership did not stabilize")

    def api_start_service(self, service, node):
        node.account.mkdirs(RedpandaMuService.DATA_DIR)

        cmd = "nohup {} ".format(self._find_binary(service, "redpanda"))
        cmd += "--redpanda-cfg {} ".format(RedpandaMuService.CONFIG_FILE)
        cmd += "--default-log-level {} ".format(self._log_level)
        cmd += "--logger-log-level=exception=debug "
        cmd += ">> {0} 2>&1 &".format(RedpandaMuService.STDOUT_STDERR_CAPTURE)

        service.logger.info(
            "Starting Redpanda service on {} with command: {}".format(
                node.account, cmd))

        node.account.ssh(cmd)

    def api_meta(self, service, node):
        topic = KafkaKVMuService.TOPIC
        return node.account.ssh_output(
            f"kcat -L -b {node.account.hostname} -t {topic} -J",
            allow_fail=False)

    def api_is_running(self, service, node):
        try:
            status = node.account.ssh_output(
                "ps -C redpanda >/dev/null && echo -n YES || echo -n NO",
                allow_fail=False)
            status = status.decode("utf-8")
            if status == "YES":
                return True
            elif status == "NO":
                return False
            else:
                raise Exception(f"Unknown status: {status}")
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            service.logger.error(
                "Failed to check if redpanda running: {} value: {} stacktrace: {}"
                .format(str(e), str(v), stacktrace))
            raise

    def api_kill_service(self, service, node):
        try:
            node.account.ssh_output(
                "ps aux | egrep [re]dpanda/bin | awk '{print $2}' | xargs -r kill -9",
                allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            service.logger.error(
                "Failed to kill redpanda type: {} value: {} stacktrace: {}".
                format(str(e), str(v), stacktrace))
            raise

    def stop_node(self, service, node):
        self.api_kill_service(service, node)

    def clean_node(self, service, node):
        self.api_kill_service(service, node)

    def _start_node(self, service, node):
        self._write_conf_file(service, node)

        self.api_start_service(service, node)

        # wait until redpanda has finished booting up
        with node.account.monitor_log(
                RedpandaMuService.STDOUT_STDERR_CAPTURE) as mon:
            mon.wait_until(
                "Successfully started Redpanda!",
                timeout_sec=RedpandaMuService.READY_TIMEOUT_SEC,
                backoff_sec=0.5,
                err_msg="Redpanda didn't finish startup in {} seconds".format(
                    RedpandaMuService.READY_TIMEOUT_SEC))

    def _find_binary(self, service, name):
        build_type = service.context.globals.get(
            RedpandaMuService.BUILD_TYPE_KEY,
            RedpandaMuService.DEFAULT_BUILD_TYPE)
        compiler = service.context.globals.get(
            RedpandaMuService.COMPILER_KEY, RedpandaMuService.DEFAULT_COMPILER)
        packaging = service.context.globals.get(
            RedpandaMuService.PACKAGING_KEY,
            RedpandaMuService.DEFAULT_PACKAGING)
        build_dir = service.context.globals.get(
            RedpandaMuService.BUILD_DIR_KEY,
            RedpandaMuService.DEFAULT_BUILD_DIR)

        if packaging not in {"dir"}:
            raise RuntimeError("Packaging type %s not supported" % packaging)

        path = os.path.join(RedpandaMuService.V_DEV_MOUNT, build_dir,
                            build_type, compiler, "dist/local/redpanda/bin",
                            name)

        if not os.path.exists(path):
            raise RuntimeError("Couldn't find binary %s: %s", name, path)

        service.logger.debug("Found binary %s: %s", name, path)

        return path

    def _write_conf_file(self, service, node):
        seed_id = min([service.idx(n) for n in service.nodes])
        seed = next(filter(lambda n: service.idx(n) == seed_id, service.nodes))

        conf = service.render("redpanda.yaml",
                              node_hostname=node.account.hostname,
                              node_id=service.idx(node),
                              seed_id=seed_id,
                              seed_hostname=seed.account.hostname,
                              data_dir=RedpandaMuService.DATA_DIR,
                              cluster=RedpandaMuService.CLUSTER_NAME)

        service.logger.info("Writing Redpanda config file: {}".format(
            RedpandaMuService.CONFIG_FILE))
        service.logger.debug(conf)
        node.account.create_file(RedpandaMuService.CONFIG_FILE, conf)

    def _registered(self, service, node):
        idx = service.idx(node)
        service.logger.debug("Checking if broker %d/%s is registered", idx,
                             node)
        kc = KafkaCat(RedpandaMuServiceServiceProxy(service, self))
        brokers = kc.metadata()["brokers"]
        brokers = {b["id"]: b for b in brokers}
        broker = brokers.get(idx, None)
        service.logger.debug("Found broker info: %s", broker)
        return broker is not None
