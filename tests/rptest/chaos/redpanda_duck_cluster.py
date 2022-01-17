# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import sys

from gobekli.logging import m
from rptest.chaos.mount_muservice import MountMuService
import logging
import traceback

chaos_event_log = logging.getLogger("chaos-event")
chaos_stdout = logging.getLogger("chaos-stdout")
errors_log = logging.getLogger("errors")


class RedpandaDuckNode:
    def __init__(self, service, redpanda_mu, node):
        self.service = service
        self.node = node
        self.node_id = node.account.hostname
        self.redpanda_mu = redpanda_mu

        try:
            ip = self.node.account.ssh_output("getent hosts " + self.node_id +
                                              " | awk '{ printf $1 }'",
                                              allow_fail=False)
            self.ip = ip.decode("utf-8")
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m(f"Failed to resolve {self.node_id} to ip",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise

    def meta(self):
        return self.redpanda_mu.api_meta(self.service, self.node)

    def kill(self):
        result = self.redpanda_mu.api_kill_service(self.service, self.node)
        return result

    def start_service(self):
        return self.redpanda_mu.api_start_service(self.service, self.node)

    def is_service_running(self):
        return self.redpanda_mu.api_is_running(self.service, self.node)

    def pause_service(self):
        try:
            self.node.account.ssh_output(
                "ps aux | egrep [re]dpanda/bin | awk '{print $2}' | xargs -r kill -STOP",
                allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m("Failed to suspend redpanda",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise

    def continue_service(self):
        try:
            self.node.account.ssh_output(
                "ps aux | egrep [re]dpanda/bin | awk '{print $2}' | xargs -r kill -CONT",
                allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m("Failed to resume redpanda",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise

    def io_ruin(self):
        result = None
        try:
            result = self.node.account.ssh_output(
                f"curl -s 127.0.0.1:{MountMuService.IOFAULT_PORT}/ruin",
                allow_fail=False)
            result = json.loads(result)
            if result["status"] != "ok":
                raise Exception("Failed to ruin io: expected status=ok got: " +
                                json.dumps(result))
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m("Failed to ruin io",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise

    def io_recover(self):
        try:
            result = self.node.account.ssh_output(
                f"curl -s 127.0.0.1:{MountMuService.IOFAULT_PORT}/recover",
                allow_fail=False)
            result = json.loads(result)
            if result["status"] != "ok":
                raise Exception(
                    "Failed to recover io: expected status=ok got: " +
                    json.dumps(result))
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m("Failed to recover io",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise

    def io_delay(self, delay_ms):
        try:
            result = self.node.account.ssh_output(
                f"curl -s 127.0.0.1:{MountMuService.IOFAULT_PORT}/delay/{delay_ms}",
                allow_fail=False)
            result = json.loads(result)
            if result["status"] != "ok":
                raise Exception(
                    "Failed to recover io: expected status=ok got: " +
                    json.dumps(result))
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m("Failed to recover io",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise

    def isolate(self, ips):
        cmd = []
        for ip in ips:
            cmd.append(f"sudo iptables -A INPUT -s {ip} -j DROP")
            cmd.append(f"sudo iptables -A OUTPUT -d {ip} -j DROP")
        cmd = " && ".join(cmd)

        try:
            self.node.account.ssh_output(cmd, allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m("Failed to recover io",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise

    def rejoin(self, ips):
        cmd = []
        for ip in ips:
            cmd.append(f"sudo iptables -D INPUT -s {ip} -j DROP")
            cmd.append(f"sudo iptables -D OUTPUT -d {ip} -j DROP")
        cmd = " && ".join(cmd)

        try:
            self.node.account.ssh_output(cmd, allow_fail=False)
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            errors_log.info(
                m("Failed to recover io",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace))
            raise


class RedpandaDuckCluster:
    def __init__(self, service, redpanda_mu):
        self.service = service
        self.redpanda_mu = redpanda_mu
        nodes = [
            RedpandaDuckNode(service, redpanda_mu, node)
            for node in service.nodes
        ]
        self.nodes = {node.node_id: node for node in nodes}

    def get_leader(self):
        meta = self.any_node().meta()
        meta = json.loads(meta)

        try:
            partitions = next(x["partitions"] for x in meta["topics"]
                              if x["topic"] == "topic1")
            leader_id = next(x["leader"] for x in partitions
                             if x["partition"] == 0)
            leader = next(x["name"] for x in meta["brokers"]
                          if x["id"] == leader_id)
            for node_id in self.nodes.keys():
                if node_id in leader:
                    return self.nodes[node_id]
        except:
            raise Exception("Can't find a leader")

        raise Exception("Can't find a leader")

    def get_follower(self):
        meta = self.any_node().meta()
        meta = json.loads(meta)

        try:
            partitions = next(x["partitions"] for x in meta["topics"]
                              if x["topic"] == "topic1")
            leader_id = next(x["leader"] for x in partitions
                             if x["partition"] == 0)
            leader = next(x["name"] for x in meta["brokers"]
                          if x["id"] == leader_id)
            for node_id in self.nodes.keys():
                if node_id not in leader:
                    return self.nodes[node_id]
        except:
            raise Exception("Can't find a follower")

        raise Exception("Can't find a follower")

    def any_node(self):
        return next(iter(self.nodes.items()))[1]
