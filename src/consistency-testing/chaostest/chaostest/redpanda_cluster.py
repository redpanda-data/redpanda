# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from sh import ssh
import sh
import time
import json
import sys
import asyncio
import aiohttp
import uuid

from gobekli.kvapi import KVNode, RequestTimedout, RequestCanceled, RequestViolated
from gobekli.logging import m
import logging

chaos_event_log = logging.getLogger("chaos-event")


class RedpandaNode:
    def __init__(self, config, node_id):
        self.config = config
        self.node_id = node_id
        self.node_config = None
        for node in config["nodes"]:
            if node["id"] == node_id:
                self.node_config = node
        if self.node_config == None:
            raise Exception(f"Unknown node_id: {node_id}")
        self.ip = self.node_config["host"]

    def meta(self):
        return ssh(
            "-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["meta_script"]).stdout

    def kill(self):
        try:
            ssh("-i", self.node_config["ssh_key"],
                self.node_config["ssh_user"] + "@" + self.node_config["host"],
                self.node_config["kill_script"])
        except sh.ErrorReturnCode:
            pass

    def is_service_running(self):
        result = ssh(
            "-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["is_active_script"])
        return "YES" in result

    def mount(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["mount_script"])

    def umount(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["umount_script"])

    def wipe_out(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["wipeout_script"])

    def strobe_start(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["strobe_start_api_script"])

    def strobe_kill(self):
        try:
            ssh("-i", self.node_config["ssh_key"],
                self.node_config["ssh_user"] + "@" + self.node_config["host"],
                self.node_config["strobe_kill_api_script"])
        except sh.ErrorReturnCode:
            pass

    def strobe_inject(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["strobe_inject_script"])

    def strobe_recover(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["strobe_recover_script"])

    def start_service(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["start_script"])

    def pause_service(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["pause_script"])

    def continue_service(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["continue_script"])

    def isolate(self, ips):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["isolate_script"], *ips)

    def rejoin(self, ips):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["rejoin_script"], *ips)

    def create_topic(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["create_topic_script"])

    def io_ruin(self, op_name):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["io_ruin_script"], op_name)
        # todo check status code

    def io_delay(self, op_name, delay_ms):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["io_delay_script"], op_name, delay_ms)
        # todo check status code

    def io_recover(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["io_recover_script"])
        # todo check status code


class EndpointNode:
    def __init__(self, config, node_id):
        self.config = config
        self.node_id = node_id
        self.node_config = None
        for node in config["endpoints"]:
            if node["id"] == node_id:
                self.node_config = node
        if self.node_config == None:
            raise Exception(f"Unknown node_id: {node_id}")
        self.ip = self.node_config["host"]

    def start_api(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["start_api_script"])

    def kill_api(self):
        try:
            ssh("-i", self.node_config["ssh_key"],
                self.node_config["ssh_user"] + "@" + self.node_config["host"],
                self.node_config["kill_api_script"])
        except sh.ErrorReturnCode:
            pass

    def rm_api_log(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["rm_api_log_script"])


chaos_stdout = logging.getLogger("chaos-stdout")


class RedpandaCluster:
    def __init__(self, config):
        self.config = config
        self.nodes = {
            config_node["id"]: RedpandaNode(config, config_node["id"])
            for config_node in config["nodes"]
        }
        self.endpoints = {
            config_node["id"]: EndpointNode(config, config_node["id"])
            for config_node in config["endpoints"]
        }

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.config["destroy_on_exit"]:
            self.teardown()
        return False

    def teardown(self):
        self._kill_api()
        self._strobe_api_kill()
        self._rm_api_log()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(
                m(f"terminating redpanda on {node_id}").with_time())
            node.kill()

        # TODO: add several checks (kill -9 may be async)
        for node_id in self.nodes:
            node = self.nodes[node_id]
            if node.is_service_running():
                chaos_event_log.info(
                    m(f"redpanda on {node_id} is still running").with_time())
                raise Exception(f"redpanda on {node_id} is still running")

        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(
                m(f"umount data dir on {node_id}").with_time())
            node.umount()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(m(f"removing data on {node_id}").with_time())
            node.wipe_out()

    async def restart(self):
        chaos_stdout.info("(re)starting a cluster")
        self.teardown()
        self._mount()
        self._start_service()
        cluster_warmup = self.config["cluster_warmup"]
        await asyncio.sleep(cluster_warmup)
        chaos_stdout.info("cluster started")
        chaos_stdout.info("creating topic")
        node = self.any_node()
        node.create_topic()
        chaos_stdout.info("topic created")
        self._start_api()
        self._strobe_api_start()
        self._strobe_recover()
        # TODO: Replace sleep with an explicit check waiting for kafkakv & strobe
        # services to start
        time.sleep(2)

        chaos_stdout.info("")

    async def is_ok(self):
        is_ok = False
        for endpoint in self.config["endpoints"]:
            host = endpoint["host"]
            port = endpoint["httpport"]
            address = f"{host}:{port}"
            kv = KVNode(endpoint["idx"], address, address)
            try:
                await kv.put_aio("test", "value1", "wid1")
                is_ok = True
            except RequestTimedout:
                pass
            except RequestCanceled:
                pass
            except RequestViolated:
                pass
            await kv.close_aio()
            if is_ok:
                return True
        return is_ok

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
            for config_node in self.config["nodes"]:
                if config_node["host"] in leader:
                    return self.nodes[config_node["id"]]
            raise Exception("Can't find leader")
        except:
            raise Exception("Can't find leader")

    def any_node(self):
        for node_id in self.nodes:
            return self.nodes[node_id]

    def _mount(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(m(f"mount data dir on {node_id}").with_time())
            node.mount()

    def _start_service(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(
                m(f"starting redpanda on {node_id}").with_time())
            node.start_service()

        attempts = 2
        while True:
            attempts -= 1
            time.sleep(5)
            running = True
            for node_id in self.nodes:
                node = self.nodes[node_id]
                if not node.is_service_running():
                    running = False
                    chaos_event_log.info(
                        m(f"redpanda on {node_id} isn't running").with_time())
                    if attempts < 0:
                        raise Exception(f"redpanda on {node_id} isn't running")
            if running:
                break

    def _start_api(self):
        for node_id in self.endpoints:
            node = self.endpoints[node_id]
            node.start_api()

    def _kill_api(self):
        for node_id in self.endpoints:
            node = self.endpoints[node_id]
            node.kill_api()

    def _strobe_api_kill(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.strobe_kill()

    def _strobe_api_start(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.strobe_start()

    def _strobe_recover(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.strobe_recover()

    def _rm_api_log(self):
        for node_id in self.endpoints:
            node = self.endpoints[node_id]
            node.rm_api_log()
