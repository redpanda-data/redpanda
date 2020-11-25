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

from gobekli.kvapi import KVNode, RequestTimedout, RequestCanceled, RequestViolated
from gobekli.logging import m
import logging
import asyncio
import urllib.request
import json
import uuid

chaos_event_log = logging.getLogger("chaos-event")


class KvelldbNode:
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

    def mount(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["mount_script"])

    def umount(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["umount_script"])

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

    def io_ruin(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["io_ruin_script"])
        # todo check status code

    def io_delay(self, delay_ms):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["io_delay_script"], delay_ms)
        # todo check status code

    def io_recover(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["io_recover_script"])
        # todo check status code


chaos_stdout = logging.getLogger("chaos-stdout")


class KvelldbCluster:
    def __init__(self, config):
        self.config = config
        self.nodes = {
            config_node["id"]: KvelldbNode(config, config_node["id"])
            for config_node in config["nodes"]
        }

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.config["destroy_on_exit"]:
            self.teardown()
        return False

    def teardown(self):
        self._strobe_api_kill()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(
                m(f"terminating kvelldb on {node_id}").with_time())
            node.kill()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            if node.is_service_running():
                chaos_event_log.info(
                    m(f"kvelldb on {node_id} is still running").with_time())
                raise Exception(f"kvelldb on {node_id} is still running")

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
        self._strobe_api_start()
        self._strobe_recover()
        chaos_stdout.info("cluster started")
        chaos_stdout.info("")

    async def is_ok(self):
        is_ok = False
        for endpoint in self.config["endpoints"]:
            host = endpoint["host"]
            port = endpoint["httpport"]
            address = f"{host}:{port}"
            kv = KVNode(address, address)
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
        for endpoint in self.config["endpoints"]:
            host = endpoint["host"]
            port = endpoint["httpport"]
            address = f"{host}:{port}"
            read_id = str(uuid.uuid1())
            try:
                with urllib.request.urlopen(
                        f"http://{address}/read?key=leader&read_id={read_id}"
                ) as response:
                    data = json.loads(response.read())
                    if data["status"] == "ok":
                        return self.nodes[endpoint["id"]]
            except:
                continue
        return None

    def _mount(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(m(f"mount data dir on {node_id}").with_time())
            node.mount()

    def _start_service(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(
                m(f"starting kvelldb on {node_id}").with_time())
            node.start_service()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            if not node.is_service_running():
                chaos_event_log.info(
                    m(f"kvelldb isn't running on {node_id}").with_time())
                raise Exception(f"kvelldb on {node_id} isn't running")

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
