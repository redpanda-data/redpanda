from sh import ssh
import sh
import time
import json
import asyncio

from gobekli.kvapi import KVNode, RequestTimedout, RequestCanceled
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

    def start_service(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["start_script"])
        # TODO: make something smarted I feel guilty for sleep
        time.sleep(2)

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

    def create_topic(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["create_topic_script"])

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


class RedpandaCluster:
    def __init__(self, config):
        self.config = config
        self.nodes = {
            config_node["id"]: RedpandaNode(config, config_node["id"])
            for config_node in config["nodes"]
        }

    async def restart(self):
        chaos_stdout.info("(re)starting a cluster")
        self.kill_api()
        self.rm_api_log()
        self.terminate_wipe_restart()
        cluster_warmup = self.config["cluster_warmup"]
        await asyncio.sleep(cluster_warmup)
        chaos_stdout.info("cluster started")
        chaos_stdout.info("creating topic")
        node = self.any_node()
        node.create_topic()
        chaos_stdout.info("topic created")
        self.start_api()
        # TODO: make something smarted I feel guilty for sleep
        time.sleep(2)
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
                # TODO add logging
                pass
            except RequestCanceled:
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

    def terminate_wipe_restart(self):
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

        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(m(f"mount data dir on {node_id}").with_time())
            node.mount()

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

    def start_api(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.start_api()

    def kill_api(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.kill_api()

    def rm_api_log(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.rm_api_log()
