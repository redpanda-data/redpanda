from sh import ssh
import sh

from gobekli.kvapi import KVNode, RequestTimedout, RequestCanceled
from gobekli.logging import m
import logging

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


class KvelldbCluster:
    def __init__(self, config):
        self.config = config
        self.nodes = {
            config_node["id"]: KvelldbNode(config, config_node["id"])
            for config_node in config["nodes"]
        }

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

    async def get_leader(self):
        leader_id = None
        for endpoint in self.config["endpoints"]:
            host = endpoint["host"]
            port = endpoint["httpport"]
            address = f"{host}:{port}"
            kv = KVNode(address, address)
            try:
                await kv.put_aio("test", "value1", "wid1")
                leader_id = endpoint["id"]
            except RequestTimedout:
                pass
            except RequestCanceled:
                pass
            await kv.close_aio()
            if leader_id != None:
                return self.nodes[leader_id]
        return None

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
