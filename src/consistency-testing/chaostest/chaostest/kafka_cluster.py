# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from sh import (ssh, scp)
import sh
import time
import json
import sys
import asyncio
import aiohttp

from gobekli.kvapi import KVNode, RequestTimedout, RequestCanceled, RequestViolated
from gobekli.logging import m
import logging
import jinja2
from tempfile import NamedTemporaryFile
import os

chaos_event_log = logging.getLogger("chaos-event")

KAFKA_CONFIG = """
broker.id={{ id }}
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs={{ logDirs }}
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
log.flush.interval.messages=1
log.flush.interval.ms=0
min.insync.replicas=2
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect={{ zookeeper }}
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
listeners=PLAINTEXT://{{ ip }}
advertised.listeners=PLAINTEXT://{{ ip }}
"""

ZOOKEEPER_CONFIG = """
tickTime=2000
initLimit=10
syncLimit=5
dataDir={{ dataDir }}
clientPort=2181
server.1={{ server1 }}
server.2={{ server2 }}
server.3={{ server3 }}
"""


class KafkaNode:
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

    def write_zookeeper_configs(self):
        f = NamedTemporaryFile(delete=False)
        f.write(
            jinja2.Template(ZOOKEEPER_CONFIG).render(
                self.node_config["zookeeper_config"]).encode("utf-8"))
        f.close()
        scp(
            "-i", self.node_config["ssh_key"], f.name,
            self.node_config["ssh_user"] + "@" + self.node_config["host"] +
            ":zoo.cfg")
        os.unlink(f.name)
        path = self.node_config["zookeeper_config"]["path"]
        myid_path = self.node_config["zookeeper_config"]["dataDir"] + "/myid"
        myid = self.node_config["zookeeper_config"]["id"]
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            f"sudo mv zoo.cfg {path}")
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            f"sudo bash -c \"echo {myid} > {myid_path}\"")

    def write_kafka_config(self):
        f = NamedTemporaryFile(delete=False)
        f.write(
            jinja2.Template(KAFKA_CONFIG).render(
                self.node_config["kafka_config"]).encode("utf-8"))
        f.close()
        scp(
            "-i", self.node_config["ssh_key"], f.name,
            self.node_config["ssh_user"] + "@" + self.node_config["host"] +
            ":kafka.cfg")
        os.unlink(f.name)
        path = self.node_config["kafka_config"]["path"]
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            f"sudo mv kafka.cfg {path}")

    def meta(self):
        return ssh(
            "-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["meta_script"]).stdout

    def mount(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["mount_script"])

    def umount(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["umount_script"])

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

    def isolate(self, ips):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["isolate_script"], *ips)

    def rejoin(self, ips):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["rejoin_script"], *ips)

    def kill(self):
        self.kill_kafka()
        self.kill_zookeeper()

    def start_service(self):
        self.start_zookeeper()
        self.start_kafka()

    def is_service_running(self):
        result = ssh(
            "-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["is_active_script"])
        return "YES" in result

    def pause_service(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["pause_script"])

    def continue_service(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["continue_script"])

    def start_kafka(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["start_kafka_script"])

    def kill_kafka(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["kill_kafka_script"])

    def start_zookeeper(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["start_zookeeper_script"])

    def kill_zookeeper(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["kill_zookeeper_script"])

    def create_topic(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["create_topic_script"])

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

    def prep_dirs(self):
        ssh("-i", self.node_config["ssh_key"],
            self.node_config["ssh_user"] + "@" + self.node_config["host"],
            self.node_config["prepdirs_script"])


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


class KafkaCluster:
    def __init__(self, config):
        self.config = config
        self.nodes = {
            config_node["id"]: KafkaNode(config, config_node["id"])
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
                m(f"terminating kafka on {node_id}").with_time())
            node.kill_kafka()
            chaos_event_log.info(
                m(f"terminating zookeeper on {node_id}").with_time())
            node.kill_zookeeper()

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

        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(m(f"preparing dirs {node_id}").with_time())
            node.prep_dirs()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.write_zookeeper_configs()
            node.write_kafka_config()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.start_zookeeper()

        for node_id in self.nodes:
            node = self.nodes[node_id]
            node.start_kafka()

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
                chaos_event_log.info(
                    m(f"put request to {address} timed out").with_time())
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

    def _start_api(self):
        for node_id in self.endpoints:
            node = self.endpoints[node_id]
            node.start_api()

    def _kill_api(self):
        for node_id in self.endpoints:
            node = self.endpoints[node_id]
            node.kill_api()

    def _rm_api_log(self):
        for node_id in self.endpoints:
            node = self.endpoints[node_id]
            node.rm_api_log()

    def _mount(self):
        for node_id in self.nodes:
            node = self.nodes[node_id]
            chaos_event_log.info(m(f"mount data dir on {node_id}").with_time())
            node.mount()

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
