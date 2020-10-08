from gobekli.kvapi import KVNode
from gobekli.workloads.symmetrical_mrsw import MRSWWorkload
from gobekli.chaos.main import (init_output, inject_recover_scenarios_aio)
from gobekli.logging import m
from chaostest.faults import *
from chaostest.kvell_cluster import KvelldbCluster
import logging
import asyncio
import argparse
import json

chaos_event_log = logging.getLogger("chaos-event")
chaos_stdout = logging.getLogger("chaos-stdout")


async def select_leader(cluster):
    node = await cluster.get_leader()
    if node == None:
        chaos_event_log.info(m("can't detect a leader").with_time())
    return node


async def select_follower(cluster):
    leader = await cluster.get_leader()
    for node_id in cluster.nodes.keys():
        if node_id != leader.node_id:
            return cluster.nodes[node_id]
    return None


known_faults = {
    "baseline":
    lambda: BaselineRecoverableFault(),
    "terminate.leader":
    lambda: TerminateNodeRecoverableFault(select_leader, "leader"),
    "terminate.follower":
    lambda: TerminateNodeRecoverableFault(select_follower, "follower"),
    "suspend.leader":
    lambda: SuspendServiceRecoverableFault(select_leader, "leader"),
    "suspend.follower":
    lambda: SuspendServiceRecoverableFault(select_follower, "follower"),
    "isolate.leader":
    lambda: IsolateNodeRecoverableFault(select_leader, "leader"),
    "isolate.follower":
    lambda: IsolateNodeRecoverableFault(select_follower, "follower"),
    "io10ms.leader":
    lambda: MakeIOSlowerRecoverableFault(select_leader, "leader"),
    "io10ms.follower":
    lambda: MakeIOSlowerRecoverableFault(select_follower, "follower"),
    "iofail.leader":
    lambda: RuinIORecoverableFault(select_leader, "leader"),
    "iofail.follower":
    lambda: RuinIORecoverableFault(select_follower, "follower")
}


def workload_factory(config):
    nodes = []
    for endpoint in config["endpoints"]:
        host = endpoint["host"]
        port = endpoint["httpport"]
        address = f"{host}:{port}"
        nodes.append(KVNode(endpoint["id"], address))
    return MRSWWorkload(nodes, config["writers"], config["readers"],
                        config["ss_metrics"])


async def run(config, n, overrides):
    init_output(config)

    if overrides:
        for overide in overrides:
            [key, value] = overide.split("=", 1)
            config[key] = json.loads(value)

    faults = {fault: known_faults[fault] for fault in config["faults"]}

    cluster = KvelldbCluster(config)
    try:
        for _ in range(0, n):
            if not config["reset_before_test"]:
                await cluster.restart()
                if not await cluster.is_ok():
                    chaos_event_log.info(
                        m(f"cluster isn't healthy").with_time())
                    raise Exception(f"cluster isn't healthy")
            await inject_recover_scenarios_aio(
                config, cluster, faults, lambda: workload_factory(config))
    except ViolationInducedExit:
        pass
    cluster.teardown()


parser = argparse.ArgumentParser(description='chaos test kvelldb')
parser.add_argument('config')
parser.add_argument('--override', action='append', required=False)
parser.add_argument('--repeat', type=int, default=1, required=False)

args = parser.parse_args()

config = None

with open(args.config, "r") as settings_json:
    config = json.load(settings_json)

asyncio.run(run(config, args.repeat, args.override))
