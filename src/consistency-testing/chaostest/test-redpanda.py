# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from gobekli.kvapi import KVNode
from gobekli.workloads.symmetrical_mrsw import MRSWWorkload
from gobekli.workloads.symmetrical_comrmw import COMRMWWorkload
from gobekli.chaos.main import (init_output, inject_recover_scenarios_aio,
                                ViolationInducedExit)
from gobekli.logging import m
from chaostest.faults import *
from chaostest.redpanda_cluster import RedpandaCluster
import logging
import asyncio
import argparse
import json
import time
import sys
import shutil
from os import path

chaos_event_log = logging.getLogger("chaos-event")
chaos_stdout = logging.getLogger("chaos-stdout")


def select_leader(cluster):
    try:
        return cluster.get_leader()
    except:
        chaos_event_log.info(m("can't detect a leader").with_time())
        raise


def select_follower(cluster):
    leader = cluster.get_leader()
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
    "fsync10ms.leader":
    lambda: MakeFsyncSlowerRecoverableFault(select_leader, "leader"),
    "fsync10ms.follower":
    lambda: MakeFsyncSlowerRecoverableFault(select_follower, "follower"),
    "io10ms.leader":
    lambda: MakeIOSlowerRecoverableFault(select_leader, "leader"),
    "io10ms.follower":
    lambda: MakeIOSlowerRecoverableFault(select_follower, "follower"),
    "iofail.leader":
    lambda: RuinIORecoverableFault(select_leader, "leader"),
    "iofail.follower":
    lambda: RuinIORecoverableFault(select_follower, "follower"),
    "strobe.leader":
    lambda: StrobeRecoverableFault(select_leader, "leader"),
    "strobe.follower":
    lambda: StrobeRecoverableFault(select_follower, "follower")
}


def workload_factory(config):
    nodes = []
    for endpoint in config["endpoints"]:
        host = endpoint["host"]
        port = endpoint["httpport"]
        address = f"{host}:{port}"
        nodes.append(KVNode(endpoint["idx"], endpoint["id"], address))
    if config["workload"]["name"] == "mrsw":
        return MRSWWorkload(nodes, config["writers"], config["readers"],
                            config["ss_metrics"])
    elif config["workload"]["name"] == "comrmw":
        return COMRMWWorkload(config["workload"]["period_s"], nodes,
                              config["writers"], config["readers"],
                              config["ss_metrics"])
    else:
        raise Exception("Unknown workload: " + config["workload"]["name"])


async def run(config_json, n, overrides):
    suite_id = int(time.time())

    with open(config_json, "r") as settings_json:
        config = json.load(settings_json)

    init_output(config, suite_id)

    shutil.copyfile(
        config_json, path.join(config["output"], str(suite_id),
                               "settings.json"))

    if overrides:
        for overide in overrides:
            [key, value] = overide.split("=", 1)
            config[key] = json.loads(value)

    faults = {fault: known_faults[fault] for fault in config["faults"]}

    with RedpandaCluster(config) as cluster:
        try:
            for _ in range(0, n):
                if not config["reset_before_test"]:
                    await cluster.restart()
                    if not await cluster.is_ok():
                        chaos_event_log.info(
                            m(f"cluster isn't healthy").with_time())
                        raise Exception(f"cluster isn't healthy")
                await inject_recover_scenarios_aio(
                    suite_id, config, cluster, faults,
                    lambda: workload_factory(config))
        except ViolationInducedExit:
            pass


parser = argparse.ArgumentParser(description='chaos test redpanda')
parser.add_argument('config')
parser.add_argument('--override', action='append', required=False)
parser.add_argument('--repeat', type=int, default=1, required=False)

args = parser.parse_args()

asyncio.run(run(args.config, args.repeat, args.override))
