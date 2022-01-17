# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import pathlib

import sys

sys.path.append("/opt/v/src/consistency-testing/gobekli")
sys.path.append("/opt/v/src/consistency-testing/chaostest")

import gobekli
import asyncio

from gobekli.kvapi import KVNode

from gobekli.chaos.main import inject_recover_scenario_aio
from gobekli.workloads import symmetrical_mrsw
from gobekli.workloads.symmetrical_mrsw import MRSWWorkload

from ducktape.tests.test import Test
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.chaos.mount_muservice import MountMuService
from rptest.chaos.redpanda_muservice import (RedpandaMuService,
                                             RedpandaMuServiceServiceProxy)
from rptest.chaos.kafkakv_muservice import KafkaKVMuService
from rptest.chaos.muservice import (MuServiceRunner, SeqMuService)

from rptest.chaos.redpanda_duck_cluster import RedpandaDuckCluster


class BaseChaosTest(Test):
    PERSISTENT_ROOT = "/mnt/chaos"

    def __init__(self, test_context, num_nodes):
        super(BaseChaosTest, self).__init__(test_context)
        self.num_nodes = num_nodes
        self.config = {
            "cmd_log": "cmd.log",
            "latency_log": "lat.log",
            "availability_log": "1s.log",
            "ss_metrics": [],
            "verbose": True,
            "warmup": 60,
            "exploitation": 60,
            "cooldown": 120,
            "workload": {
                "name": "mrsw"
            }
        }
        pathlib.Path(BaseChaosTest.PERSISTENT_ROOT).mkdir(parents=True,
                                                          exist_ok=True)

    def setUp(self):
        self.mount_mu = MountMuService()
        self.redpanda_mu = RedpandaMuService()
        self.kafkakv_mu = KafkaKVMuService(self.redpanda_mu)
        seq = SeqMuService([self.mount_mu, self.redpanda_mu, self.kafkakv_mu])
        self.service = MuServiceRunner(seq, self.test_context, self.num_nodes)
        self.service.start()
        redpanda = RedpandaMuServiceServiceProxy(self.service,
                                                 self.redpanda_mu)
        tools = KafkaCliTools(redpanda, KafkaCliTools.VERSIONS[0])
        tools.create_topic(TopicSpec(name=KafkaKVMuService.TOPIC))

    def tearDown(self):
        self.service.stop()

    def run(self, failure_factory):
        cluster = RedpandaDuckCluster(self.service, self.redpanda_mu)

        def workload_factory():
            nodes = []
            for x in self.kafkakv_mu.endpoints:
                nodes.append(KVNode(len(nodes), x, x))
            return MRSWWorkload(nodes, 3, 3, [])

        asyncio.run(
            inject_recover_scenario_aio(BaseChaosTest.PERSISTENT_ROOT,
                                        self.config, cluster, workload_factory,
                                        failure_factory))
