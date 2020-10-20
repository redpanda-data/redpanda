import time
import pathlib
import os

import traceback
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
        tools.create_topic(KafkaKVMuService.TOPIC)

    def tearDown(self):
        self.service.stop()

    def run(self, failure_factory):
        cluster = RedpandaDuckCluster(self.service, self.redpanda_mu)
        workload_factory = lambda: MRSWWorkload(
            list(map(lambda x: KVNode(x, x), self.kafkakv_mu.endpoints)), 3, 3,
            [])

        asyncio.run(
            inject_recover_scenario_aio(BaseChaosTest.PERSISTENT_ROOT,
                                        self.config, cluster, workload_factory,
                                        failure_factory))
