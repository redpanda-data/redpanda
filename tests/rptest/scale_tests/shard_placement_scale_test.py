# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.tests.prealloc_nodes import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.services.redpanda import SISettings, LoggingConfig
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations


class ShardPlacementScaleTest(RedpandaTest):
    def __init__(self, ctx, *args, **kwargs):
        si_settings = SISettings(test_context=ctx)
        super().__init__(
            *args,
            test_context=ctx,
            num_brokers=5,
            si_settings=si_settings,
            # trace logging kills preformance, so we run with info level.
            log_config=LoggingConfig('info'),
            **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    def enable_feature(self):
        self.redpanda.set_feature_active("node_local_core_assignment",
                                         active=True)

    def start_omb(self):
        producer_rate_mbps = 100

        workload = {
            "name": "CommonWorkload",
            "topics": 1,
            "partitions_per_topic": 1000,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 25,
            "producers_per_topic": 5,
            "producer_rate": producer_rate_mbps * 1024,
            "message_size": 1024,
            "payload_file": "payload/payload-1Kb.data",
            "key_distributor": "KEY_ROUND_ROBIN",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 3,
            "warmup_duration_minutes": 1,
        }
        driver = {
            "name": "CommonWorkloadDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "topic_config": {
                "cleanup.policy": "compact,delete",
            },
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                # producer settings to ensure batches of decent size
                # (otherwise most of the batches contain just 1 message
                # which leads to unreasonably high reactor util)
                "max.request.size": 5 * 2**20,
                "linger.ms": 500,
                "max.in.flight.requests.per.connection": 1,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "max.partition.fetch.bytes": 131072
            },
        }
        validator = {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS:
            [OMBSampleConfigurations.gte(producer_rate_mbps)]
        }

        self._benchmark = OpenMessagingBenchmark(ctx=self.test_context,
                                                 redpanda=self.redpanda,
                                                 driver=driver,
                                                 workload=(workload,
                                                           validator),
                                                 topology="ensemble")
        self._benchmark.start()
        self.logger.info(f"OMB started")

    def omb_topics(self):
        rpk = RpkTool(self.redpanda)
        return [t for t in rpk.list_topics() if t.startswith('test-topic-')]

    def finish_omb(self):
        benchmark_time_min = self._benchmark.benchmark_time() + 2
        self._benchmark.wait(timeout_sec=benchmark_time_min * 60)
        self._benchmark.check_succeed()

    @cluster(num_nodes=8)
    @skip_debug_mode
    def test_manual_moves(self):
        self.redpanda.start()
        self.enable_feature()

        self.start_omb()

        time.sleep(100)

        # trigger random xshard moves for 20% of partitions on the first node

        admin = Admin(self.redpanda)

        node = self.redpanda.nodes[0]
        node_id = self.redpanda.node_id(node)
        n_cores = self.redpanda.get_node_cpu_count()

        omb_topic = self.omb_topics()[0]

        partitions = [
            p["partition_id"] for p in admin.get_partitions(node=node)
            if p["topic"] == omb_topic
        ]
        partitions = random.sample(partitions, len(partitions) // 5)

        self.logger.info(
            f"moving {len(partitions)} partitions of topic {omb_topic}"
            f" on node {node.name}")
        for partition in partitions:
            core = random.randrange(n_cores)
            self.logger.info(f"moving {omb_topic}/{partition} to core {core}")
            admin.set_partition_replica_core(omb_topic,
                                             partition,
                                             node_id,
                                             core=core)
        self.logger.info("finished moving")

        time.sleep(100)
        admin.trigger_cores_rebalance(node)
        self.logger.info("triggered manual rebalance")

        self.finish_omb()

    @cluster(num_nodes=8)
    @skip_debug_mode
    def test_node_add(self):
        self.redpanda.add_extra_rp_conf({
            "core_balancing_continuous": True,
        })

        seed_nodes = self.redpanda.nodes[0:4]
        joiner_nodes = self.redpanda.nodes[4:]

        self.redpanda.start(nodes=seed_nodes)
        self.enable_feature()

        self.start_omb()

        time.sleep(120)
        self.redpanda.start(nodes=joiner_nodes)
        self.logger.info(
            f"added nodes {[n.name for n in joiner_nodes]} to the cluster")

        self.finish_omb()

        # check that the node rebalance was finished

        admin = Admin(self.redpanda)

        assert len(admin.list_reconfigurations()) == 0

        omb_topic = self.omb_topics()[0]
        for node in joiner_nodes:
            partitions = [
                p for p in admin.get_partitions(node=node)
                if p["topic"] == omb_topic
            ]
            assert len(partitions) > 0
