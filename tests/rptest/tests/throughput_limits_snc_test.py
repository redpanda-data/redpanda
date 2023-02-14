# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
import random
import time
from collections import Counter
from enum import Enum
from typing import Tuple

from ducktape.mark import parametrize
from ducktape.tests.test import TestContext
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest

# This file is about throughput limiting that works at shard/node/cluster (SNC)
# levels, like cluster-wide and node-wide throughput limits


class ThroughputLimitsSncBase(RedpandaTest):
    """
    Tests for throughput limiting that works at shard/node/cluster (SNC)
    levels, like cluster-wide and node-wide throughput limits
    """
    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        super(ThroughputLimitsSncBase, self).__init__(test_ctx,
                                                      num_brokers=3,
                                                      *args,
                                                      **kwargs)
        rnd_seed_override = test_ctx.globals.get("random_seed")
        if rnd_seed_override is None:
            # default seed value is composed from
            # - current time - the way the default generator ctor does
            # - hash of the redpanda service instance - to avoid possible
            #   identical seeds while running multiple tests in parallel
            #   with --repeat
            self.rnd_seed = int(time.time() * 1_000_000_000) + hash(
                self.redpanda.service_id)
        else:
            self.rnd_seed = rnd_seed_override
        self.logger.info(f"Random seed: {self.rnd_seed}")
        self.rnd = random.Random(self.rnd_seed)

        self.rpk = RpkTool(self.redpanda)

    class ConfigProp(Enum):
        QUOTA_NODE_MAX_IN = "kafka_throughput_limit_node_in_bps"
        QUOTA_NODE_MAX_EG = "kafka_throughput_limit_node_out_bps"
        THROTTLE_DELAY_MAX_MS = "max_kafka_throttle_delay_ms"
        BAL_WINDOW_MS = "kafka_quota_balancer_window_ms"
        BAL_PERIOD_MS = "kafka_quota_balancer_node_period_ms"
        QUOTA_SHARD_MIN_RATIO = "kafka_quota_balancer_min_shard_throughput_ratio"
        QUOTA_SHARD_MIN_BPS = "kafka_quota_balancer_min_shard_throughput_bps"

    def current_effective_node_quota(self) -> Tuple[int, int]:
        metrics = self.redpanda.metrics_sample(
            "quotas_quota_effective", metrics_endpoint=MetricsEndpoint.METRICS)

        assert metrics, "Effecive quota metric is missing"
        self.logger.debug(f"Samples: {metrics.samples}")

        node_quota_in = sum(
            int(s.value) for s in metrics.label_filter({
                "direction": "ingress"
            }).samples)
        node_quota_eg = sum(
            int(s.value) for s in metrics.label_filter({
                "direction": "egress"
            }).samples)
        return node_quota_in, node_quota_eg


class ThroughputLimitsSncConfiguration(ThroughputLimitsSncBase):
    """
    Tests for throughput limiting that works at shard/node/cluster (SNC)
    levels with randomized initial cluster configuration
    """
    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        super(ThroughputLimitsSncConfiguration,
              self).__init__(test_ctx, *args, **kwargs)

        self.config = {}
        for prop in list(self.ConfigProp):
            val = self.get_config_parameter_random_value(prop)
            self.config[prop] = val
            if not val is None:
                self.redpanda.add_extra_rp_conf({prop.value: val})

        # Moving partition leadership around will make throughput assessment
        # too complicated by the test
        self.redpanda.add_extra_rp_conf({"enable_leader_balancer": False})
        self.logger.info(
            f"Initial cluster props: {self.redpanda._extra_rp_conf}")

    def get_config_parameter_random_value(
            self, prop: ThroughputLimitsSncBase.ConfigProp):
        if prop in [
                self.ConfigProp.QUOTA_NODE_MAX_IN,
                self.ConfigProp.QUOTA_NODE_MAX_EG
        ]:
            r = self.rnd.randrange(4)
            if r == 0:
                return None
            if r == 1:
                return 64  # practical minimum
            return math.floor(2**(self.rnd.random() * 35 + 5))  # up to 1 TB/s

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_BPS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return math.floor(2**(self.rnd.random() * 30))  # up to 1 GB/s

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_RATIO:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return 1
            return self.rnd.random()

        if prop == self.ConfigProp.THROTTLE_DELAY_MAX_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return math.floor(2**(self.rnd.random() * 25))  # up to ~1 year

        if prop == self.ConfigProp.BAL_WINDOW_MS:
            r = self.rnd.randrange(4)
            if r == 0:
                return 1
            if r == 1:
                return 2147483647
            return math.floor(2**(self.rnd.random() * 31))

        if prop == self.ConfigProp.BAL_PERIOD_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return math.floor(2**(self.rnd.random() * 22))  # up to ~1.5 months

        raise Exception(f"Unsupported ConfigProp: {prop}")

    @cluster(num_nodes=3)
    def test_configuration(self):
        """
        Test various configuration patterns, including extreme ones,
        verify that it does not wreck havoc onto cluster
        """

        # TBD: parameterize to run under load or not

        errors = []
        # 12 passes approximately take 30 secs on a 3-node cluster
        for _ in range(12):

            change = {}
            for _ in range(self.rnd.randrange(4)):
                config_param = self.rnd.choice(list(self.ConfigProp))
                config_value = self.get_config_parameter_random_value(
                    config_param)
                self.config[config_param] = config_value
                change[config_param.value] = config_value
            self.logger.info(f"Changing cluster prop: {change}")
            self.redpanda.set_cluster_config(change)

            # set_cluster_config waits for the prop to be replicated
            # it takes time so no need for a sleep()

            def check_node_quota_metric(self, config_prop: self.ConfigProp,
                                        effective_node_quota: int) -> str:
                config_val = self.config[config_prop]
                if config_val is None:
                    return
                expected_node_quota = config_val * len(self.redpanda.nodes)
                if effective_node_quota == expected_node_quota:
                    return
                e = (
                    f"Expected quota value mismatch. "
                    f"Effective {effective_node_quota} != expected {expected_node_quota}. "
                    f"Direction: {config_prop.name}")
                self.logger.error(e)
                return e

            effective_node_quota = self.current_effective_node_quota()
            self.logger.debug(
                f"current_effective_node_quota: {effective_node_quota}")
            errors.append(
                check_node_quota_metric(self,
                                        self.ConfigProp.QUOTA_NODE_MAX_IN,
                                        effective_node_quota[0]))
            errors.append(
                check_node_quota_metric(self,
                                        self.ConfigProp.QUOTA_NODE_MAX_EG,
                                        effective_node_quota[1]))

        errors = set([e for e in errors if e])
        assert len(errors) == 0, (
            f"Test has failed with {len(errors)} distinct errors. "
            f"{errors}, rnd_seed: {self.rnd_seed}")


class ThroughputLimitsSnc(ThroughputLimitsSncBase):
    """
    A generic class for tests for throughput limiting that works at
    shard/node/cluster (SNC) levels
    """
    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        super(ThroughputLimitsSnc, self).__init__(test_ctx, *args, **kwargs)
        # at least 3 partitions needed overall to have 3 partition leaders
        # at the same node,
        self.topics = (TopicSpec(partition_count=3), )
        self.failed_checks = []
        self.total_checks = 0

    def check_equal_epsilon(self, measured: int, expected: int, epsilon: float,
                            what: str):
        error = (measured - expected) / expected
        measured = int(measured)
        if abs(error) > epsilon:
            self.failed_checks.append(
                f"{what} ({measured}) is not within ±{epsilon*100}% error "
                f"of the expected value ({expected}), "
                f"actual error: {float(f'{error*100:.4g}'):+g}%")
            self.logger.error(
                f"(FAIL) {what}: {measured} == {expected} {float(f'{error:.4g}'):+g} < ±{epsilon*100}%"
            )
        else:
            self.logger.info(
                f"(PASS) {what}: {measured} == {expected} {float(f'{error:.3g}'):+g} < ±{epsilon*100}%"
            )
        self.total_checks += 1

    @cluster(num_nodes=4)
    @parametrize(config_quota_node_max=8 * 1024 * 1024)
    @parametrize(config_quota_node_max=2 * 1024 * 1024)
    @parametrize(config_quota_node_max=512 * 1024)
    def test_node_limits(self, config_quota_node_max):
        """
        Apply various backpressure to one node of the cluster.
        Verify that node throughput limits are applied.
        """

        # add partitions to the topic until there are 3 partition
        # leaders on any of the nodes
        partitions_cnt = len(self.redpanda.partitions(self.topic))
        while True:
            while True:
                parts = self.redpanda.partitions(self.topic)
                if len(parts) < partitions_cnt:
                    self.logger.debug(
                        f"New partition not arrived yet, waiting. "
                        f"{parts} < {partitions_cnt}")
                    continue
                parts_c = Counter([p.leader for p in parts])
                if parts_c[None] == 0:
                    break
                self.logger.debug(f"Waiting for leader of the new partition")
                time.sleep(0.1)

            parts_c_m = parts_c.most_common(1)
            most_active_node, most_active_count = parts_c_m[0]
            self.logger.debug(
                f"Most active node is {most_active_node.name} "
                f"with {most_active_count} leaders. All stat: {parts_c}")
            if most_active_count == 3:
                break
            assert most_active_count < 3

            self.rpk.add_topic_partitions(self.topic, 1)
            partitions_cnt += 1

        self.logger.info(f"Working node: {most_active_node.name}")
        working_partitions = [p for p in parts if p.leader == most_active_node]
        self.logger.debug(f"Working partitions: {working_partitions}")

        self.redpanda.set_cluster_config({
            self.ConfigProp.QUOTA_NODE_MAX_IN.value:
            config_quota_node_max,
            self.ConfigProp.BAL_PERIOD_MS.value:
            100,
        })

        msg_size = 128 * 1024
        # do as many messages as needed to run for 10s at the set limit
        # however at smaller limits, more messages are needed to balance
        # the quota, therefore a constant component is added too
        msg_count = 100 + int(10 * config_quota_node_max / msg_size)
        time_to_run = msg_size * msg_count / config_quota_node_max
        # only test on one of the partitions yet, multi partition tests TBD
        partition = working_partitions[0].index

        p = RpkProducer(self.test_context,
                        self.redpanda,
                        topic=self.topic,
                        partition=partition,
                        msg_size=msg_size,
                        msg_count=msg_count,
                        produce_timeout=20)

        self.logger.info(
            f"Starting: msg_size: {msg_size}, msg_count: {msg_count}, time_to_run: {float(f'{time_to_run:.3g}'):g} s, partition: {partition}"
        )
        p.start()
        self.logger.info(f"Started")
        start = time.time()
        p.wait(10 + time_to_run)
        end = time.time()
        p.stop()
        measured_tp = (msg_size * msg_count) / (end - start)
        self.logger.info(
            f"Spent: {float(f'{end-start:.3g}'):g} s, test measured TP: {int(measured_tp)}, node limit: {config_quota_node_max}"
        )
        self.check_equal_epsilon(measured_tp, config_quota_node_max, 0.1,
                                 "Throughput measured by the test")

        assert len(
            self.failed_checks
        ) == 0, f"{len(self.failed_checks)}/{self.total_checks} checks have failed. {self.failed_checks}"
