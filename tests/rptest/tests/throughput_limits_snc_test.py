# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random, time, math
from enum import Enum
from typing import Tuple
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from ducktape.tests.test import TestContext

# This file is about throughput limiting that works at shard/node/cluster (SNC)
# levels, like cluster-wide and node-wide throughput limits


class ThroughputLimitsSnc(RedpandaTest):
    """
    Throughput limiting that works at shard/node/cluster (SNC)
    levels, like cluster-wide and node-wide throughput limits
    """

    # see later if need to split into more classes

    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        super(ThroughputLimitsSnc, self).__init__(test_ctx,
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

        self.config = {}
        for prop in list(self.ConfigProp):
            val = self.get_config_parameter_random_value(prop)
            self.config[prop] = val
            if not val is None:
                self.redpanda.add_extra_rp_conf({prop.value: val})
        self.logger.info(
            f"Initial cluster props: {self.redpanda._extra_rp_conf}")

    class ConfigProp(Enum):
        QUOTA_NODE_MAX_IN = "kafka_throughput_limit_node_in_bps"
        QUOTA_NODE_MAX_EG = "kafka_throughput_limit_node_out_bps"
        THROTTLE_DELAY_MAX_MS = "max_kafka_throttle_delay_ms"
        BAL_WINDOW_MS = "kafka_quota_balancer_window_ms"
        BAL_PERIOD_MS = "kafka_quota_balancer_node_period_ms"
        QUOTA_SHARD_MIN_RATIO = "kafka_quota_balancer_min_shard_throughput_ratio"
        QUOTA_SHARD_MIN_BPS = "kafka_quota_balancer_min_shard_throughput_bps"

    def binexp_random(self, min: int, max: int):
        min_exp = min.bit_length() - 1
        max_exp = max.bit_length() - 1
        return math.floor(2**(self.rnd.random() * (max_exp - min_exp) +
                              min_exp))

    def get_config_parameter_random_value(self, prop: ConfigProp):
        if prop in [
                self.ConfigProp.QUOTA_NODE_MAX_IN,
                self.ConfigProp.QUOTA_NODE_MAX_EG
        ]:
            r = self.rnd.randrange(4)
            min = 128  # practical minimum
            if r == 0:
                return None
            if r == 1:
                return min
            return self.binexp_random(min, 2**40)  # up to 1 TB/s

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_BPS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return self.binexp_random(0, 2**30)  # up to 1 GB/s

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
            return self.binexp_random(0, 2**25)  # up to ~1 year

        if prop == self.ConfigProp.BAL_WINDOW_MS:
            r = self.rnd.randrange(4)
            min = 1
            max = 2147483647
            if r == 0:
                return min
            if r == 1:
                return max
            return self.binexp_random(min, max)

        if prop == self.ConfigProp.BAL_PERIOD_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return self.binexp_random(0, 2**22)  # up to ~1.5 months

        raise Exception(f"Unsupported ConfigProp: {prop}")

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
