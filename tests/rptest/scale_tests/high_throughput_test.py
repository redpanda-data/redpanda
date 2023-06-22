# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
import time

from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (RESTART_LOG_ALLOW_LIST,
                                      AdvertisedTierConfig, CloudTierName,
                                      MetricsEndpoint, SISettings)
from rptest.tests.prealloc_nodes import PreallocNodesTest

kiB = 1024
MiB = kiB * kiB
GiB = kiB * MiB

NoncloudTierConfigs = {
    #   ingress|          segment size|       partitions max|
    #             egress|       cloud cache size|connections # limit|
    #           # of brokers|           partitions min|           memory per broker|
    "docker-local":
    AdvertisedTierConfig(3 * MiB, 9 * MiB, 3, 128 * MiB, 20 * GiB, 1, 100, 100,
                         2 * GiB),
}


class HighThroughputTest2(PreallocNodesTest):
    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        self._ctx = test_ctx

        cloud_tier_str = test_ctx.globals.get("cloud_tier", "docker-local")
        if cloud_tier_str in NoncloudTierConfigs.keys():
            cloud_tier = None
            self.tier_config = NoncloudTierConfigs[cloud_tier_str]
            extra_rp_conf = {
                'log_segment_size': self.tier_config.segment_size,
                'cloud_storage_cache_size': self.tier_config.cloud_cache_size,
                # Disable segment merging: when we create many small segments
                # to pad out tiered storage metadata, we don't want them to
                # get merged together.
                'cloud_storage_enable_segment_merging': False,
                'disable_batch_cache': True,
            }
            num_brokers = self.tier_config.num_brokers

        else:
            try:
                cloud_tier = CloudTierName(cloud_tier_str)
            except ValueError:
                raise RuntimeError(
                    f"Unknown cloud tier specified: {cloud_tier_str}. Supported tiers: {CloudTierName.list()+NoncloudTierConfigs.keys()}"
                )
            extra_rp_conf = None
            num_brokers = None

        super(HighThroughputTest2,
              self).__init__(test_ctx,
                             1,
                             *args,
                             num_brokers=num_brokers,
                             extra_rp_conf=extra_rp_conf,
                             cloud_tier=cloud_tier,
                             disable_cloud_storage_diagnostics=True,
                             **kwargs)

        if cloud_tier is not None:
            self.tier_config = self.redpanda.advertised_tier_config
        test_ctx.logger.info(f"Cloud tier {cloud_tier}: {self.tier_config}")

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_throughput_simple(self):

        ingress_rate = self.tier_config.ingress_rate
        self.topics = [
            TopicSpec(partition_count=self.tier_config.partitions_min)
        ]
        super(HighThroughputTest2, self).setUp()

        target_run_time = 15
        target_data_size = target_run_time * ingress_rate
        msg_count = int(math.sqrt(target_data_size) / 2)
        msg_size = target_data_size // msg_count
        self.logger.info(
            f"Producing {msg_count} messages of {msg_size} B, "
            f"{msg_count*msg_size} B total, target rate {ingress_rate} B/s")

        producer0 = KgoVerifierProducer(self.test_context,
                                        self.redpanda,
                                        self.topic,
                                        msg_size=msg_size,
                                        msg_count=msg_count)
        producer0.start()
        start = time.time()

        producer0.wait()
        time0 = time.time() - start

        producer0.stop()
        producer0.free()

        rate0 = msg_count * msg_size / time0
        self.logger.info(f"Producer: {time0} s, {rate0} B/s")
        assert rate0 / ingress_rate > 0.5, f"Producer rate is too low. measured: {rate0} B/s, expected: {ingress_rate} B/s"
