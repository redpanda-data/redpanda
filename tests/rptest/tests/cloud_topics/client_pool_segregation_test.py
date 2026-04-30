# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Smoke test for the cloud_storage client-pool segregation feature.

Validates wiring end-to-end:
  - cloud_storage_max_capped_pool_pct is accepted as a cluster config.
  - The new capped-lease metrics (cloud_client_capped_lease_waiters,
    cloud_client_capped_lease_wait) and the new
    cloud_client_capped_lease_duration histogram are exposed once the
    client pool is in use. The pre-existing cloud_client_lease_duration
    histogram tracks all leases.
"""

from typing import Any
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (
    CLOUD_TOPICS_CONFIG_STR,
    MetricsEndpoint,
    SISettings,
)
from rptest.tests.redpanda_test import RedpandaTest


class ClientPoolSegregationSmokeTest(RedpandaTest):
    """
    End-to-end smoke test that exercises the per-shard
    cloud_storage_max_capped_pool_pct knob and verifies that the
    new capped-lease Prometheus metrics are exposed once we drive
    cloud-storage traffic through the pool.
    """

    NUM_BROKERS = 1
    TOPIC_NAME = "ct_pool_seg_smoke"
    PARTITION_COUNT = 4
    MSG_SIZE = 4096
    MSG_COUNT = 2000

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        extra_rp_conf: dict[str, Any] = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "enable_cluster_metadata_upload_loop": False,
            # Non-default value so the binding actually changes shape from
            # what the constructor seeded with shard_local defaults.
            "cloud_storage_max_capped_pool_pct": 25,
        }

        super().__init__(
            test_context=test_context,
            num_brokers=self.NUM_BROKERS,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )
        self.rpk = RpkTool(self.redpanda)

    def _metric_sum(self, metric_name: str, expect_metric: bool = True) -> float:
        return self.redpanda.metric_sum(
            metric_name=metric_name,
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=expect_metric,
        )

    @cluster(num_nodes=2)
    def test_segregation_metrics_exposed(self):
        # Create a CT topic so produce drives the cloud-storage pool.
        self.rpk.create_topic(
            topic=self.TOPIC_NAME,
            partitions=self.PARTITION_COUNT,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            self.MSG_SIZE,
            self.MSG_COUNT,
        )
        producer.start()
        producer.wait()

        # The pool-utilization metric proves the cloud-storage pool was
        # actually exercised.
        wait_until(
            lambda: self._metric_sum("vectorized_cloud_client_all_requests") > 0,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="cloud-storage pool was never exercised",
        )

        # New metrics.
        # Values are not meaningful since we're not applying any stress conditions.
        self._metric_sum("vectorized_cloud_client_capped_lease_waiters")
        self._metric_sum("vectorized_cloud_client_capped_lease_wait")
        self._metric_sum("vectorized_cloud_client_capped_lease_duration")
        self._metric_sum("vectorized_cloud_client_lease_duration")

    @cluster(num_nodes=1)
    def test_pct_config_accepted(self):
        # Live-update should be accepted at the boundary values.
        # pct=0 floors capped_capacity at 1 (so capped callers always
        # make some forward progress); pct=100 disables segregation.
        self.redpanda.set_cluster_config({"cloud_storage_max_capped_pool_pct": 0})
        self.redpanda.set_cluster_config({"cloud_storage_max_capped_pool_pct": 100})
        self.redpanda.set_cluster_config({"cloud_storage_max_capped_pool_pct": 50})
