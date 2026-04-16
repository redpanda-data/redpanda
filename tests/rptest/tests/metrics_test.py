# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from ducktape.mark import matrix
from ducktape.tests.test import Test

from rptest.clients.default import DefaultClient
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint, make_redpanda_service
from rptest.tests.redpanda_test import RedpandaTest

BOOTSTRAP_CONFIG = {
    "disable_metrics": False,
}


class MetricsTest(Test):
    def __init__(self, test_ctx, *args, **kwargs):
        self.ctx = test_ctx
        self.redpanda = None
        self.client = None
        super(MetricsTest, self).__init__(test_ctx, *args, **kwargs)

    def setUp(self):
        pass

    def start_redpanda(self, aggregate_metrics):
        rp_conf = BOOTSTRAP_CONFIG.copy()
        rp_conf["aggregate_metrics"] = aggregate_metrics
        self.redpanda = make_redpanda_service(
            self.ctx, num_brokers=3, extra_rp_conf=rp_conf
        )
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()
        self.client = DefaultClient(self.redpanda)

    @staticmethod
    def filter_metrics(metrics):
        # We ignore those because:
        #  - seastar metrics so not affected by aggregate_metrics anyway
        #  - compaction io_queue class metrics can pop up after a delay so might make this flaky
        return list(metric for metric in metrics if "io_queue" not in metric)

    @cluster(num_nodes=3)
    @matrix(aggregate_metrics=[True, False])
    def test_aggregate_metrics(self, aggregate_metrics):
        """
        Verify that changing aggregate_metrics does preserve metric counts

        """

        self.start_redpanda(aggregate_metrics)

        topic_spec = TopicSpec(name="test", partition_count=100, replication_factor=3)

        self.client.create_topic(topic_spec)

        metrics_pre_change = self.filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n")
        )

        self.redpanda.set_cluster_config({"aggregate_metrics": not aggregate_metrics})

        metrics_post_change = self.filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n")
        )

        self.redpanda.set_cluster_config({"aggregate_metrics": aggregate_metrics})

        metrics_pre_chanage_again = self.filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n")
        )

        assert len(metrics_pre_change) != len(metrics_post_change)
        assert len(metrics_pre_change) == len(metrics_pre_chanage_again)


class DisableMetricsTest(RedpandaTest):
    # Allowlist of Seastar metric group prefixes that are expected to
    # remain on the internal endpoint even with disable_metrics=True.
    # Any metric family not matching is a Redpanda application metric
    # and should not be present.
    SEASTAR_METRIC_RE = re.compile(
        r"^vectorized_("
        r"alien|"
        r"httpd|"
        r"io_queue|"
        r"memory|"
        r"network|"
        r"reactor|"
        r"scheduler|"
        r"stall_detector"
        r")_"
    )

    def __init__(self, test_ctx):
        super().__init__(
            test_ctx,
            num_brokers=1,
            extra_rp_conf={
                "disable_metrics": True,
                "disable_public_metrics": True,
            },
        )

    @cluster(num_nodes=1)
    def test_disable_metrics(self):
        """
        Verify that starting Redpanda with both disable_metrics and
        disable_public_metrics causes the internal endpoint to contain
        only Seastar metrics (no Redpanda application metrics) and the
        public endpoint to be completely empty.
        """
        node = self.redpanda.nodes[0]

        # Check internal metrics: only Seastar metrics should remain.
        internal_families = self.redpanda.metrics(node, MetricsEndpoint.METRICS)
        non_seastar = [
            f.name
            for f in internal_families
            if f.samples and not self.SEASTAR_METRIC_RE.match(f.name)
        ]
        for name in non_seastar:
            self.redpanda.logger.debug(f"unexpected internal metric family: {name}")
        assert len(non_seastar) == 0, (
            f"Expected only Seastar metrics on internal endpoint, "
            f"got {len(non_seastar)} Redpanda metric families: "
            f"{non_seastar[:10]}"
        )

        # Check public metrics: should be completely empty.
        public_families = self.redpanda.metrics(node, MetricsEndpoint.PUBLIC_METRICS)
        non_empty = [f.name for f in public_families if f.samples]
        for name in non_empty:
            self.redpanda.logger.debug(f"unexpected public metric family: {name}")
        assert len(non_empty) == 0, (
            f"Expected no public metrics, got {len(non_empty)} "
            f"families: {non_empty[:10]}"
        )
