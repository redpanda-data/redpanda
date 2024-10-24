# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from ducktape.mark import matrix
from rptest.services.redpanda import make_redpanda_service
from ducktape.tests.test import Test
from rptest.clients.default import DefaultClient

BOOTSTRAP_CONFIG = {
    'disable_metrics': False,
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
        rp_conf['aggregate_metrics'] = aggregate_metrics
        self.redpanda = make_redpanda_service(self.ctx,
                                              num_brokers=3,
                                              extra_rp_conf=rp_conf)
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()
        self.client = DefaultClient(self.redpanda)

    @staticmethod
    def filter_metrics(metrics):
        # We ignore those because:
        #  - seastar metrics so not affected by aggregate_metrics anyway
        #  - compaction io_queue class metrics can pop up after a delay so might make this flaky
        return list(metric for metric in metrics if not "io_queue" in metric)

    @cluster(num_nodes=3)
    @matrix(aggregate_metrics=[True, False])
    def test_aggregate_metrics(self, aggregate_metrics):
        """
        Verify that changing aggregate_metrics does preserve metric counts

        """

        self.start_redpanda(aggregate_metrics)

        topic_spec = TopicSpec(name="test",
                               partition_count=100,
                               replication_factor=3)

        self.client.create_topic(topic_spec)

        metrics_pre_change = self.filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n"))

        self.redpanda.set_cluster_config(
            {"aggregate_metrics": not aggregate_metrics})

        metrics_post_change = self.filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n"))

        self.redpanda.set_cluster_config(
            {"aggregate_metrics": aggregate_metrics})

        metrics_pre_chanage_again = self.filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n"))

        assert len(metrics_pre_change) != len(metrics_post_change)
        assert len(metrics_pre_change) == len(metrics_pre_chanage_again)
