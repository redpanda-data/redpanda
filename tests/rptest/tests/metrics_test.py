# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os

from prometheus_client.parser import text_string_to_metric_families
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from ducktape.mark import matrix, ignore, parametrize
from ducktape.mark import parametrize
from rptest.services.redpanda import MetricsEndpoint, ResourceSettings, SISettings, make_redpanda_service
from ducktape.tests.test import Test
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from pathlib import Path

BOOTSTRAP_CONFIG = {
    'disable_metrics': False,
}

GOLDEN_METRICS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   "metrics_test_golden_metrics.txt")

METRICS_PAGE_LINK = 'https://vectorizedio.atlassian.net/l/cp/L74Rfvgc'


def golden_metrics_path(aggregate_metrics: bool, public_metrics: bool):
    filename = "metrics_test_golden_metrics"

    if aggregate_metrics:
        filename += "_aggregate"
    elif public_metrics:
        filename += "_public"

    filename += ".txt"

    return Path(__file__).parent / filename


class MetricsTest(Test):
    def __init__(self, test_ctx, *args, **kwargs):

        self.ctx = test_ctx
        self.redpanda = None
        self.client = None
        super(MetricsTest, self).__init__(test_ctx, *args, **kwargs)

    def setUp(self):
        pass

    def start_redpanda(self,
                       aggregate_metrics,
                       enable_ts=False,
                       num_brokers=3):
        rp_conf = BOOTSTRAP_CONFIG.copy()
        rp_conf['aggregate_metrics'] = aggregate_metrics
        resource_settings = ResourceSettings(num_cpus=2)
        si_settings = None
        if enable_ts:
            si_settings = SISettings(self.ctx)
        self.redpanda = make_redpanda_service(
            self.ctx,
            num_brokers=num_brokers,
            resource_settings=resource_settings,
            si_settings=si_settings,
            extra_rp_conf=rp_conf)
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()
        self.client = DefaultClient(self.redpanda)
        self.rpk_client = RpkTool(self.redpanda)

    @staticmethod
    def filter_metrics(metric: str):
        # version label will be different across tests
        if 'application_build' in metric:
            return False

        # these only get added in a delayed fashion and are from seastar anyway
        if 'io_queue' in metric:
            return False

        # these are dynamic and target depends on the nodes so not stable neither
        if 'rpc_client' in metric:
            return False

        return True

    @staticmethod
    def clean_metrics(metrics: [str]):
        # Note we basically do all operations on the string form of the
        # prometheus output. We could also parse them into some object
        # representation using something like
        # prometheus_client.parser.text_string_to_metric_families. However that
        # library doesn't offer an easy to convert manipulated metrics back to
        # text form so it's not of too much use.

        # remove known bad lines and remove value from samples
        def strip_comments_and_filter(metric: str):
            # we don't care about TYPE or HELP changes
            if metric.startswith('#'):
                return False

            if metric == '' or metric == '\n':
                return False

            return MetricsTest.filter_metrics(metric)

        # otherwise regenerating the golden file causes lots of noise
        def set_0_value(metric: str):
            metric_name, metric_value = metric.split(' ')
            return f"{metric_name} 0"

        return [
            set_0_value(metric) for metric in metrics
            if strip_comments_and_filter(metric)
        ]

    @cluster(num_nodes=3)
    @matrix(aggregate_metrics=[True, False])
    def test_aggregate_metrics(self, aggregate_metrics):
        """
        Verify that changing aggregate_metrics does preserve metric counts

        """
        def filter_metrics(metrics):
            return [
                metric for metric in metrics if self.filter_metrics(metric)
            ]

        self.start_redpanda(aggregate_metrics)

        topic_spec = TopicSpec(name="test",
                               partition_count=100,
                               replication_factor=3)

        self.client.create_topic(topic_spec)

        metrics_pre_change = filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n"))

        self.redpanda.set_cluster_config(
            {"aggregate_metrics": not aggregate_metrics})

        metrics_post_change = filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n"))

        self.redpanda.set_cluster_config(
            {"aggregate_metrics": aggregate_metrics})

        metrics_pre_chanage_again = filter_metrics(
            self.redpanda.raw_metrics(self.redpanda.nodes[0]).split("\n"))

        assert len(metrics_pre_change) != len(metrics_post_change)
        assert len(metrics_pre_change) == len(metrics_pre_chanage_again)

    def _setup_redpanda_for_new_metrics_t(self, aggregate_metrics=False):
        # start redpanda and create topics for metrics test
        self.start_redpanda(aggregate_metrics=aggregate_metrics,
                            enable_ts=True,
                            num_brokers=1)

        # There is no hard rule here or specific logic behind how many
        # partitions or topics we create. The idea is to call out potential
        # scaling in both dimensions in case metrics are added that carry a
        # topic and or partition label.

        for i in range(10):
            topic_spec = TopicSpec(name=f"test-{i}",
                                   partition_count=10,
                                   replication_factor=1,
                                   redpanda_remote_read=True,
                                   redpanda_remote_write=True)

            self.client.create_topic(topic_spec)

        for i in range(10):
            self.rpk_client.produce(topic=f"test-{i}", key="key", msg='hello')

        for i in range(10):
            self.rpk_client.consume(topic=f"test-{i}", group=f"test-{i}", n=1)

    def _test_new_metrics(self, aggregate_metrics, public_metrics):
        f"""
        Verify that no new metrics are added without being added to the golden
        metrics file.

        The idea is to make adding metrics a concious decision and adhering to
        certain guidelines.

        Please read {METRICS_PAGE_LINK} for more info.
        """
        assert not (aggregate_metrics and public_metrics)

        self._setup_redpanda_for_new_metrics_t(aggregate_metrics)

        golden_metrics = self.clean_metrics(
            golden_metrics_path(aggregate_metrics,
                                public_metrics).read_text().splitlines())

        metrics = self.clean_metrics(
            self.redpanda.raw_metrics(
                self.redpanda.nodes[0], MetricsEndpoint.PUBLIC_METRICS
                if public_metrics else MetricsEndpoint.METRICS).split('\n'))

        # find new metrics
        new_metrics_series = []
        for metric in metrics:
            if metric not in golden_metrics:
                new_metrics_series.append(metric)

        if len(new_metrics_series) != 0:
            max_output = 100
            new_metrics = '\n' + '\n'.join(new_metrics_series[:max_output])
            if len(new_metrics_series) > max_output:
                new_metrics += "\n... truncated"

            self.logger.error(new_metrics)

        error_msg = f"Found {len(new_metrics_series)} new metric series, please refer to {METRICS_PAGE_LINK} " + f"and amend {golden_metrics_path(aggregate_metrics, public_metrics)} as needed"
        assert len(new_metrics_series) == 0, error_msg

    @cluster(num_nodes=1)
    @matrix(aggregate_metrics=[True, False])
    def test_new_internal_metrics(self, aggregate_metrics):
        self._test_new_metrics(aggregate_metrics, False)

    @cluster(num_nodes=1)
    def test_new_public_metrics(self):
        self._test_new_metrics(False, True)

    @cluster(num_nodes=1)
    @parametrize(generate_golden=False)
    def test_regen_golden_metrics(self, generate_golden):
        """
        'Test' to regenerate the metrics file. Doesn't do anything by default.

        Run this test with generate_golden=True to regenerate the golden metrics file, .e.g:

        task rp:run-ducktape-tests DUCKTAPE_ARGS="tests/rptest/tests/metrics_test.py::MetricsTest.test_regen_golden_metrics --parameters='{\"generate_golden\":true}'"
        """
        # do first otherwise we don't use the requested amount of nodes and the test fails
        self._setup_redpanda_for_new_metrics_t()

        if not generate_golden:
            self.logger.info("Not updating golden metrics file")
            return

        def gen_metrics_file(aggregate_metrics, public_metrics):
            self.redpanda.set_cluster_config(
                {"aggregate_metrics": aggregate_metrics})

            self.logger.warn(
                f"Regenerating golden metrics file {golden_metrics_path(aggregate_metrics, public_metrics)}"
            )

            metrics = self.clean_metrics(
                self.redpanda.raw_metrics(
                    self.redpanda.nodes[0], MetricsEndpoint.PUBLIC_METRICS if
                    public_metrics else MetricsEndpoint.METRICS).split('\n'))

            golden_metrics_path(aggregate_metrics,
                                public_metrics).write_text('\n'.join(metrics))

        for aggregate_metrics in [False, True]:
            gen_metrics_file(aggregate_metrics, False)

        gen_metrics_file(False, True)
