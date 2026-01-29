# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint, RedpandaService
from rptest.tests.redpanda_test import RedpandaMixedTest
from rptest.util import expect_exception


class DefaultClientTest(RedpandaMixedTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, min_brokers=1)

    @cluster(num_nodes=1)
    def test_create_delete_topic(self):
        name = "test-create-delete-topic"
        spec = TopicSpec(name=name, replication_factor=1)

        client = self.client()

        client.create_topic(spec)

        desc = client.describe_topic(name)
        assert desc.name == name
        assert len(desc.partitions) > 0

        client.delete_topic(name)


class KafkaCliToolsTest(RedpandaMixedTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, min_brokers=1)

    @cluster(num_nodes=1)
    def test_produce_consume(self):
        name = "test-produce-consume"
        spec = TopicSpec(name=name, replication_factor=1)
        client = KafkaCliTools(self.redpanda)
        rpk = RpkTool(self.redpanda)

        client.create_topic(spec)

        # produce and consume a message to the topic
        client.produce(topic=name, num_records=1, record_size=100)
        rpk.consume(topic=name, n=1, timeout=10)

        client.delete_topic(name)


class RedpandaMixedTestSelfTest(RedpandaMixedTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, min_brokers=1, **kwargs)

    @cluster(num_nodes=1)
    def test_rpk(self):
        """A very basic rpk test."""
        rpk = RpkTool(self.redpanda)

        rpk.list_topics()

        name = "test-rpk-create-topic"
        rpk.create_topic(name)
        rpk.delete_topic(name)

    @cluster(num_nodes=1)
    def test_metrics(self):
        """Test metrics_sample() can retrieve internal metrics."""

        # gauge
        uptime = "vectorized_application_uptime"
        # counter, doesn't end in total
        polls = "vectorized_reactor_polls"
        # counter, ends in total
        awake = "vectorized_reactor_awake_time_ms_total"
        utilization = "vectorized_reactor_utilization"

        # single-pattern fuzzy
        vectorized_application_uptime = self.redpanda.metrics_sample(
            sample_pattern=uptime
        )
        assert vectorized_application_uptime is not None, "expected some metrics"
        if isinstance(self.redpanda, RedpandaService):
            assert len(vectorized_application_uptime.samples) == 1, "should be 1 node"
        else:
            assert len(vectorized_application_uptime.samples) >= 1, (
                "should be >=3s nodes"
            )
        assert vectorized_application_uptime.samples[0].value > 0, (
            "expected uptime greater than 0"
        )

        # single-pattern fuzzy, substring
        vectorized_application_uptime = self.redpanda.metrics_sample(
            sample_pattern="application_up"
        )
        assert vectorized_application_uptime is not None, "expected some metrics"
        if isinstance(self.redpanda, RedpandaService):
            assert len(vectorized_application_uptime.samples) == 1, "should be 1 node"
        else:
            assert len(vectorized_application_uptime.samples) >= 1, (
                "should be >=3s nodes"
            )
        assert vectorized_application_uptime.samples[0].value > 0, (
            "expected uptime greater than 0"
        )
        assert vectorized_application_uptime.samples[0].sample == uptime

        # multi-pattern fuzzy
        sample_patterns = [
            uptime,
            utilization,
        ]
        samples = self.redpanda.metrics_samples(sample_patterns)
        assert samples is not None, "expected sample patterns to match"

        count = self.redpanda.metric_sum(
            "vectorized_application_uptime", expect_metric=True
        )
        assert count > 0, "expected count greater than 0"

        # test cases for bad combinations of name and sample_pattern
        # Expect ValueError when neither provided
        try:
            _ = self.redpanda.metrics_sample(sample_pattern="", name="")
            assert False, (
                "Expected ValueError when neither sample_pattern nor name provided"
            )
        except ValueError as e:
            assert "Either 'name' or 'sample_pattern'" in str(e)

        # Expect ValueError when both provided
        try:
            _ = self.redpanda.metrics_sample(sample_pattern=uptime, name=uptime)
            assert False, (
                "Expected ValueError when both sample_pattern and name provided"
            )
        except ValueError as e:
            assert "both were" in str(e) or "both provided" in str(e)

        # exact match single
        vectorized_application_uptime = self.redpanda.metrics_sample(name=uptime)
        assert vectorized_application_uptime is not None, "expected some metrics"
        if isinstance(self.redpanda, RedpandaService):
            assert len(vectorized_application_uptime.samples) == 1, "should be 1 node"
        else:
            assert len(vectorized_application_uptime.samples) >= 1, (
                "should be >=3s nodes"
            )
        assert vectorized_application_uptime.samples[0].value > 0, (
            "expected uptime greater than 0"
        )

        # exact match two
        exact_two = self.redpanda.metrics_samples(names=[uptime, utilization])
        assert uptime in exact_two
        assert utilization in exact_two
        assert len(exact_two) == 2

        uptime_value = exact_two[uptime]
        if isinstance(self.redpanda, RedpandaService):
            assert len(uptime_value.samples) == 1, "should be 1 node"
        else:
            assert len(uptime_value.samples) >= 1, "should be >=3s nodes"
        assert uptime_value.samples[0].value > 0, "expected uptime greater than 0"

        ####################
        # test metrics sum #
        ####################
        def test_sum(ep: MetricsEndpoint, metric: str):
            res1 = self.redpanda.metric_sum(
                metric, metrics_endpoint=ep, expect_metric=True
            )
            res2 = self.redpanda.metric_sum(
                metric, metrics_endpoint=ep, expect_metric=True
            )

            assert isinstance(res1, float), f"expected float {metric} on {ep}"

            assert res1 > 0 and res2 > 0, (
                f"expected positive {metric} on {ep}, got {res1}, {res2}"
            )

            if metric == awake:
                # awake is too quantized (ms level) to increase every time
                valid = res2 >= res1
                msg = "not decrease"
            else:
                valid = res2 > res1
                msg = "increase"
            assert valid, f"expected {metric} to {msg} over time: {res1} {res2}"

        for m in [uptime, polls, awake]:
            test_sum(MetricsEndpoint.METRICS, m)
            if not m.endswith("_total"):
                # this should always work, see redpanda._metric_basename() docstring
                test_sum(MetricsEndpoint.METRICS, m + "_total")

        test_sum(
            MetricsEndpoint.PUBLIC_METRICS, "redpanda_application_uptime_seconds_total"
        )

        with expect_exception(AssertionError, lambda _: True):
            self.redpanda.metric_sum(
                "non_existent_metric",
                metrics_endpoint=MetricsEndpoint.METRICS,
                expect_metric=True,
            )

        if isinstance(self.redpanda, RedpandaService):
            # test that filtering works, only on RS as RSC doesn't support it on
            # public metrics
            uptime_only = self.redpanda.metrics(
                self.redpanda.nodes[0], MetricsEndpoint.METRICS, name=uptime
            )

            for m in uptime_only:
                assert m.name == uptime, f"expected only {uptime}, got {m.name} instead"
