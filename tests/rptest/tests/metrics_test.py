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
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint, make_redpanda_service
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result

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
        #  - cluster_info registers in the background once the cluster UUID is
        #    known, so it can appear between scrapes
        return list(
            metric
            for metric in metrics
            if "io_queue" not in metric and "cluster_info" not in metric
        )

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


class DisableMetricsTest(Test):
    # Group-name prefixes for metrics that Seastar registers on the internal
    # endpoint and that legitimately remain even with disable_metrics=True.
    # Any internal family not matching this is a Redpanda application metric
    # and must not be present.
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

    # Redpanda-owned metrics that match SEASTAR_METRIC_RE only because they
    # share a group name with Seastar (available_memory registers under the
    # "memory" group, see src/v/resource_mgmt/available_memory.cc). They are
    # still Redpanda metrics and must obey the disable flags, so they count
    # as leaks if present on a disabled endpoint despite matching the regex.
    REDPANDA_METRICS_MATCHING_SEASTAR_RE = {
        "vectorized_memory_available_memory",
        "vectorized_memory_available_memory_low_water_mark",
    }

    def __init__(self, test_ctx, *args, **kwargs):
        self.ctx = test_ctx
        self.redpanda = None
        super().__init__(test_ctx, *args, **kwargs)

    def setUp(self):
        pass

    def start_redpanda(self, disable_metrics, disable_public_metrics):
        self.redpanda = make_redpanda_service(
            self.ctx,
            num_brokers=1,
            extra_rp_conf={
                "disable_metrics": disable_metrics,
                "disable_public_metrics": disable_public_metrics,
            },
        )
        self.redpanda.start()

    def _is_allowed_internal(self, name):
        return (
            bool(self.SEASTAR_METRIC_RE.match(name))
            and name not in self.REDPANDA_METRICS_MATCHING_SEASTAR_RE
        )

    def _redpanda_metrics(self, node, endpoint):
        """Names of the Redpanda-owned metric families present on `endpoint`.

        On the internal endpoint Seastar metrics legitimately remain, so they
        are filtered out; the public endpoint only ever carries Redpanda
        metrics, so every non-empty family counts.
        """
        families = self.redpanda.metrics(node, endpoint)
        if endpoint == MetricsEndpoint.METRICS:
            return [
                f.name
                for f in families
                if f.samples and not self._is_allowed_internal(f.name)
            ]
        return [f.name for f in families if f.samples]

    def _verify_endpoint_disabled(self, disabled_endpoint):
        """Disable a single metrics endpoint and verify it is the only one
        affected: it must carry no Redpanda metrics, while the other endpoint
        keeps exposing them."""
        enabled_endpoint = (
            MetricsEndpoint.PUBLIC_METRICS
            if disabled_endpoint == MetricsEndpoint.METRICS
            else MetricsEndpoint.METRICS
        )
        self.start_redpanda(
            disable_metrics=disabled_endpoint == MetricsEndpoint.METRICS,
            disable_public_metrics=disabled_endpoint == MetricsEndpoint.PUBLIC_METRICS,
        )
        node = self.redpanda.nodes[0]

        leaked = self._redpanda_metrics(node, disabled_endpoint)
        for name in leaked:
            self.redpanda.logger.debug(
                f"leaked Redpanda metric on disabled "
                f"{disabled_endpoint.value} endpoint: {name}"
            )
        assert len(leaked) == 0, (
            f"disabling the {disabled_endpoint.value} endpoint did not suppress "
            f"Redpanda metrics; leaked {len(leaked)} families: {leaked[:10]}"
        )

        present = self._redpanda_metrics(node, enabled_endpoint)
        assert len(present) > 0, (
            f"expected Redpanda metrics to remain on the "
            f"{enabled_endpoint.value} endpoint, but found none"
        )

    @cluster(num_nodes=1)
    def test_disable_internal_metrics(self):
        """
        With disable_metrics=True (and public metrics still enabled), the
        internal /metrics endpoint must expose only Seastar metrics - no
        Redpanda application metrics - while the public endpoint is
        unaffected and still exposes Redpanda metrics.
        """
        self._verify_endpoint_disabled(MetricsEndpoint.METRICS)

    @cluster(num_nodes=1)
    def test_disable_public_metrics(self):
        """
        With disable_public_metrics=True (and internal metrics still
        enabled), the public endpoint must be completely empty while the
        internal endpoint is unaffected and still exposes Redpanda metrics.
        """
        self._verify_endpoint_disabled(MetricsEndpoint.PUBLIC_METRICS)


class ClusterIdentityMetricsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(test_ctx, num_brokers=3, *args, **kwargs)

    def _cluster_info_labels(self, node, endpoint) -> dict[str, str] | None:
        """The labels of the cluster_info metric on `endpoint`, or None if
        the metric has not been registered yet."""
        prefix = (
            "redpanda" if endpoint == MetricsEndpoint.PUBLIC_METRICS else "vectorized"
        )
        families = [
            f
            for f in self.redpanda.metrics(
                node, endpoint, name=f"{prefix}_cluster_info"
            )
            if f.samples
        ]
        if not families:
            return None
        assert len(families) == 1, f"expected a single family, got {families}"
        samples = families[0].samples
        assert len(samples) == 1, (
            f"expected a single cluster_info series, got {samples}"
        )
        assert samples[0].value == 1
        return samples[0].labels

    @cluster(num_nodes=3)
    def test_cluster_info_metric(self):
        """
        Every broker must expose the cluster_info identity metric on both
        metrics endpoints, carrying the bootstrap cluster UUID as reported
        by the admin API's GET /v1/cluster/uuid and the immutable cluster
        metrics UUID generated by the metrics reporter.

        The metric registers in the background once both identities are
        known, so tolerate a brief delay after startup.
        """
        admin = Admin(self.redpanda)
        expected_uuid = admin.get_cluster_uuid(self.redpanda.nodes[0])
        assert expected_uuid, "admin API returned no cluster UUID"
        # The metrics reporter initializes the cluster_id config property
        # from the cluster metrics UUID, and nothing in this test modifies
        # it afterwards, so it is a valid expectation for the
        # cluster_metrics_uuid label.
        expected_metrics_uuid = wait_until_result(
            lambda: admin.get_cluster_config(self.redpanda.nodes[0]).get("cluster_id"),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="cluster_id was not initialized",
        )

        for node in self.redpanda.nodes:
            for endpoint in [MetricsEndpoint.METRICS, MetricsEndpoint.PUBLIC_METRICS]:
                labels = wait_until_result(
                    lambda: self._cluster_info_labels(node, endpoint),
                    timeout_sec=30,
                    backoff_sec=1,
                    retry_on_exc=True,
                    err_msg=f"cluster_info metric did not appear on "
                    f"{endpoint.value} of {node.name}",
                )
                expected_labels = {
                    "cluster_uuid": expected_uuid,
                    "cluster_metrics_uuid": expected_metrics_uuid,
                }
                assert labels == expected_labels, (
                    f"cluster_info on {endpoint.value} of {node.name} reports "
                    f"{labels}, expected {expected_labels}"
                )
