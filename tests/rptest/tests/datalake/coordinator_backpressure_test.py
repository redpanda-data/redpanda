# Copyright 2026 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.admin import v2 as admin_v2
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint, SISettings
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest

ADD_FILES_BACKPRESSURE_METRIC = (
    "redpanda_iceberg_coordinator_add_files_backpressure_rejections_total"
)
FETCH_OFFSETS_BACKPRESSURE_METRIC = (
    "redpanda_iceberg_coordinator_fetch_offsets_backpressure_signals_total"
)
# Translator-side counter of how often translation backed off in response to
# coordinator backpressure.
TRANSLATION_BACKOFF_METRIC = "redpanda_iceberg_translation_backpressure_backoffs_total"
# Total parquet files created by translation.
FILES_CREATED_METRIC = "redpanda_iceberg_translation_files_created_total"

# Coordinator sheds load once this many pending files accumulate.
MAX_PENDING_FILES = 10


class CoordinatorBackpressureTest(RedpandaTest):
    """
    End-to-end check that datalake coordinator backpressure behaves sensibly:
    when a backlog of pending files builds up faster than it can commit, the
    coordinator sheds load, translators back off in response, and the backlog
    still commits to the catalog correctly.
    """

    def __init__(self, test_ctx, *args, **kwargs):
        super(CoordinatorBackpressureTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                # Translate eagerly, but commit rarely and only a file at a
                # time, so a backlog of pending files accumulates between commits
                # far faster than it drains and trips backpressure.
                "iceberg_target_lag_ms": 1000,
                "iceberg_catalog_commit_interval_ms": 10000,
                "datalake_coordinator_max_files_per_commit": 1,
                # Shed load once a small backlog of pending files accumulates.
                "datalake_coordinator_max_pending_files": MAX_PENDING_FILES,
            },
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def metric_sum(self, metric: str) -> float:
        """
        Returns the total value of a coordinator backpressure counter across all
        nodes and partitions.
        """
        samples = self.redpanda.metrics_sample(
            metric, self.redpanda.nodes, MetricsEndpoint.PUBLIC_METRICS
        )
        if not samples:
            return 0
        return sum(s.value for s in samples.samples)

    def pending_file_count(self) -> int:
        """
        Returns the number of pending data files (including DLQ files) the
        coordinator is tracking for the topic, read from the coordinator state
        admin endpoint. This mirrors what the coordinator counts when deciding
        to shed load.
        """
        admin = admin_v2.Admin(self.redpanda)
        request = admin_v2.datalake_pb.GetCoordinatorStateRequest(
            topics_filter=[self.topic_name]
        )
        response = admin.datalake().get_coordinator_state(request)
        total = 0
        for t_state in response.state.topic_states.values():
            for p_state in t_state.partition_states.values():
                for entry in p_state.pending_entries:
                    total += len(entry.data.data_files) + len(entry.data.dlq_files)
        return total

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_backpressure_on_pending_backlog(self, cloud_storage_type):
        """
        A burst across many partitions drives the pending-file backlog to the
        cap; the coordinator sheds load and translators back off in response.
        Once the pressure is relieved, the whole backlog commits to the table
        exactly once and the backlog drains.
        """
        partition_count = 30
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=partition_count)
            spark = dl.spark()

            # A burst across many partitions floods translation faster than the
            # coordinator commits, so a backlog of pending files builds up.
            dl.produce_to_topic(self.topic_name, msg_size=1024, msg_count=30000)

            # The coordinator sheds load once the backlog crosses the cap.
            wait_until(
                lambda: self.metric_sum(FETCH_OFFSETS_BACKPRESSURE_METRIC) > 0,
                timeout_sec=30,
                backoff_sec=1,
                err_msg="coordinator never applied backpressure",
            )

            # Translators respect the signal by backing off rather than spinning.
            wait_until(
                lambda: self.metric_sum(TRANSLATION_BACKOFF_METRIC) > 0,
                timeout_sec=30,
                backoff_sec=1,
                err_msg="no translator backed off in response to backpressure",
            )

            # The backlog is visible at (or above) the cap via the coordinator
            # state endpoint.
            wait_until(
                lambda: self.pending_file_count() >= MAX_PENDING_FILES,
                timeout_sec=15,
                backoff_sec=1,
                err_msg="pending file backlog never reached the backpressure threshold",
            )

            # Backpressure throttles the pipeline but must not stall commits:
            # the coordinator should keep landing files in the Iceberg table.
            # Feed the pipeline a bit so there is new data to commit if needed.
            committed_before = spark.count_parquet_files("redpanda", self.topic_name)
            dl.produce_to_topic(self.topic_name, msg_size=1024, msg_count=100)
            wait_until(
                lambda: spark.count_parquet_files("redpanda", self.topic_name)
                > committed_before,
                timeout_sec=60,
                backoff_sec=2,
                err_msg="coordinator stopped committing to iceberg under backpressure",
            )

            # Under sustained backpressure the pipeline should settle: with
            # translators blocked, neither the pending-file backlog nor the
            # number of parquet files translated keeps growing. Both should
            # hold steady over a window shorter than the commit interval.
            def quiesced():
                before = (
                    self.pending_file_count(),
                    self.metric_sum(FILES_CREATED_METRIC),
                )
                time.sleep(5)
                after = (
                    self.pending_file_count(),
                    self.metric_sum(FILES_CREATED_METRIC),
                )
                return before == after

            wait_until(
                quiesced,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="pending/translated file counts never stabilized under backpressure",
            )

            # Relieve the pressure so the backlog drains promptly.
            self.redpanda.set_cluster_config(
                {
                    "iceberg_catalog_commit_interval_ms": 1000,
                    "datalake_coordinator_max_files_per_commit": 10000,
                    "datalake_coordinator_max_pending_files": 1000000,
                }
            )
            wait_until(
                lambda: self.pending_file_count() == 0,
                timeout_sec=30,
                backoff_sec=1,
                err_msg="pending file backlog never drained",
            )

            # The whole backlog commits to the table exactly once (no gaps or
            # duplicates).
            DatalakeVerifier.oneshot(self.redpanda, self.topic_name, spark)
