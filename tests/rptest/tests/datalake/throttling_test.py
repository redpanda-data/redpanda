# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.tests.datalake.catalog_service_factory import (
    supported_catalog_types,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class DatalakeThrottlingTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeThrottlingTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            environment={"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"},
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def _total_throttle(self):
        total = 0
        sample = self.redpanda.metrics_sample("total_throttle")
        assert sample is not None, "total_throttle metric not found"
        for s in sample.samples:
            self.logger.debug(f"total throttle - metrics sample: {s}")
            total += s.value
        return total

    def _throttled_requests(self):
        total = 0
        sample = self.redpanda.metrics_sample("throttled_requests")
        assert sample is not None, "throttled_requests metric not found"
        for s in sample.samples:
            self.logger.debug(f"requests throttled - metrics sample: {s}")
            total += s.value
        return total

    def producer_throttled(self, dl: DatalakeServices):
        KgoVerifierProducer.oneshot(
            self.test_ctx,
            self.redpanda,
            self.topic_name,
            msg_size=1024,
            msg_count=3 * 10240,
            client_name="iceberg_producer",
        )

        throttle = self._total_throttle()
        throttled_requests = self._throttled_requests()
        return throttle > 0 and throttled_requests > 0

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_basic_throttling(self, cloud_storage_type, catalog_type):
        msg_cnt = 100
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.TRINO],
            catalog_type=catalog_type,
        ) as dl:
            non_iceberg_topic = TopicSpec(partition_count=1, replication_factor=1)
            rpk = RpkTool(self.redpanda)
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            DefaultClient(self.redpanda).create_topic(non_iceberg_topic)
            dl.produce_to_topic(self.topic_name, 1024, msg_cnt)
            dl.wait_for_translation(self.topic_name, msg_count=msg_cnt)
            assert self._total_throttle() == 0, (
                "There should be no throttling in baseline conditions"
            )

            # Block translation by setting the max number of translations to 0
            self.redpanda.set_cluster_config(
                {
                    "datalake_scheduler_max_concurrent_translations": 0,
                    "iceberg_target_backlog_size": 1000,
                    "iceberg_backlog_controller_p_coeff": 1.0,
                    "iceberg_throttle_backlog_size_ratio": 0.0005,
                }
            )
            admin = Admin(self.redpanda)

            # Set the disk space to relatively small value
            new_total = 20 * (1024 * 1024 * 1024)
            new_free = 10 * (1024 * 1024 * 1024)
            admin.set_disk_stat_override(
                "data",
                self.redpanda.nodes[0],
                total_bytes=new_total,
                free_bytes=new_free,
            )
            # Produce some more messages
            wait_until(lambda: self.producer_throttled(dl), timeout_sec=60)

            # Validate that non Iceberg related producers are not throttled
            current_throttle = self._total_throttle()
            dl.produce_to_topic(non_iceberg_topic.name, 1024, 10)
            assert self._total_throttle() == current_throttle, (
                "Total throttle should not increase as the topic is not iceberg enabled"
            )
            # Enable translation back
            self.redpanda.set_cluster_config(
                {"datalake_scheduler_max_concurrent_translations": 4}
            )
            partitions = rpk.describe_topic(self.topic_name)
            total_messages = 0
            for p in partitions:
                total_messages += p.high_watermark
            self.logger.info(f"waiting for translation of {total_messages} messages")
            dl.wait_for_translation(self.topic_name, msg_count=total_messages)
            dl.produce_to_topic(self.topic_name, 1024, msg_cnt)
            assert self._total_throttle() == current_throttle, (
                "Total throttle should not increase as the translation is progressing"
            )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        catalog_type=supported_catalog_types(),
    )
    def test_backlog_metric(self, cloud_storage_type, catalog_type):
        def collect_backlog_metric():
            sample = self.redpanda.metrics_sample(
                "vectorized_iceberg_backlog_controller_backlog_size"
            )
            assert sample is not None, "iceberg_backlog metric not found"
            total = 0
            for s in sample.samples:
                self.logger.info(f"backlog metrics sample : {s}")
                total += s.value
            return total

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=catalog_type,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            # execute few iterations to verify backlog calculation
            for i in range(1, 5):
                self.redpanda.set_cluster_config(
                    {"datalake_scheduler_max_concurrent_translations": 0}
                )
                dl.produce_to_topic(self.topic_name, 1024, 20000)

                wait_until(
                    lambda: collect_backlog_metric() > 10240,
                    timeout_sec=30,
                    backoff_sec=2,
                )

                self.redpanda.set_cluster_config(
                    {"datalake_scheduler_max_concurrent_translations": 10}
                )
                dl.wait_for_translation(self.topic_name, msg_count=i * 20000)
                # after everything is translated reported backlog should be 0
                wait_until(
                    lambda: collect_backlog_metric() == 0, timeout_sec=30, backoff_sec=2
                )
                # restart all nodes
                self.redpanda.restart_nodes(self.redpanda.nodes)
                # check backlog again
                for _ in range(0, 5):
                    assert collect_backlog_metric() == 0, (
                        "Backlog should be 0 as all data are translated"
                    )
                    time.sleep(1)
