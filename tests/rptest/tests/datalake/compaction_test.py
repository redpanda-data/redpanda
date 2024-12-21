# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import time
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.utils.mode_checks import skip_debug_mode
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier


class CompactionGapsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(CompactionGapsTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx, fast_uploads=True),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
                "datalake_coordinator_snapshot_max_delay_secs": 10,
                "log_compaction_interval_ms": 2000
            },
            *args,
            **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"
        self.segment_size = 5 * 1024 * 1024
        self.kafka_cat = KafkaCat(self.redpanda)

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def partition_segments(self) -> int:
        assert len(self.redpanda.nodes) == 1, self.redpanda.nodes
        node = self.redpanda.nodes[0]
        storage = self.redpanda.node_storage(node)
        topic_partitions = storage.partitions("kafka", self.topic_name)
        assert len(topic_partitions) == 1, len(topic_partitions)
        segment_count = len(topic_partitions[0].segments)
        self.redpanda.logger.debug(f"Current segment count: {segment_count}")
        return segment_count

    def wait_until_segment_count(self, count):
        wait_until(
            lambda: self.partition_segments() == count,
            timeout_sec=30,
            backoff_sec=3,
            err_msg=f"Timed out waiting for segment count to reach {count}")

    def produce_until_segment_count(self, count):
        timeout_sec = 30
        deadline = time() + timeout_sec
        while True:
            current_segment_count = self.partition_segments()
            if current_segment_count >= count:
                return
            if time() > deadline:
                assert False, f"Unable to reach segment count {count} in {timeout_sec}s, current count {current_segment_count}"
            KgoVerifierProducer.oneshot(self.test_ctx,
                                        self.redpanda,
                                        self.topic_name,
                                        2024,
                                        10000,
                                        key_set_cardinality=2)

    def ensure_translation(self, dl: DatalakeServices):
        (_, max_offset) = self.kafka_cat.list_offsets(topic=self.topic_name,
                                                      partition=0)
        self.redpanda.logger.debug(
            f"Ensuring translation until: {max_offset - 1}")
        dl.wait_for_translation_until_offset(self.topic_name, max_offset - 1)

    def do_test_no_gaps(self, dl: DatalakeServices):

        dl.create_iceberg_enabled_topic(self.topic_name,
                                        iceberg_mode="key_value",
                                        config={
                                            "cleanup.policy":
                                            TopicSpec.CLEANUP_COMPACT,
                                            "segment.bytes": self.segment_size
                                        })

        for _ in range(5):
            self.produce_until_segment_count(5)
            # # Ensure everything is translated
            self.ensure_translation(dl)
            # # Disable iceberg
            dl.set_iceberg_mode_on_topic(self.topic_name, "disabled")
            # Append more data
            self.produce_until_segment_count(8)
            # # Compact the data
            # # One closed segment and one open (current) segment
            self.wait_until_segment_count(2)
            # # Enable iceberg again
            dl.set_iceberg_mode_on_topic(self.topic_name, "key_value")

    @cluster(num_nodes=4)
    @skip_debug_mode
    @matrix(cloud_storage_type=supported_storage_types())
    def test_translation_no_gaps(self, cloud_storage_type):
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              include_query_engines=[QueryEngineType.TRINO
                                                     ]) as dl:
            self.do_test_no_gaps(dl)


class CompactionTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self.extra_rp_conf = {
            "iceberg_enabled": "true",
            "iceberg_catalog_commit_interval_ms": 5000,
            "datalake_coordinator_snapshot_max_delay_secs": 10,
            "log_compaction_interval_ms": 2000,
            "log_segment_size": 2 * 1024**2,  # 2 MiB
            "compacted_log_segment_size": 1024**2  # 1 MiB
        }

        super(CompactionTest,
              self).__init__(test_ctx,
                             num_brokers=1,
                             si_settings=SISettings(test_context=test_ctx,
                                                    fast_uploads=True),
                             extra_rp_conf=self.extra_rp_conf,
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "tapioca"
        self.msg_size = 1024  # 1 KiB
        self.total_data = 100 * 1024**2  # 100 MiB
        self.msg_count = int(self.total_data / self.msg_size)
        self.kafka_cat = KafkaCat(self.redpanda)

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    def produce(self):
        producer = KgoVerifierProducer(self.test_ctx,
                                       self.redpanda,
                                       self.topic_name,
                                       msg_size=self.msg_size,
                                       msg_count=self.msg_count,
                                       key_set_cardinality=100,
                                       validate_latest_values=True)

        producer.start(clean=True)
        producer.wait()
        producer.free()

    def wait_for_compaction(self):
        # Restart each redpanda broker to force roll segments
        self.redpanda.restart_nodes(self.redpanda.nodes)

        def get_complete_sliding_window_rounds():
            return self.redpanda.metric_sum(
                metric_name=
                "vectorized_storage_log_complete_sliding_window_rounds_total",
                metrics_endpoint=MetricsEndpoint.METRICS,
                topic=self.topic_name)

        # Sleep until the log has been fully compacted.
        self.prev_sliding_window_rounds = -1

        def compaction_has_completed():
            new_sliding_window_rounds = get_complete_sliding_window_rounds()
            res = new_sliding_window_rounds > 0 and self.prev_sliding_window_rounds == new_sliding_window_rounds
            self.prev_sliding_window_rounds = new_sliding_window_rounds
            return res

        wait_until(
            compaction_has_completed,
            timeout_sec=120,
            backoff_sec=self.extra_rp_conf['log_compaction_interval_ms'] /
            1000 * 10,
            err_msg="Compaction did not stabilize.")

    def verify_log_and_table(self, dl: DatalakeServices):
        # Verify a fully compacted log with a sequential consumer
        consumer = KgoVerifierSeqConsumer(
            self.test_ctx,
            self.redpanda,
            self.topic_name,
            compacted=True,
            validate_latest_values=True,
        )

        consumer.start(clean=False)
        consumer.wait(timeout_sec=120)
        consumer.free()

        verifier = DatalakeVerifier(self.redpanda,
                                    self.topic_name,
                                    dl.trino(),
                                    compacted=True)

        verifier.start()
        verifier.wait()

        for partition, offset_consumed in verifier._max_consumed_offsets.items(
        ):
            assert consumer.consumer_status.validator.max_offsets_consumed[str(
                partition)] == offset_consumed

    def do_test_compaction(self, dl: DatalakeServices):
        dl.create_iceberg_enabled_topic(
            self.topic_name,
            iceberg_mode="key_value",
            config={"cleanup.policy": TopicSpec.CLEANUP_COMPACT})

        for _ in range(5):
            self.produce()
            # Compact the data. Compaction settling indicates that
            # everything will have been translated into the Iceberg table already.
            self.wait_for_compaction()
            # Assert on read log and on iceberg table.
            self.verify_log_and_table(dl)

    @cluster(num_nodes=4)
    #@skip_debug_mode
    @matrix(cloud_storage_type=supported_storage_types())
    def test_compaction(self, cloud_storage_type):
        with DatalakeServices(self.test_ctx,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=False,
                              include_query_engines=[QueryEngineType.TRINO
                                                     ]) as dl:
            self.do_test_compaction(dl)
