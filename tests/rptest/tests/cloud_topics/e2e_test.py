# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import TestContext
from typing import Any
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from collections.abc import Iterable

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.admin.v2 import Admin, metastore_pb, ntp_pb
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import (
    SISettings,
    make_redpanda_service,
    MetricsEndpoint,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale
import rptest.tests.cloud_topics.utils as ct_utils


class EndToEndCloudTopicsBase(EndToEndTest):
    s3_topic_name = "panda_topic"

    num_brokers = 3

    topics = (
        TopicSpec(
            name=s3_topic_name,
            partition_count=5,
            replication_factor=3,
        ),
    )

    rpk: RpkTool

    def __init__(
        self,
        test_context: TestContext,
        extra_rp_conf: dict[str, Any] | None = None,
        environment: dict[str, str] | None = None,
    ):
        super(EndToEndCloudTopicsBase, self).__init__(test_context=test_context)

        self.test_context = test_context
        self.topic = self.s3_topic_name

        conf = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "enable_cluster_metadata_upload_loop": False,
        }

        if extra_rp_conf:
            for k, v in conf.items():
                extra_rp_conf[k] = v
        else:
            extra_rp_conf = conf

        self.si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)
        self.scale = Scale(test_context)

        self.redpanda = make_redpanda_service(
            context=self.test_context,
            num_brokers=self.num_brokers,
            si_settings=self.si_settings,
            extra_rp_conf=extra_rp_conf,
            environment=environment,
        )
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        # Allow tests to select storage mode via @matrix(storage_mode=...).
        # Default to cloud if not specified.
        storage_mode = (self.test_context.injected_args or {}).get(
            "storage_mode", TopicSpec.STORAGE_MODE_CLOUD
        )
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )
        for topic in self.topics:
            config = {
                TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                "cleanup.policy": topic.cleanup_policy,
            }
            if topic.min_cleanable_dirty_ratio is not None:
                config["min.cleanable.dirty.ratio"] = topic.min_cleanable_dirty_ratio
            if topic.delete_retention_ms is not None:
                config["delete.retention.ms"] = topic.delete_retention_ms
            self.rpk.create_topic(
                topic=topic.name,
                partitions=topic.partition_count,
                replicas=topic.replication_factor,
                config=config,
            )

    def wait_until_reconciled(self, topic: str, partition: int):
        def get_offsets():
            last_record: int | None = None
            output = self.rpk.consume(
                topic,
                partition=partition,
                offset=":end",
                format="%o\n",
                read_committed=True,
            )
            for line in output.splitlines():
                last_record = int(line)
            metastore = self.admin.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
            )
            return metastore.get_offsets(req=req).offsets.next_offset, last_record

        def is_reconciled() -> bool:
            next_offset, last_record = get_offsets()
            # Check the last observable record's offset against the next offset expected.
            # For transactions, this could be much less than the HWM if there are aborts.
            return (next_offset - 1) == last_record

        def message() -> str:
            try:
                next_offset, last_record = get_offsets()
                return f"failed to reconcile all data: topic={topic}, partition={partition}, last_record={last_record}, next_offset={next_offset}"
            except Exception:
                return f"failed to reconcile all data: topic={topic}, partition={partition}, unable to fetch offsets"

        wait_until(
            condition=is_reconciled,
            timeout_sec=60,
            backoff_sec=5,
            err_msg=message,
            retry_on_exc=True,
        )

    def wait_until_all_reconciled(self, topics: Iterable[TopicSpec] | None = None):
        for topic in topics or self.topics:
            for partition in range(topic.partition_count):
                self.wait_until_reconciled(topic=topic.name, partition=partition)


class EndToEndCloudTopicsTest(EndToEndCloudTopicsBase):
    def __init__(self, test_context, extra_rp_conf=None, env=None):
        super(EndToEndCloudTopicsTest, self).__init__(test_context, extra_rp_conf, env)

    def await_num_produced(self, min_records, timeout_sec=120):
        wait_until(
            lambda: self.producer.num_acked > min_records,
            timeout_sec=timeout_sec,
            err_msg="Producer failed to produce messages for %ds." % timeout_sec,
        )

    @cluster(num_nodes=5)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_write(self, storage_mode: str):
        self.start_producer()

        self.await_num_produced(min_records=50000)

        self.start_consumer()
        self.run_validation()

        self.wait_until_all_reconciled()

    @cluster(num_nodes=5)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_delete_records(self, storage_mode: str):
        self.start_producer()
        self.await_num_produced(min_records=50000)
        self.producer.stop()
        for part in self.rpk.describe_topic(self.s3_topic_name):
            self.logger.info(
                f"lwm={part.start_offset},hwm={part.high_watermark},lso={part.last_stable_offset}"
            )
        output = self.rpk.trim_prefix(self.s3_topic_name, 35)
        self.logger.info(f"{output}")
        for part in self.rpk.describe_topic(self.s3_topic_name):
            assert part.start_offset == 35, (
                f"expected the start offset to be 35 after, got: {part}"
            )
            self.logger.info(
                f"lwm={part.start_offset},hwm={part.high_watermark},lso={part.last_stable_offset}"
            )
        self.start_consumer()
        self.run_consumer_validation(
            expected_missing_records=35 * self.topics[0].partition_count
        )
        self.wait_until_all_reconciled()

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_get_size(self, storage_mode: str):
        """
        Test that the metastore GetSize RPC returns the correct partition size.

        1. Before any data is written, GetSize should return either 0 or NOT_FOUND
           (partitions are lazily created in the metastore).
        2. After writing data, GetSize should eventually return a positive value.
        """
        topic = self.s3_topic_name
        partition = 0

        def get_partition_size() -> int | None:
            return ct_utils.get_l1_partition_size(self.admin, topic, partition)

        # Before writing data, the partition should either not exist or have size 0
        initial_size = get_partition_size()
        assert initial_size is None or initial_size == 0, (
            f"Expected partition size to be 0 or not found before writing data, "
            f"got {initial_size}"
        )
        self.logger.info(
            f"Initial partition size: {initial_size} (None means not found)"
        )

        # Write data to the topic
        self.start_producer()
        self.await_num_produced(min_records=50000)
        self.producer.stop()

        # Wait for the data to be reconciled to the metastore
        self.wait_until_reconciled(topic=topic, partition=partition)

        # Waits until the the partition size reaches a reported positive size
        ct_utils.wait_until_l1_partition_size(
            self.admin, topic, partition, lambda size: size > 0
        )


class EndToEndCloudTopicsTxTest(EndToEndCloudTopicsBase):
    """Cloud topics end-to-end test with transactions used."""

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=1,
            replication_factor=3,
        ),
    )
    kgo_producer: KgoVerifierProducer
    kgo_consumer: KgoVerifierSeqConsumer

    def __init__(self, test_context, extra_rp_conf=None, env=None):
        super(EndToEndCloudTopicsTxTest, self).__init__(
            test_context, extra_rp_conf, env
        )
        self.msg_size = 4096
        # Use a smaller message count to prevent timeouts
        self.msg_count = 1000
        self.per_transaction = 10

    def start_producer_with_tx(self):
        assert self.redpanda and self.topic
        self.kgo_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            use_transactions=True,
            transaction_abort_rate=0.1,
            msgs_per_transaction=self.per_transaction,
            debug_logs=True,
            tolerate_failed_produce=True,
        )
        self.kgo_producer.start()
        self.kgo_producer.wait()

    def start_consumer_with_tx(self):
        traffic_node = self.kgo_producer.nodes[0]
        assert self.redpanda and self.topic
        self.kgo_consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            self.msg_size,
            loop=False,
            nodes=[traffic_node],
            use_transactions=True,
            debug_logs=True,
            trace_logs=True,
        )
        self.kgo_consumer.start(clean=False)
        self.kgo_consumer.wait()

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_write(self, storage_mode: str):
        self.start_producer_with_tx()
        self.start_consumer_with_tx()
        # Validate by checking stats
        pstatus = self.kgo_producer.produce_status
        cstatus = self.kgo_consumer.consumer_status
        committed_messages = pstatus.acked - pstatus.aborted_transaction_messages
        assert pstatus.acked == self.msg_count
        assert 0 < committed_messages <= self.msg_count
        assert cstatus.validator.valid_reads == committed_messages
        assert cstatus.validator.invalid_reads == 0
        assert cstatus.validator.out_of_scope_invalid_reads == 0
        self.wait_until_all_reconciled(self.topics)


class EndToEndCloudTopicsCompactionTest(EndToEndCloudTopicsBase):
    """Cloud topics end-to-end test with a compacted topic."""

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=1,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            min_cleanable_dirty_ratio=0.0,
            delete_retention_ms=3000,
        ),
    )
    kgo_producer: KgoVerifierProducer
    kgo_consumer: KgoVerifierSeqConsumer

    def __init__(self, test_context):
        key_map_memory_kb = test_context.injected_args[
            "cloud_topics_compaction_key_map_memory_kb"
        ]
        extra_rp_conf = {
            "cloud_topics_compaction_interval_ms": 4000,
            "cloud_topics_compaction_key_map_memory": key_map_memory_kb * 1024,
        }
        environment = {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
        super(EndToEndCloudTopicsCompactionTest, self).__init__(
            test_context,
            extra_rp_conf,
            environment,
        )
        self.msg_size = 4096
        # Use a smaller message count to prevent timeouts
        self.msg_count = 1000
        self.key_set_cardinality = 100
        self.tombstone_probability = 0.5

    def _metric_sum(self, metric_name):
        assert self.redpanda
        return self.redpanda.metric_sum(
            metric_name=metric_name,
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    def get_removed_records(self):
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_records_removed_total"
        )

    def get_log_compactions(self):
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_log_compactions_total"
        )

    def get_managed_logs(self):
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_managed_log_count"
        )

    def produce(self):
        assert self.redpanda
        assert self.topic
        try:
            self.kgo_producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic,
                msg_size=self.msg_size,
                msg_count=self.msg_count,
                key_set_cardinality=self.key_set_cardinality,
                tombstone_probability=self.tombstone_probability,
                validate_latest_values=True,
                tolerate_failed_produce=True,
            )
            self.kgo_producer.start()
            self.kgo_producer.wait_for_latest_value_map()
            self.kgo_producer.wait()
        finally:
            self.kgo_producer.stop()

    def consume(self):
        assert self.redpanda
        assert self.topic
        traffic_node = self.kgo_producer.nodes[0]
        try:
            self.kgo_consumer = KgoVerifierSeqConsumer(
                self.test_context,
                self.redpanda,
                self.topic,
                self.msg_size,
                loop=False,
                compacted=True,
                validate_latest_values=True,
                nodes=[traffic_node],
            )
            self.kgo_consumer.start(clean=False)
            self.kgo_consumer.wait()
        finally:
            self.kgo_consumer.stop()

    @cluster(num_nodes=4)
    @matrix(cloud_topics_compaction_key_map_memory_kb=[3, 10, 128 * 1024])
    def test_compact(self, cloud_topics_compaction_key_map_memory_kb):
        def seen_managed_logs():
            return self.get_managed_logs() > 0

        wait_until(
            seen_managed_logs,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Did not see management of compact-enabled CTPs.",
        )

        num_rounds = 1
        self.prev_log_compactions = 0.0
        self.prev_removed_records = 0.0
        for i in range(0, num_rounds):
            self.produce()

            def seen_compaction():
                log_compactions = self.get_log_compactions()
                res = log_compactions > self.prev_log_compactions
                self.prev_log_compactions = log_compactions
                return res

            wait_until(
                seen_compaction,
                timeout_sec=360,
                backoff_sec=1,
                err_msg="Did not see compaction of managed CTPs.",
            )

            def seen_removed_records():
                removed_records = self.get_removed_records()
                res = removed_records > self.prev_removed_records
                self.prev_removed_records = removed_records
                return res

            wait_until(
                seen_removed_records,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Did not see removed records during compaction of CTPs.",
            )

            def consumed_latest_values():
                try:
                    self.consume()
                    return True
                except Exception:
                    return False

            wait_until(
                consumed_latest_values,
                timeout_sec=360,
                backoff_sec=1,
                err_msg="Did not see a fully compacted CTP log.",
            )
