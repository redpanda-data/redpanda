# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import time

from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import (
    SISettings,
    make_redpanda_service,
)
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale


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

    def __init__(self, test_context, extra_rp_conf=None, environment=None):
        super(EndToEndCloudTopicsBase, self).__init__(test_context=test_context)

        self.test_context = test_context
        self.topic = self.s3_topic_name

        conf = dict(
            cloud_topics_enabled=True,
            enable_cluster_metadata_upload_loop=False,
        )

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

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        for topic in self.topics:
            self.rpk.create_topic(
                topic=topic.name,
                partitions=topic.partition_count,
                replicas=topic.replication_factor,
                config={
                    "redpanda.cloud_topic.enabled": "true",
                },
            )


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
    def test_write(self):
        self.start_producer()

        self.await_num_produced(min_records=50000)

        self.start_consumer()
        self.run_validation()

    @cluster(num_nodes=5)
    def test_delete_records(self):
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
    def test_write(self):
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
