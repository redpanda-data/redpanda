# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

from rptest.services.cluster import cluster
from ducktape.mark import ok_to_fail
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.util import Scale
from rptest.archival.s3_client import S3Client
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import (produce_until_segments, wait_for_segments_removal,
                         await_bucket_creation)
from rptest.clients.rpk import RpkTool
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer
from rptest.services.franz_go_verifiable_services import ServiceStatus, KGO_ALLOW_LOGS
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.prealloc_nodes import PreallocNodesTest

import uuid
import os
import time
import requests
import random
import threading
import re


class EndToEndShadowIndexingTest(EndToEndTest):
    segment_size = 1048576  # 1 Mb
    s3_host_name = "minio-s3"
    s3_access_key = "panda-user"
    s3_secret_key = "panda-secret"
    s3_region = "panda-region"
    s3_topic_name = "panda-topic"
    topics = (TopicSpec(
        name=s3_topic_name,
        partition_count=1,
        replication_factor=3,
    ), )

    def __init__(self, test_context):
        super(EndToEndShadowIndexingTest,
              self).__init__(test_context=test_context)

        self.s3_bucket_name = f"panda-bucket-{uuid.uuid1()}"
        self.topic = EndToEndShadowIndexingTest.s3_topic_name
        self._extra_rp_conf = dict(
            cloud_storage_enabled=True,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
            cloud_storage_access_key=EndToEndShadowIndexingTest.s3_access_key,
            cloud_storage_secret_key=EndToEndShadowIndexingTest.s3_secret_key,
            cloud_storage_region=EndToEndShadowIndexingTest.s3_region,
            cloud_storage_bucket=self.s3_bucket_name,
            cloud_storage_disable_tls=True,
            cloud_storage_api_endpoint=EndToEndShadowIndexingTest.s3_host_name,
            cloud_storage_api_endpoint_port=9000,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=EndToEndShadowIndexingTest.segment_size,  # 1MB
        )

        self.scale = Scale(test_context)
        self.redpanda = RedpandaService(
            context=test_context,
            num_brokers=3,
            extra_rp_conf=self._extra_rp_conf,
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.s3_client = S3Client(
            region=EndToEndShadowIndexingTest.s3_region,
            access_key=EndToEndShadowIndexingTest.s3_access_key,
            secret_key=EndToEndShadowIndexingTest.s3_secret_key,
            endpoint=f"http://{EndToEndShadowIndexingTest.s3_host_name}:9000",
            logger=self.logger,
        )

    def setUp(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        self.s3_client.create_bucket(self.s3_bucket_name)
        self.redpanda.start()
        for topic in EndToEndShadowIndexingTest.topics:
            self.kafka_tools.create_topic(topic)

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)

    @cluster(num_nodes=5)
    def test_write(self):
        """Write at least 10 segments, set retention policy to leave only 5
        segments, wait for segments removal, consume data and run validation,
        that everything that is acked is consumed."""
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_BYTES:
                5 * EndToEndShadowIndexingTest.segment_size,
            },
        )
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=6)

        self.start_consumer()
        self.run_validation()


def await_minimum_produced_records(redpanda, producer, min_acked=0):
    # Block until the producer has
    # written a minimum amount of data.
    wait_until(lambda: producer.produce_status.acked > min_acked,
               timeout_sec=300,
               backoff_sec=5)


def await_s3_objects(redpanda):
    # Check that some data is written to S3
    def check_s3():
        objects = list(redpanda.get_objects_from_si())
        return len(objects) > 0

    wait_until(check_s3,
               timeout_sec=30,
               backoff_sec=1,
               err_msg='Failed to write objects to S3')


class ShadowIndexingInfiniteRetentionTest(PreallocNodesTest):
    # Test infinite retention while SI is enabled.
    # This class ports Test5 from https://github.com/redpanda-data/redpanda/issues/3572
    # into ducktape.
    segment_size = 2**30
    topics = [TopicSpec(partition_count=5)]

    def __init__(self, test_context):
        self._si_settings = SISettings(
            log_segment_size=self.segment_size,
            cloud_storage_cache_size=20 * 2**30,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False)

        super(ShadowIndexingInfiniteRetentionTest,
              self).__init__(test_context=test_context,
                             node_prealloc_count=1,
                             num_brokers=3,
                             si_settings=self._si_settings)

    @cluster(num_nodes=4, log_allow_list=KGO_ALLOW_LOGS)
    def test_infinite_retention(self):
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        await_bucket_creation(self.redpanda,
                              self._si_settings.cloud_storage_bucket)

        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes', '-1')
        rpk.alter_topic_config(self.topic, 'retention.ms', '-1')

        # 100k messages of size 2**18
        # is ~24GB of data.
        msg_size = 2**18
        msg_count = 100000

        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             self.preallocated_nodes)
        producer.start(clean=False)
        await_minimum_produced_records(self.redpanda,
                                       producer,
                                       min_acked=msg_count / 10)
        await_s3_objects(self.redpanda)

        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, msg_size, 100, 10,
            self.preallocated_nodes)
        rand_consumer.start(clean=False)

        producer.wait()
        rand_consumer.shutdown()
        rand_consumer.wait()


class ShadowIndexingWhileBusyTest(PreallocNodesTest):
    # With SI enabled, run common operations against a cluster
    # while the system is under load (busy).
    # This class ports Test6 from https://github.com/redpanda-data/redpanda/issues/3572
    # into ducktape.
    segment_size = 2**30
    topics = [TopicSpec(partition_count=5)]

    def __init__(self, test_context):
        self._si_settings = SISettings(
            log_segment_size=self.segment_size,
            cloud_storage_cache_size=20 * 2**30,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False)

        super(ShadowIndexingWhileBusyTest,
              self).__init__(test_context=test_context,
                             node_prealloc_count=1,
                             num_brokers=3,
                             si_settings=self._si_settings)

    @ok_to_fail  # broken pipe inconsistently causes failure from cloud_storage/remote.cc:108
    @cluster(num_nodes=4, log_allow_list=KGO_ALLOW_LOGS)
    def test_create_or_delete_topics_while_busy(self):
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        await_bucket_creation(self.redpanda,
                              self._si_settings.cloud_storage_bucket)

        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes',
                               str(self.segment_size))

        # 100k messages of size 2**18
        # is ~24GB of data.
        msg_size = 2**18
        msg_count = 100000
        timeout = 600

        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             self.preallocated_nodes)
        producer.start(clean=False)
        await_minimum_produced_records(self.redpanda,
                                       producer,
                                       min_acked=msg_count / 10)
        await_s3_objects(self.redpanda)

        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, msg_size, 100, 10,
            self.preallocated_nodes)

        rand_consumer.start(clean=False)

        random_topics = []

        # Do random ops until the producer stops
        def create_or_delete_until_producer_fin():
            nonlocal random_topics
            trigger = random.randint(1, 2)

            if trigger == 1:
                some_topic = TopicSpec()
                self.logger.debug(f'Create topic: {some_topic}')
                self.client().create_topic(some_topic)
                random_topics.append(some_topic)

            if trigger == 2:
                if len(random_topics) > 0:
                    random.shuffle(random_topics)
                    some_topic = random_topics.pop()
                    self.logger.debug(f'Delete topic: {some_topic}')
                    self.client().delete_topic(some_topic.name)

            return producer.status == ServiceStatus.FINISH

        wait_until(create_or_delete_until_producer_fin,
                   timeout_sec=timeout,
                   backoff_sec=0.5,
                   err_msg='Producer did not finish')

        producer.wait()
        rand_consumer.shutdown()
        rand_consumer.wait()