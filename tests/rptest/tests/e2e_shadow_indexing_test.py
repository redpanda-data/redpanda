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
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import RedpandaService
from rptest.util import Scale
from rptest.archival.s3_client import S3Client
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import (
    produce_until_segments,
    wait_for_segments_removal,
)
from rptest.clients.rpk import RpkTool
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer
from rptest.services.franz_go_verifiable_services import ServiceStatus
from rptest.services.rpk_consumer import RpkConsumer

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


class MoreE2EShadowIndexingTest(EndToEndTest):
    # Over the last couple months, engineers were tasked to identify failures with one of Redpanda's latest features, shadow indexing.
    # Many new tests and workloads were made as a result of this effort. These tests, however, were setup and run manually.
    # This class ports many of those tests to ducktape.
    # For more details, see https://github.com/redpanda-data/redpanda/issues/3572
    segment_size = 1048576  # 1MB
    s3_host_name = "minio-s3"
    s3_access_key = "panda-user"
    s3_secret_key = "panda-secret"
    s3_region = "panda-region"
    s3_topic_name = "panda-topic"
    topics = []
    groups = []
    num_groups = 2

    def __init__(self, test_context):
        super(MoreE2EShadowIndexingTest,
              self).__init__(test_context=test_context)

        self.s3_bucket_name = f"panda-bucket-{uuid.uuid1()}"
        self._extra_rp_conf = dict(
            cloud_storage_enabled=True,
            cloud_storage_access_key=self.s3_access_key,
            cloud_storage_secret_key=self.s3_secret_key,
            cloud_storage_region=self.s3_region,
            cloud_storage_bucket=self.s3_bucket_name,
            cloud_storage_disable_tls=True,
            cloud_storage_api_endpoint=self.s3_host_name,
            cloud_storage_api_endpoint_port=9000,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
        )

        if test_context.function_name == 'test_si_cache_limit':
            self.segment_size = 2**30
            self._extra_rp_conf.update(cloud_storage_cache_size=5 *
                                       self.segment_size)

        elif test_context.function_name == 'test_infinite_retention':
            self.segment_size = 2**30

        self._extra_rp_conf.update(log_segment_size=self.segment_size)

        self.scale = Scale(test_context)
        self.redpanda = RedpandaService(
            context=test_context,
            num_brokers=3,
            extra_rp_conf=self._extra_rp_conf,
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.s3_client = S3Client(
            region=self.s3_region,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            endpoint=f"http://{self.s3_host_name}:9000",
            logger=self.logger,
        )

        # Used in the consumer groups tests
        self._exceptions = [None] * self.num_groups

    def setUp(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        self.s3_client.create_bucket(self.s3_bucket_name)
        self.redpanda.start()

        if self.test_context.function_name == 'test_consumer_groups':
            self.topics = [
                TopicSpec(partition_count=5) for i in range(self.num_groups)
            ]
            self.groups = [f'group-{i}' for i in range(self.num_groups)]

            for spec in self.topics:
                self.kafka_tools.create_topic(spec)
                self.topic = spec.name
        else:
            spec = TopicSpec(partition_count=5)
            self.kafka_tools.create_topic(spec)
            self.topic = spec.name
            self.topics.append(spec)

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)

    def _gen_manifests(self, msg_size, custom_node):
        # Create manifests
        small_producer = FranzGoVerifiableProducer(self.test_context,
                                                   self.redpanda, self.topic,
                                                   msg_size, 10000,
                                                   custom_node)
        small_producer.start()
        small_producer.wait()

        def check_s3():
            objects = list(self.s3_client.list_objects(self.s3_bucket_name))
            return len(objects) > 0

        wait_until(check_s3,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='Failed to write objects to S3')

    # Setup a topic with infinite retention.
    @cluster(num_nodes=4)
    def test_infinite_retention(self):
        # Node alloc is first because CI fails in the debug pipeline due to
        # the warning message "Test requested X nodes, used only Y"
        fgo_node = self.test_context.cluster.alloc(ClusterSpec.simple_linux(1))
        self.logger.debug(f"Allocated verifier node {fgo_node[0].name}")

        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes', str(5 * 2**30))
        rpk.alter_topic_config(self.topic, 'retention.ms', '-1')

        # 100k messages of size 2**18
        # is ~24GB of data.
        msg_size = 2**18
        msg_count = 100000

        self._gen_manifests(msg_size, fgo_node)

        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             fgo_node)
        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, msg_size, 100, 10,
            fgo_node)

        producer.start(clean=False)
        rand_consumer.start(clean=False)

        producer.wait()
        rand_consumer.shutdown()
        rand_consumer.wait()

    # Run common operations against a cluster
    # while the system is under load (busy).
    @cluster(num_nodes=4)
    def test_create_or_delete_topics_while_busy(self):
        # Node alloc is first because CI fails in the debug pipeline due to
        # the warning message "Test requested X nodes, used only Y"
        fgo_node = self.test_context.cluster.alloc(ClusterSpec.simple_linux(1))
        self.logger.debug(f"Allocated verifier node {fgo_node[0].name}")

        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes',
                               str(self.segment_size))

        # 1k messages of size 2**15
        # is ~32MB of data.
        msg_size = 2**15
        msg_count = 1000

        self._gen_manifests(msg_size, fgo_node)

        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             fgo_node)
        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, msg_size, 100, 10,
            fgo_node)

        producer.start(clean=False)
        rand_consumer.start(clean=False)

        random_topics = []

        # Do random ops until the validation stops
        def create_or_delete_until_producer_fin():
            nonlocal random_topics
            trigger = random.randint(1, 6)

            if trigger == 2:
                some_topic = TopicSpec()
                print(f'Create topic: {some_topic}')
                self.kafka_tools.create_topic(some_topic)
                random_topics.append(some_topic)

            if trigger == 3:
                if len(random_topics) > 0:
                    some_topic = random_topics.pop()
                    print(f'Delete topic: {some_topic}')
                    self.kafka_tools.delete_topic(some_topic.name)

            if trigger == 4:
                random.shuffle(random_topics)

            return producer.status == ServiceStatus.FINISH

        wait_until(create_or_delete_until_producer_fin,
                   timeout_sec=300,
                   backoff_sec=0.5,
                   err_msg='Producer did not finish')

        producer.wait()
        rand_consumer.shutdown()
        rand_consumer.wait()

    def _worker(self, idx):
        # 1k messages of size 2**15
        # is ~32MB of data.
        msg_size = 2**15
        msg_count = 1000
        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topics[idx].name, msg_size,
                                             msg_count)
        # Using RpkConsumer because the si-verifier (underneath FranzGoVerifier)
        # does not support consumer groups
        consumer = RpkConsumer(self.test_context,
                               self.redpanda,
                               self.topics[idx].name,
                               group=self.groups[idx],
                               num_msgs=msg_count)

        producer.start(clean=False)
        producer.wait()

        consumer.start()
        consumer.wait()

        # The si-verifier (underneath FranzGoVerifier) writes non-printable bytes
        # as the key and value. Check those.
        key_size = 25

        def check_msgs():

            if len(consumer.messages) < msg_count:
                return False

            self.logger.debug(f'Len: {len(consumer.messages)}')

            for msg in consumer.messages:
                topic = msg['topic']
                key = msg['key']
                value = msg['value']

                if topic != self.topics[idx].name:
                    self.logger.debug(f'Topic check failed: {topic}')
                    return False

                if len(key) != key_size:
                    self.logger.debug(
                        f'Key check failed: Expected size {key_size}, got size {len(key)}'
                    )
                    return False

                if len(value) != msg_size:
                    self.logger.debug(
                        f'Value check failed: Expected size {msg_size}, got size {len(value)}'
                    )
                    return False

            return True

        try:
            wait_until(check_msgs,
                       timeout_sec=300,
                       backoff_sec=1,
                       err_msg='consumer failed to fetch all messages')
        except Exception as ex:
            self._exceptions[idx] = ex

        producer.stop()
        consumer.stop()

    # Run a workload against SI with group consuming.
    # In the past, a customer has run into issues with group consuming on systems
    # with SI enabled
    @cluster(num_nodes=7)
    def test_consumer_groups(self):
        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        for spec in self.topics:
            rpk.alter_topic_config(spec.name, 'redpanda.remote.write', 'true')
            rpk.alter_topic_config(spec.name, 'redpanda.remote.read', 'true')
            rpk.alter_topic_config(spec.name, 'retention.bytes',
                                   str(self.segment_size))
        workers = []
        for idx in range(self.num_groups):
            th = threading.Thread(name=f'LoadGen-{idx}',
                                  target=self._worker,
                                  args=(idx, ))
            th.start()
            workers.append(th)

        for th in workers:
            th.join()

        for ex in self._exceptions:
            if ex:
                raise ex