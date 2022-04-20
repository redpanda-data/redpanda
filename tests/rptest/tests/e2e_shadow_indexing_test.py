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
from ducktape.services.background_thread import BackgroundThreadService

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


class ShadowIndexingConsumerGroupsTest(RedpandaTest):
    # Run a workload against SI with group consuming.
    # In the past, a customer has run into issues with group consuming on systems
    # with SI enabled
    # This class ports Test7 from https://github.com/redpanda-data/redpanda/issues/3572
    # into ducktape.
    segment_size = 1048576
    num_groups = 2
    groups = [f'group-{i}' for i in range(num_groups)]
    topics = [TopicSpec(partition_count=5) for i in range(num_groups)]

    # 1k messages of size 2**15
    # is ~32MB of data. Using ~32MB of data
    # because RpkConsumer fails at
    # GBs of data.
    msg_size = 2**15
    msg_count = 1000

    def __init__(self, test_context):
        self._si_settings = SISettings(
            log_segment_size=self.segment_size,
            cloud_storage_cache_size=20 * 2**30,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False)

        super(ShadowIndexingConsumerGroupsTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             si_settings=self._si_settings)

        self._exceptions = [None] * self.num_groups

    def _worker(self, idx):
        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topics[idx].name,
                                             self.msg_size, self.msg_count)
        # Using RpkConsumer because the si-verifier (underneath FranzGoVerifier)
        # does not support consumer groups
        consumer = RpkConsumer(self.test_context,
                               self.redpanda,
                               self.topics[idx].name,
                               group=self.groups[idx],
                               num_msgs=self.msg_count)

        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        try:
            producer.start(clean=False)
            await_minimum_produced_records(self.redpanda,
                                           producer,
                                           min_acked=self.msg_count / 10)
            await_s3_objects(self.redpanda)

            consumer.start()
            # Wait for consumer to fetch all data
            consumer.wait()
        except Exception as ex:
            self._exceptions[idx] = ex
            return

        # The si-verifier (underneath FranzGoVerifier) writes non-printable bytes
        # as the key and value. Check those.
        key_size = 25

        def check_msgs():

            if len(consumer.messages) < self.msg_count:
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

                if len(value) != self.msg_size:
                    self.logger.debug(
                        f'Value check failed: Expected size {self.msg_size}, got size {len(value)}'
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

    @cluster(num_nodes=7, log_allow_list=KGO_ALLOW_LOGS)
    def test_consumer_groups(self):

        await_bucket_creation(self.redpanda,
                              self._si_settings.cloud_storage_bucket)

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


# Original listdir in RedpandaService uses SFTP underneath.
# Unfortunately, ducky hangs when calling SFTP procedures
# while producers are running.
# Therefore, the below is a re-write of listdir using SSH
# only.
def _listdir(node, path, only_dirs=False):
    cmd = f'ls {path}/'
    if only_dirs:
        cmd = f'ls -d {path}/*/'

    try:
        ents = node.account.ssh_output(cmd)
        return ents.decode().strip().split()
    except Exception as ex:
        return []


# A service to collect segment filenames continuously
# in the background.
class ProfileDataDirService(BackgroundThreadService):
    def __init__(self, test_context, redpanda, topic):
        super(ProfileDataDirService, self).__init__(test_context, num_nodes=1)

        self.segments = []

        self.redpanda = redpanda

        self.topic = topic

        self.running = True

    def _find_segment_files_on_node(self, topic_path, node, segments):
        # Get the partition directories
        partition_dirs = _listdir(node, topic_path, only_dirs=True)
        self.logger.debug(
            f'node: {node.name}, path: {topic_path}, partition dirs: {partition_dirs}'
        )

        for partition_path in partition_dirs:
            files = _listdir(node, partition_path)

            self.logger.debug(
                f'node: {node.name}, path: {partition_path}, files: {files}')
            for _file in files:
                # The regex is from src/v/storage/fs_utils.h::parse_segment_filename
                match = re.match(r'(^(\d+)-(\d+)-([\x00-\x7F]+).log$)', _file)
                if match and _file not in segments:
                    self.logger.debug(f'Adding {_file}')
                    segments.append(_file)

    def _find_segment_files_on_all_nodes(self, segments):
        topic_path = f'/var/lib/redpanda/data/kafka/{self.topic}'

        for node in self.redpanda.nodes:
            self._find_segment_files_on_node(topic_path, node, segments)

    def _worker(self, idx, node):
        # Make sure running is True
        self.running = True
        while self.running:
            self._find_segment_files_on_all_nodes(self.segments)
            time.sleep(0.1)

    def stop_node(self, node):
        self.running = False


class ShadowIndexingCachedSegmentsTest(PreallocNodesTest):
    # Check that the segment files loaded into the SI cache
    # were also within the local data dir before
    # retention deleted the segments.
    # This class ports Test8 from https://github.com/redpanda-data/redpanda/issues/3572
    # into ducktape.
    segment_size = 1048576  # 1MB
    topics = [TopicSpec(partition_count=1)]

    def __init__(self, test_context):
        self._si_settings = SISettings(
            log_segment_size=self.segment_size,
            cloud_storage_cache_size=20 * 2**30,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False)

        super(ShadowIndexingCachedSegmentsTest,
              self).__init__(test_context=test_context,
                             node_prealloc_count=1,
                             num_brokers=3,
                             si_settings=self._si_settings)

    def _check_si_cache_before_fetch(self):
        # On all nodes, the SI cache should be empty or not exist
        for node in self.redpanda.nodes:
            contents = _listdir(node,
                                '/var/lib/redpanda/data/cloud_storage_cache')

            if contents != []:
                raise FileNotFoundError(f'{node.name} SI cache not empty')

    def _check_si_cache_after_fetch(self, data_dir_segments):
        # Sample file path:
        # /var/lib/redpanda/data/cloud_storage_cache/<some string>/kafka/<topic name>/<partition>/<segment file>
        root = '/var/lib/redpanda/data/cloud_storage_cache'

        segment_files = []

        # Get the paths to the partition dirs within the
        # SI cache
        for node in self.redpanda.nodes:
            partitions_on_node = []
            some_dirs = _listdir(node, root, only_dirs=True)

            for d in some_dirs:
                path = f'{d}kafka/{self.topic}'
                partition_dirs = _listdir(node, path, only_dirs=True)

                # Merge the lists of partitions together.
                partitions_on_node += partition_dirs

            for partition_dir in partitions_on_node:
                files = _listdir(node, partition_dir)
                self.logger.debug(files)

                # The data within the SI cache is a .log.N segment file
                # where N is an positive integer
                # Trim off the .N of the segment file
                # Also, ignore the .index file in the SI cache
                temp = []
                for _file in files:
                    if '.index' not in _file:
                        temp.append(_file[:-2])

                # Merge the list of files
                segment_files += temp

        for segment_file in segment_files:
            match = re.match(r'(^(\d+)-(\d+)-([\x00-\x7F]+).log$)',
                             segment_file)
            if match is None:
                raise Exception(f'Invalid segment filename {segment_file}')

            if segment_file not in data_dir_segments:
                raise FileNotFoundError(f'{segment_file} not in data dir')

    @ok_to_fail  # NoSuchBucket error inconsistently causes failure
    @cluster(num_nodes=6, log_allow_list=KGO_ALLOW_LOGS)
    def test_cached_segments(self):
        # 1k messages of size 2**15
        # is ~32MB of data.
        msg_size = 2**15
        msg_count = 1000

        # Node alloc is first because CI fails in the debug pipeline due to
        # the warning message "Test requested X nodes, used only Y"
        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             self.preallocated_nodes)

        consumer = RpkConsumer(self.test_context,
                               self.redpanda,
                               self.topic,
                               num_msgs=msg_count)

        # Need to profile the data dir from the beginning
        # or else we may miss some segment files
        data_dir_monitor = ProfileDataDirService(self.test_context,
                                                 self.redpanda, self.topic)

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
        # Small retention.ms so we do not have to wait that long
        # for segment removal
        rpk.alter_topic_config(self.topic, 'retention.ms', '120000')

        data_dir_monitor.start()

        # Produce some data
        producer.start(clean=False)
        await_minimum_produced_records(self.redpanda,
                                       producer,
                                       min_acked=msg_count / 10)
        await_s3_objects(self.redpanda)
        producer.wait()

        # Retention will kick-in soon. So suspend
        # execution until only a few segments are left
        # in the data dir.
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=1)

        self._check_si_cache_before_fetch()

        # Fetch the data. The data should go
        # into the SI cache.
        consumer.start()
        consumer.wait()

        assert len(consumer.messages) == msg_count

        data_dir_monitor.stop()
        data_dir_monitor.wait()

        # The SI cache should have segments in it.
        # Check if those segment files existed before retention
        self._check_si_cache_after_fetch(data_dir_monitor.segments)