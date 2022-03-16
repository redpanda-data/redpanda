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
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer, ManifestMaker
from rptest.services.franz_go_verifiable_services import ServiceStatus
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest

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


class MoreE2EShadowIndexingTest(RedpandaTest):
    # Over the last couple months, engineers were tasked to identify failures with one of Redpanda's latest features, shadow indexing.
    # Many new tests and workloads were made as a result of this effort. These tests, however, were setup and run manually.
    # This class ports many of those tests to ducktape.
    # For more details, see https://github.com/redpanda-data/redpanda/issues/3572
    segment_size = 1048576  # 1MB
    topics = []
    groups = []
    num_groups = 2

    def __init__(self, test_context):

        # Cache limit test and infinite retention have
        # segment size of 1G
        if test_context.function_name == 'test_si_cache_limit' or test_context.function_name == 'test_infinite_retention':
            self.segment_size = 2**30

        # All tests use 20GB cache except or test_si_cache_limit.
        # 20GB is the default size in RP docs.
        if test_context.function_name == 'test_si_cache_limit':
            self.si_settings = SISettings(log_segment_size=self.segment_size,
                                          cloud_storage_cache_size=5 *
                                          self.segment_size)
        else:
            self.si_settings = SISettings(log_segment_size=self.segment_size,
                                          cloud_storage_cache_size=20 * 2**30)

        super(MoreE2EShadowIndexingTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf={
                                 'disable_metrics': True,
                                 'election_timeout_ms': 5000,
                                 'raft_heartbeat_interval_ms': 500,
                             },
                             si_settings=self.si_settings)

        self.fgo_node = self.test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))
        self.logger.debug(f"Allocated verifier node {self.fgo_node[0].name}")

        # Used in the consumer groups tests
        self._exceptions = [None] * self.num_groups

    def setUp(self):
        if self.test_context.function_name == 'test_consumer_groups':
            self.topics = [
                TopicSpec(partition_count=5) for i in range(self.num_groups)
            ]
            self.groups = [f'group-{i}' for i in range(self.num_groups)]
        else:
            self.topics = [TopicSpec(partition_count=5)]

        super().setUp()

    def _gen_manifests(self, msg_size):
        # Create manifests
        small_producer = ManifestMaker(self.test_context, self.redpanda,
                                       self.topics, msg_size, 10000,
                                       self.fgo_node)
        small_producer.start()
        small_producer.wait()

        def check_s3():
            objects = list(self.redpanda.get_objects_from_si())
            return len(objects) > 0

        wait_until(check_s3,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='Failed to write objects to S3')

    def free_nodes(self):
        # Free the normally allocated nodes (e.g. RedpandaService)
        super().free_nodes()

        assert len(self.fgo_node) == 1

        # The verifier opens huge numbers of connections, which can interfere
        # with subsequent tests' use of the node.  Clear them down first.
        wait_until(lambda: self.redpanda.sockets_clear(self.fgo_node[0]),
                   timeout_sec=120,
                   backoff_sec=10)

        # Free the hand-allocated node that we share between the various
        # verifier services
        self.logger.debug(f"Freeing verifier node {self.fgo_node[0].name}")
        self.test_context.cluster.free_single(self.fgo_node[0])

    # Setup a topic with infinite retention.
    @cluster(num_nodes=4)
    def test_infinite_retention(self):
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

        self._gen_manifests(msg_size)

        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             self.fgo_node)
        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, msg_size, 100, 10,
            self.fgo_node)

        producer.start(clean=False)
        rand_consumer.start(clean=False)

        producer.wait()
        rand_consumer.shutdown()
        rand_consumer.wait()

    # Run common operations against a cluster
    # while the system is under load (busy).
    @cluster(num_nodes=4)
    def test_create_or_delete_topics_while_busy(self):
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

        self._gen_manifests(msg_size)

        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             self.fgo_node)
        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, msg_size, 100, 10,
            self.fgo_node)

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
                self.client().create_topic(some_topic)
                random_topics.append(some_topic)

            if trigger == 3:
                if len(random_topics) > 0:
                    some_topic = random_topics.pop()
                    print(f'Delete topic: {some_topic}')
                    self.client().delete_topic(some_topic.name)

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
    @cluster(num_nodes=8)
    def test_consumer_groups(self):
        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        for spec in self.topics:
            rpk.alter_topic_config(spec.name, 'redpanda.remote.write', 'true')
            rpk.alter_topic_config(spec.name, 'redpanda.remote.read', 'true')
            rpk.alter_topic_config(spec.name, 'retention.bytes',
                                   str(self.segment_size))

        self._gen_manifests(2**15)

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

    def _listdir(self, path, node, only_dirs=False):
        try:
            ents = node.account.sftp_client.listdir(path)
        except FileNotFoundError:
            # The SI cache doesn't exist until something
            # is fetched from the bucket. So the
            # FileNotFoundError is possible.
            return []

        if not only_dirs:
            return ents
        paths = map(lambda fn: (fn, os.path.join(path, fn)), ents)

        def safe_isdir(path):
            try:
                return node.account.isdir(path)
            except FileNotFoundError:
                # Things that no longer exist are also no longer directories
                return False

        return [p[0] for p in paths if safe_isdir(p[1])]

    # Useful for debugging
    def _walk_path(self, root):
        for node in self.redpanda.nodes:
            dirs = [root]

            # Simple dir walk implementation.
            while len(dirs) > 0:
                path = dirs.pop()
                contents = self._listdir(path, node)
                self.logger.debug(
                    f'node: {node.name}, path: {path}, contents: {contents}')

                sub_dirs = self._listdir(path, node, only_dirs=True)
                for sub_dir in sub_dirs:
                    dirs.append(f'{path}/{sub_dir}')

    def _find_segment_files_on_node(self, topic_path, node, segments):
        # Get the partition directories
        partition_dirs = self._listdir(topic_path, node, only_dirs=True)
        self.logger.debug(
            f'node: {node.name}, path: {topic_path}, partition dirs: {partition_dirs}'
        )

        for part_dir in partition_dirs:
            part_path = f'{topic_path}/{part_dir}'
            files = self._listdir(part_path, node)

            self.logger.debug(
                f'node: {node.name}, path: {part_path}, files: {files}')
            for _file in files:
                self.logger.debug(f'file: {_file}')
                # The regex is from src/v/storage/fs_utils.h::parse_segment_filename
                match = re.match(r'(^(\d+)-(\d+)-([\x00-\x7F]+).log$)', _file)
                if match:
                    segments.append(_file)

    def _find_segment_files_on_all_nodes(self, segments):
        topic_path = f'/var/lib/redpanda/data/kafka/{self.topic}'

        for node in self.redpanda.nodes:
            self._find_segment_files_on_node(topic_path, node, segments)

    def _check_si_cache_before_fetch(self):
        # SI cache should be empty on all nodes
        for node in self.redpanda.nodes:
            contents = self._listdir(
                '/var/lib/redpanda/data/cloud_storage_cache', node)
            assert contents == [], f'SI cache on {node.name} is not empty before fetch'

    def _check_si_cache_after_fetch(self, data_dir_segments):
        # Sample file path:
        # /var/lib/redpanda/data/cloud_storage_cache/<some string>/kafka/<topic name>/<partition>/<segment file>
        root = '/var/lib/redpanda/data/cloud_storage_cache'

        segment_files = []

        # Get the paths to the partition dirs within the
        # SI cache
        for node in self.redpanda.nodes:
            all_parts_dirs = []
            some_dirs = self._listdir(root, node, only_dirs=True)

            for d in some_dirs:
                path = f'{root}/{d}/kafka/{self.topic}'
                parts_dirs = self._listdir(path, node, only_dirs=True)

                # Merge the lists of partitions together.
                all_parts_dirs += parts_dirs

            for part_dir in all_parts_dirs:
                path = f'{root}/{d}/kafka/{self.topic}/{part_dir}'
                files = self._listdir(path, node)

                # The data within the SI cache is a .log.N segment file
                # where N is an positive integer
                # Trim off the .N of the segment file
                temp = [_file[:-2] for _file in files]

                # Merge the list of files
                segment_files += temp

        for segment_file in segment_files:
            match = re.match(r'(^(\d+)-(\d+)-([\x00-\x7F]+).log$)',
                             segment_file)
            assert match, f'Invalid segment filename {segment_file}'
            assert segment_file in data_dir_segments, f'{segment_file} was not in data dir'

    @cluster(num_nodes=5)
    def test_data_loss(self):
        # Creating the producer/consumers first because CI fails
        # in the debug pipeline due to the warning message
        # "Test requested X nodes, used only Y"

        # 1k messages of size 2**15
        # is ~32MB of data.
        msg_size = 2**15
        msg_count = 1000
        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             self.fgo_node)

        consumer = RpkConsumer(self.test_context,
                               self.redpanda,
                               self.topic,
                               num_msgs=msg_count)

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
        # Small retention.ms so we do not have to wait that long
        # for segment removal
        retention_ms = 120000
        rpk.alter_topic_config(self.topic, 'retention.ms', str(retention_ms))

        self._gen_manifests(msg_size)

        # Produce some data
        producer.start(clean=False)
        producer.wait()

        # Profile the data dir
        self.logger.debug('Before retention')
        data_dir_segments = []
        self._find_segment_files_on_all_nodes(data_dir_segments)

        # Wait for retention to kick in.
        time.sleep((retention_ms / 1000))

        self.logger.debug('Before fetch')
        self._check_si_cache_before_fetch()

        # Fetch the data. The data should go
        # into the SI cache.
        consumer.start()
        consumer.wait()

        assert len(consumer.messages) == msg_count

        # The SI cache should have segments in it.
        # Check if those segment files existed before retention
        self.logger.debug('After fetch')
        self._check_si_cache_after_fetch(data_dir_segments)

    # Test the SI cache limit
    @cluster(num_nodes=4)
    def test_si_cache_limit(self):
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        # Remote write/read set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes',
                               str(self.segment_size))

        # 100k messages of size 2**18
        # is ~24GB of data.
        msg_size = 2**18
        msg_count = 100000

        self._gen_manifests(msg_size)

        producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                             self.topic, msg_size, msg_count,
                                             self.fgo_node)

        # The SI cache is set to 5GB in size. Test the cache limit by
        # running 8 readers that consume 1GB segments effectively forcing
        # ~8GB of IO. The readers read from random offsets.
        rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, msg_size, 10, 8,
            self.fgo_node)

        producer.start(clean=False)
        rand_consumer.start(clean=False)

        producer.wait()
        rand_consumer.shutdown()
        rand_consumer.wait()

        # Return True if x is within the percent threshold of
        # the given size
        def in_percent_threshold(x, size, threshold):
            percent_of_size = size * (threshold / 100)
            percent_threshold = size + percent_of_size
            self.logger.debug(f'Percent threshold: {percent_threshold}')
            return x < percent_threshold

        # Check SI cache size on all nodes
        def check_si_cache_size():
            is_cache_size_respected = True

            for node in self.redpanda.nodes:
                cmd = 'df --block-size=1 /var/lib/redpanda/data/cloud_storage_cache | awk \'{print $3}\' | tail -n1'
                result = node.account.ssh_output(cmd, timeout_sec=45).decode()
                df = float(result)

                # Use -b so we get values in bytes. This flag also accounts
                #for holes in "sparse" files since -b uses --apparent-size underneath
                cmd = 'du -sb /var/lib/redpanda/data/cloud_storage_cache | awk \'{print $1}\''
                result = node.account.ssh_output(cmd, timeout_sec=45).decode()
                du = float(result)

                self.logger.debug(f'{node.name}, df: {df}, du: {du}')

                # Disk usage on si cache should be no more than 5%
                # larger than the configured cache size. In general,
                # 5% is an OK place to start.
                if not in_percent_threshold(
                        du, self.si_settings.cloud_storage_cache_size,
                        threshold=5):
                    is_cache_size_respected = False

            return is_cache_size_respected

        wait_until(check_si_cache_size,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='Exceeded SI cache size on a node')

        # Check open files count on all nodes
        file_counts = {node.name: 0 for node in self.redpanda.nodes}

        def is_open_file_count_increasing():
            is_count_stable = True
            for node in self.redpanda.nodes:
                files = self.redpanda.lsof_node(node)
                file_count = sum(1 for _ in files)
                self.logger.debug(f'{node.name} open file count: {file_count}')

                nonlocal file_counts
                if file_count > file_counts[node.name]:
                    file_counts[node.name] = file_count
                    is_count_stable = False

            return is_count_stable

        wait_until(is_open_file_count_increasing,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='Open file count is increasing on a node')
