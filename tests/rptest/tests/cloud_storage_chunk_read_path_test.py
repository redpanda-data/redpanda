import pprint
from threading import Thread
from time import sleep

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.services.admin import Admin
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.kgo_verifier_services import KgoVerifierRandomConsumer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import RedpandaService
from rptest.services.redpanda import SISettings
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import Scale
from rptest.util import wait_for_removal_of_n_segments
from rptest.utils.si_utils import nodes_report_cloud_segments


class CloudStorageChunkReadTest(PreallocNodesTest):
    def __init__(self, test_context):
        self.log_segment_size = 1048576 * 5
        self.test_context = test_context
        self.message_size = 1024
        self.si_settings = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
        )
        self.si_settings.load_context(self.logger, test_context=test_context)

        self.default_chunk_size = 1024 * 256
        super().__init__(test_context=test_context,
                         node_prealloc_count=1,
                         num_brokers=3,
                         si_settings=self.si_settings,
                         extra_rp_conf={
                             'cloud_storage_cache_chunk_size':
                             self.default_chunk_size
                         })

        self.scale = Scale(test_context)
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.topics = (TopicSpec(name='panda-topic',
                                 partition_count=1,
                                 replication_factor=3,
                                 segment_bytes=self.log_segment_size), )

        self.extra_rp_conf = {
            'cloud_storage_cache_chunk_size': self.default_chunk_size
        }
        if not self.redpanda.dedicated_nodes:
            self.extra_rp_conf.update(
                {'cloud_storage_max_segment_readers_per_shard': 10})
        self.redpanda.set_extra_rp_conf(self.extra_rp_conf)

    def setup(self):
        # Do not start redpanda here, let the tests start with custom config options
        pass

    def _set_params_and_start_redpanda(self, **kwargs):
        if kwargs:
            self.extra_rp_conf.update(kwargs)
            self.redpanda.set_extra_rp_conf(self.extra_rp_conf)
        self.redpanda.start()
        self._create_initial_topics()

    def _trim_and_verify(self):
        """
        Use admin API trim hook to validate that the trimming logic can successfully
        hit a target size of zero irrespective of what kind of content we promoted.
        """
        admin = Admin(self.redpanda)
        self.redpanda.for_nodes(
            self.redpanda.nodes, lambda n: admin.cloud_storage_trim(
                byte_limit=0, object_limit=0, node=n))

        for node_storage in self.redpanda.storage().nodes:
            assert node_storage.cache is not None, f"Node {node_storage.name} has no cache stats"

            # The only file to elude the trim should be accesstime tracker
            assert node_storage.cache.objects <= 1, f"Node {node_storage.name} has too many objects: {node_storage.cache.objects}"

            # Byte size will include accesstime file, so we must be tolerant
            assert node_storage.cache.bytes <= 1E6, f"Node {node_storage.name} size is too great: {node_storage.cache.bytes}"

            # No index files should survive a trim to zero
            assert node_storage.cache.indices <= 0, f"Node {node_storage.name} still has {node_storage.cache.indices} index files"

    def _produce_baseline(self,
                          n_segments=20,
                          msg_size=None,
                          msg_count=200000,
                          n_remove=None):
        n_remove = n_remove or n_segments // 2
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size or self.message_size,
                                       msg_count=msg_count,
                                       custom_node=self.preallocated_nodes)
        producer.start()
        wait_until(
            lambda: nodes_report_cloud_segments(self.redpanda, n_segments),
            timeout_sec=180,
            backoff_sec=3)
        producer.stop()
        producer.wait()

        snapshot = self.redpanda.storage(all_nodes=True).segments_by_node(
            "kafka", self.topic, 0)
        for t in self.topics:
            self.client().alter_topic_config(
                t.name, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                self.log_segment_size)

        # Wait for half of the segments to be removed, to exercise the read path
        # when using the sequential consumer later on.
        wait_for_removal_of_n_segments(self.redpanda,
                                       self.topic,
                                       partition_idx=0,
                                       n=n_remove,
                                       original_snapshot=snapshot)

        return producer

    def _remove_indices_from_cloud(self):
        for obj in self.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket, self.topic):
            if obj.key.endswith('.index'):
                self.cloud_storage_client.delete_object(
                    self.si_settings.cloud_storage_bucket, obj.key)

    def _assert_files_in_cache(self, expr: str, must_be_absent=False):
        """
        Asserts that files matching a pattern are present in the cache directory on _any one_ of the started nodes.
        If must_be_absent is True, then the files should be absent on every started node.
        """
        found_files = []
        for node in self.redpanda.started_nodes():
            # Print all files in cache for debugging
            files = [
                l.strip() for l in node.account.ssh_capture(
                    f"""find {self.redpanda.DATA_DIR}/cloud_storage_cache""")
            ]
            self.redpanda.logger.debug(f'files in cache: {files}')

            found_files += [
                l.strip() for l in node.account.ssh_capture(
                    f"""find {self.redpanda.DATA_DIR}/cloud_storage_cache -regex '{expr}'"""
                )
            ]

        if must_be_absent:
            assert not found_files, 'unexpected files in cache dir: ' \
                                    f'{pprint.pformat(found_files)} ' \
                                    f'matching expression {expr}'
        else:
            assert found_files, f'no files in cache dir matching expression {expr}'

    def _assert_not_in_cache(self, expr: str):
        return self._assert_files_in_cache(expr=expr, must_be_absent=True)

    def _assert_in_cache(self, expr: str):
        return self._assert_files_in_cache(expr=expr, must_be_absent=False)

    @cluster(num_nodes=4)
    def test_read_chunks(self):
        self.default_chunk_size = 1048576
        self._set_params_and_start_redpanda(
            cloud_storage_cache_chunk_size=self.default_chunk_size)

        self._produce_baseline()

        rand_cons = KgoVerifierRandomConsumer(self.test_context,
                                              self.redpanda,
                                              self.topic,
                                              0,
                                              50,
                                              10,
                                              nodes=self.preallocated_nodes)

        metric = 'vectorized_cloud_storage_partition_chunk_size'
        m = MetricCheck(self.logger,
                        self.redpanda,
                        self.redpanda.partitions(self.topic)[0].leader,
                        [metric],
                        labels={
                            'namespace': 'kafka',
                            'topic': self.topic,
                            'partition': '0',
                        })
        rand_cons.start()
        rand_cons.wait(timeout_sec=300)
        m.expect([(metric, lambda a, b: a < b <= 2 * self.default_chunk_size)])

        # There should be no log files in cache
        self._assert_not_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')
        # There should be some chunk files in cache
        self._assert_in_cache(f'.*kafka/{self.topic}/.*_chunks/[0-9]+')

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          0,
                                          nodes=self.preallocated_nodes)
        consumer.start()
        consumer.wait(timeout_sec=120)
        self._assert_not_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')

        self._trim_and_verify()

    @cluster(num_nodes=4)
    @parametrize(prefetch=0)
    @parametrize(prefetch=3)
    @parametrize(prefetch=5)
    def test_prefetch_chunks(self, prefetch):
        self.log_segment_size = 1048576 * 10
        self.topics[0].segment_bytes = self.log_segment_size
        self._set_params_and_start_redpanda(
            cloud_storage_chunk_prefetch=prefetch)

        # Smaller messages mean chunks are closer to requested limit. We need more messages to be able
        # to produce the required number of segments
        self._produce_baseline(msg_size=100,
                               n_segments=5,
                               msg_count=200000 * 100,
                               n_remove=1)

        metric = 'vectorized_cloud_storage_partition_chunk_size'
        m = MetricCheck(self.logger,
                        self.redpanda,
                        self.redpanda.partitions(self.topic)[0].leader,
                        [metric],
                        labels={
                            'namespace': 'kafka',
                            'topic': self.topic,
                            'partition': '0',
                        })

        # Cap max bytes to only get the first chunk.
        rpk = RpkTool(self.redpanda)
        rpk.consume(self.topic,
                    partition=0,
                    fetch_max_bytes=100,
                    n=1,
                    offset='start')
        m.expect([(metric, lambda a, b: a <= b <= 2 * self.default_chunk_size)
                  ])

        self._assert_not_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')
        chunk_files = set()
        for node in self.redpanda.started_nodes():
            chunk_files |= set([
                l.strip() for l in node.account.ssh_capture(
                    f"""find {self.redpanda.DATA_DIR}/cloud_storage_cache """
                    f"""-regex '.*kafka/{self.topic}/.*_chunks/[0-9]+'""")
            ])

        self.logger.info(f'found {len(chunk_files)} chunk files')
        for cf in chunk_files:
            self.logger.debug(f'chunk file: {cf}')

        if prefetch:
            assert len(
                chunk_files
            ) == prefetch + 1, f'prefetch={prefetch} but {len(chunk_files)} chunks: ' \
                               f'{pprint.pformat(chunk_files)} found in cache, expected {prefetch + 1}'

        self._trim_and_verify()

    @cluster(num_nodes=4)
    def test_fallback_mode(self):
        """
        In fallback mode, redpanda is not able to download the index from cloud storage.
        This should result in fallback mode being engaged, which instructs the leader to
        start downloading full segments. Additionally, the index is generated on the fly
        during download.
        """
        self._set_params_and_start_redpanda()

        self._produce_baseline(n_segments=5)
        self._remove_indices_from_cloud()

        self._consume_baseline()

        # There should be no chunk files in cache in fallback mode
        self._assert_not_in_cache(f'kafka/{self.topic}/.*_chunks/[0-9]+$')

        # There should be some log files in cache in fallback mode
        self._assert_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')

        # Index files should have been generated during full segment download.
        self._assert_in_cache(f'.*kafka/{self.topic}/.*index$')

        self._trim_and_verify()

    def _consume_baseline(self, timeout=60, max_msgs=None):
        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          0,
                                          nodes=self.preallocated_nodes,
                                          max_msgs=max_msgs)
        consumer.start()
        consumer.wait(timeout_sec=timeout)

    @cluster(num_nodes=4)
    def test_read_when_chunk_api_disabled(self):
        self._set_params_and_start_redpanda(
            cloud_storage_disable_chunk_reads=True)
        self._produce_baseline(n_segments=5)
        self._consume_baseline()

        self._assert_not_in_cache(f'kafka/{self.topic}/.*_chunks/[0-9]+$')
        self._assert_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')
        self._assert_in_cache(f'.*kafka/{self.topic}/.*index$')

        self._trim_and_verify()

    @cluster(num_nodes=4, log_allow_list=["Exceeded cache size limit"])
    def test_read_when_cache_smaller_than_segment_size(self):
        self.si_settings.cloud_storage_cache_size = 1048576 * 4
        self.redpanda.set_si_settings(self.si_settings)

        # Avoid overshooting the cache with a smaller chunk size
        self._set_params_and_start_redpanda(
            cloud_storage_cache_chunk_size=1048576 // 2)

        n_segments = 4
        msg_count = (self.log_segment_size *
                     (n_segments + 1)) // self.message_size

        self.logger.info(f'producing {msg_count} messages')
        self._produce_baseline(n_segments=4, msg_count=msg_count)

        # Read roughly 2 segments worth of data from the cloud
        read_count = msg_count // 2
        self.logger.info(f'reading {read_count} messages')

        observe_cache_dir = ObserveCacheDir(self.redpanda, self.topic)
        observe_cache_dir.start()

        self._consume_baseline(timeout=180, max_msgs=read_count)
        observe_cache_dir.stop()
        observe_cache_dir.join(timeout=10)
        assert not observe_cache_dir.is_alive(
        ), 'cache observer is unexpectedly alive'

        self._assert_not_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')
        self._assert_in_cache(f'.*kafka/{self.topic}/.*_chunks/[0-9]+')

        chunks_found = observe_cache_dir.chunks
        for file in chunks_found:
            self.logger.info(f'chunk file {file}')

        cache_sizes = observe_cache_dir.sizes
        self.logger.info(f'chunk counts: {pprint.pformat(cache_sizes)}')

        # We read a little over two segments, and each segment can have around 5 chunks.
        # We should observe 10 chunks or more in the cache.
        assert len(
            chunks_found
        ) >= 10, f'expected to find at least 10 chunks, found {len(chunks_found)}'

        # The cache can hold upto 8 chunks at a time, assert that we did not overshoot this limit.
        # Account for potential trimming by increasing the limit by a bit.
        max_chunks_at_a_time_in_cache = max(cache_sizes)
        assert max_chunks_at_a_time_in_cache <= 9, f'found {max_chunks_at_a_time_in_cache} in cache, ' \
                                                   f'which can hold only 8 chunks at a time'

        self._trim_and_verify()

    @cluster(num_nodes=4)
    def test_read_when_segment_size_smaller_than_chunk_size(self):
        self._set_params_and_start_redpanda(cloud_storage_cache_chunk_size=16 *
                                            1024 * 1024)

        self._produce_baseline(n_segments=20)
        rand_cons = KgoVerifierRandomConsumer(self.test_context,
                                              self.redpanda,
                                              self.topic,
                                              msg_size=0,
                                              rand_read_msgs=50,
                                              parallel=15,
                                              nodes=self.preallocated_nodes)
        rand_cons.start()
        rand_cons.wait(timeout_sec=300)

        self._consume_baseline()

        self._assert_not_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')
        self._assert_in_cache(f'.*kafka/{self.topic}/.*_chunks/[0-9]+')

        self._trim_and_verify()


class ObserveCacheDir(Thread):
    def __init__(self, redpanda: RedpandaService, topic: str):
        super().__init__()
        self.redpanda = redpanda
        self.topic = topic
        self.chunks = set()
        self.sizes = set()
        self.stop_requested = False
        self.sleep_interval = 1

    def run(self) -> None:
        while not self.stop_requested:
            sleep(self.sleep_interval)
            for node in self.redpanda.started_nodes():
                polled_files = set([
                    l.strip() for l in node.account.ssh_capture(
                        f"""find {self.redpanda.DATA_DIR}/cloud_storage_cache """
                        f"""-regex '.*kafka/{self.topic}/.*_chunks/[0-9]+'""")
                ])
                self.sizes.add(len(polled_files))
                self.chunks |= polled_files

    def stop(self):
        self.stop_requested = True
