import pprint

from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.kgo_verifier_services import KgoVerifierRandomConsumer
from rptest.services.redpanda import SISettings
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import Scale, wait_for_local_storage_truncate
from rptest.utils.si_utils import nodes_report_cloud_segments


class CloudStorageChunkReadTest(PreallocNodesTest):
    def __init__(self, test_context):
        self.log_segment_size = 1048576 * 5
        self.test_context = test_context
        self.si_settings = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
            fast_uploads=True,
        )
        self.si_settings.load_context(self.logger, test_context=test_context)

        super().__init__(
            test_context=test_context,
            node_prealloc_count=1,
            num_brokers=3,
            si_settings=self.si_settings,
            extra_rp_conf={'cloud_storage_cache_chunk_size': 1024 * 256})

        self.scale = Scale(test_context)
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.topics = (TopicSpec(name='panda-topic',
                                 partition_count=1,
                                 replication_factor=3,
                                 segment_bytes=self.log_segment_size), )

        self.extra_rp_conf = {'cloud_storage_cache_chunk_size': 1024 * 256}
        if not self.redpanda.dedicated_nodes:
            self.extra_rp_conf.update(
                {'cloud_storage_max_readers_per_shard': 10})
        self.redpanda.set_extra_rp_conf(self.extra_rp_conf)

    def setup(self):
        # Do not start redpanda here, let the tests start with custom config options
        pass

    def teardown(self):
        self.redpanda.cloud_storage_client.empty_bucket(
            self.si_settings.cloud_storage_bucket)

    def _set_params_and_start_redpanda(self, **kwargs):
        if kwargs:
            self.extra_rp_conf.update(kwargs)
            self.redpanda.set_extra_rp_conf(self.extra_rp_conf)
        self.redpanda.start()
        self._create_initial_topics()

    def _produce_baseline(self, n_segments=20, msg_count=200000):
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=1024,
                                       msg_count=msg_count,
                                       custom_node=self.preallocated_nodes)
        producer.start()
        wait_until(
            lambda: nodes_report_cloud_segments(self.redpanda, n_segments),
            timeout_sec=180,
            backoff_sec=3)
        producer.stop()
        producer.wait()

        for t in self.topics:
            self.client().alter_topic_config(
                t.name, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                self.log_segment_size)

        # Wait for half of the segments to be removed, to exercise the read path
        # when using the sequential consumer later on.
        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        partition_idx=0,
                                        target_bytes=(n_segments // 2) *
                                        self.log_segment_size)

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

    @cluster(num_nodes=4)
    def test_read_chunks(self):
        self._set_params_and_start_redpanda(
            cloud_storage_cache_chunk_size=1048576 * 8)

        self._produce_baseline()

        rand_cons = KgoVerifierRandomConsumer(self.test_context,
                                              self.redpanda,
                                              self.topic,
                                              0,
                                              50,
                                              10,
                                              nodes=self.preallocated_nodes)
        rand_cons.start()
        rand_cons.wait(timeout_sec=300)

        # There should be no log files in cache
        self._assert_files_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$',
                                    must_be_absent=True)
        # There should be some chunk files in cache
        self._assert_files_in_cache(f'.*kafka/{self.topic}/.*_chunks/[0-9]+')

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          0,
                                          nodes=self.preallocated_nodes)
        consumer.start()
        consumer.wait(timeout_sec=120)
        self._assert_files_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$',
                                    must_be_absent=True)

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
        self._assert_files_in_cache(f'kafka/{self.topic}/.*_chunks/[0-9]+$',
                                    must_be_absent=True)

        # There should be some log files in cache in fallback mode
        self._assert_files_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')

        # Index files should have been generated during full segment download.
        self._assert_files_in_cache(f'.*kafka/{self.topic}/.*index$')

    def _consume_baseline(self, timeout=60):
        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          0,
                                          nodes=self.preallocated_nodes)
        consumer.start()
        consumer.wait(timeout_sec=timeout)

    @cluster(num_nodes=4)
    def test_read_when_chunk_api_disabled(self):
        self._set_params_and_start_redpanda(
            cloud_storage_disable_chunk_reads=True)
        self._produce_baseline(n_segments=5)
        self._consume_baseline()

        self._assert_files_in_cache(f'kafka/{self.topic}/.*_chunks/[0-9]+$',
                                    must_be_absent=True)

        self._assert_files_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$')
        self._assert_files_in_cache(f'.*kafka/{self.topic}/.*index$')

    @cluster(num_nodes=4)
    def test_read_when_cache_smaller_than_segment_size(self):
        self.si_settings.cloud_storage_cache_size = 1048576 * 4
        self.redpanda.set_si_settings(self.si_settings)
        self._set_params_and_start_redpanda(
            cloud_storage_cache_chunk_size=1048576)

        self._produce_baseline(n_segments=6, msg_count=50000)
        self._consume_baseline(timeout=180)

        self._assert_files_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$',
                                    must_be_absent=True)
        self._assert_files_in_cache(f'.*kafka/{self.topic}/.*_chunks/[0-9]+')

    @cluster(num_nodes=4)
    def test_read_when_segment_size_smaller_than_chunk_size(self):
        self.topics[0].segment_bytes = 512 * 1024
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

        self._assert_files_in_cache(fr'.*kafka/{self.topic}/.*\.log\.[0-9]+$',
                                    must_be_absent=True)
        self._assert_files_in_cache(f'.*kafka/{self.topic}/.*_chunks/[0-9]+')
