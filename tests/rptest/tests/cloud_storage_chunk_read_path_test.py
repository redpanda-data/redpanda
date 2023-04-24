from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.redpanda import SISettings, RedpandaService
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import Scale
from rptest.utils.si_utils import nodes_report_cloud_segments


class CloudStorageChunkReadTest(PreallocNodesTest):
    def __init__(self, test_context):
        self.log_segment_size = 1048576 * 10
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

        extra_rp_conf = {'cloud_storage_cache_chunk_size': 1024 * 256}
        if not self.redpanda.dedicated_nodes:
            extra_rp_conf.update({'cloud_storage_max_readers_per_shard': 10})
        self.redpanda.set_extra_rp_conf(extra_rp_conf)

    def setup(self):
        super().setup()
        for t in self.topics:
            self.client().alter_topic_config(
                t.name, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_MS, 1000)

    def teardown(self):
        self.redpanda.cloud_storage_client.empty_bucket(
            self.si_settings.cloud_storage_bucket)

    @cluster(num_nodes=4)
    def test_read_chunks(self):
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=1024,
                                       msg_count=1000 * 1000,
                                       custom_node=self.preallocated_nodes)
        producer.start()
        wait_until(lambda: nodes_report_cloud_segments(self.redpanda, 5),
                   timeout_sec=120,
                   backoff_sec=3)
        producer.stop()
        producer.wait()

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          0,
                                          nodes=self.preallocated_nodes)
        consumer.start()
        consumer.wait(timeout_sec=120)
