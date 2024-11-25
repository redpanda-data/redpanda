from rptest.archival.s3_client import S3Client
from rptest.clients.default import TopicSpec
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog
from rptest.services.cluster import cluster

from rptest.services.redpanda import SISettings
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer

from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until
import time
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix


class RestCatalogConnectionTest(RedpandaTest):
    def __init__(self, test_context):
        self._topic = None

        super(RestCatalogConnectionTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(test_context,
                                   cloud_storage_enable_remote_read=False,
                                   cloud_storage_enable_remote_write=False),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 10000
            })
        self.catalog_service = IcebergRESTCatalog(
            test_context,
            cloud_storage_bucket=self.si_settings.cloud_storage_bucket,
            filesystem_wrapper_mode=False)

    def setUp(self):
        self.catalog_service.start()

        self.redpanda.add_extra_rp_conf({
            "iceberg_catalog_type":
            "rest",
            "iceberg_rest_catalog_endpoint":
            self.catalog_service.catalog_url,
            "iceberg_rest_catalog_client_id":
            "panda-user",
            "iceberg_rest_catalog_client_secret":
            "panda-secret",
        })
        self.redpanda.start()

    @property
    def msg_size(self):
        return 128

    @property
    def msg_count(self):
        return int(100 if self.debug_mode else 5 * self.producer_throughput /
                   self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 20 * 1024 * 1024

    def start_producer(self, topic_name: str):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            self.msg_size,
            self.msg_count,
            rate_limit_bps=self.producer_throughput,
            debug_logs=True,
        )

        producer.start(clean=False)

        wait_until(lambda: producer.produce_status.acked > 10,
                   timeout_sec=120,
                   backoff_sec=1)

        return producer

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_redpanda_connection_to_rest_catalog(self, cloud_storage_type):

        catalog = self.catalog_service.client()
        namespace = "redpanda"
        topic = TopicSpec(name='datalake-test-topic', partition_count=3)

        self.client().create_topic(topic)
        self.client().alter_topic_config(topic.name, "redpanda.iceberg.mode",
                                         "key_value")

        producer = self.start_producer(topic_name=topic.name)
        # wait for the producer to finish
        producer.wait()

        def table_created_in_iceberg():
            namespaces = catalog.list_namespaces()
            return (namespace, ) in namespaces and (
                namespace, topic.name) in catalog.list_tables(namespace)

        def data_available_in_iceberg_table():
            if not table_created_in_iceberg():
                # Wait for the table to be created
                return False
            table = catalog.load_table(f"{namespace}.{topic.name}")
            return len(table.snapshots()) > 0

        wait_until(data_available_in_iceberg_table,
                   timeout_sec=90,
                   backoff_sec=5,
                   err_msg="Error waiting for Iceberg table to have data",
                   retry_on_exc=True)

        def all_data_translated():
            table = catalog.load_table(f"{namespace}.{topic.name}")
            df = table.scan().to_pandas()
            offsets_df = df['redpanda'].str.get('offset').head(5).to_list()
            self.logger.info(f"offsets: {offsets_df}")
            return all([isinstance(o, int) for o in offsets_df])

        wait_until(
            all_data_translated,
            timeout_sec=90,
            backoff_sec=5,
            err_msg="Error waiting for data to be fully available in icberg",
            retry_on_exc=True)
