# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService
from rptest.services.redpanda import SISettings
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierRandomConsumer

LOCAL_CONFIGURATION = {
    "partition_amount": 3,
    "segment_size": 1024 * 100,
    "throughput": 10000,
    "key_set_cardinality": 100,
    "retention.local.target.bytes": 5 * 1024 * 100,
    "produce_offsets": 10000000,
    "random_message_validate": 10000
}


class CloudStorageCompactionTest(EndToEndTest):
    topic = "panda-topic"
    num_brokers = 3

    def __init__(self, test_context, extra_rp_conf=None, environment=None):
        super(CloudStorageCompactionTest,
              self).__init__(test_context=test_context)

        if self.test_context.globals.get("dedicated_nodes", False):
            self.configuration = LOCAL_CONFIGURATION
        else:
            self.configuration = LOCAL_CONFIGURATION

        self.topic = CloudStorageCompactionTest.topic
        self.test_context = test_context

        self._init_redpanda(test_context, extra_rp_conf, environment)
        self._init_redpanda_read_replica(test_context, extra_rp_conf,
                                         environment)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def _init_redpanda(self, test_context, extra_rp_conf, environment):
        self.si_settings = SISettings(
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=self.configuration["segment_size"],
            cloud_storage_readreplica_manifest_sync_timeout_ms=500,
            cloud_storage_segment_max_upload_interval_sec=5)

        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)

        self.redpanda = RedpandaService(context=self.test_context,
                                        num_brokers=3,
                                        si_settings=self.si_settings,
                                        extra_rp_conf=extra_rp_conf,
                                        environment=environment)

    def _init_redpanda_read_replica(self, test_context, extra_rp_conf,
                                    environment):
        self.rr_si_settings = SISettings(
            cloud_storage_bucket='none',
            bypass_bucket_creation=True,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=self.configuration["segment_size"],
            cloud_storage_readreplica_manifest_sync_timeout_ms=500,
            cloud_storage_segment_max_upload_interval_sec=5)
        self.rr_si_settings.load_context(self.logger, test_context)
        self.rr_cluster = RedpandaService(self.test_context,
                                          num_brokers=3,
                                          si_settings=self.rr_si_settings)

    def setUp(self):
        assert self.redpanda
        assert self.rr_cluster
        self.redpanda.start()
        self._create_topic()
        self.rr_cluster.start(start_si=False)
        wait_until(self._create_read_repica_topic_success,
                   timeout_sec=30,
                   backoff_sec=5)

    def _create_topic(self):
        self.topic_spec = TopicSpec(
            name=self.topic,
            partition_count=self.configuration["partition_amount"],
            replication_factor=3,
            cleanup_policy="compact,delete",
            segment_bytes=self.configuration["segment_size"])
        self.kafka_tools.create_topic(self.topic_spec)
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic_spec.name, 'redpanda.remote.write',
                               'true')

    def _create_read_repica_topic_success(self):
        try:
            rpk_rr_cluster = RpkTool(self.rr_cluster)
            conf = {
                'redpanda.remote.readreplica':
                self.si_settings.cloud_storage_bucket,
            }
            rpk_rr_cluster.create_topic(self.topic, config=conf)
            return True
        except RpkException as e:
            if "The server experienced an unexpected error when processing the request" in str(
                    e):
                return False
            else:
                raise

    def start_workload(self):
        self.start_producer(
            num_nodes=1,
            throughput=self.configuration["throughput"],
            repeating_keys=self.configuration["key_set_cardinality"])
        wait_until(lambda: len(self.producer.last_acked_offsets) != 0, 30)

    def tearDown(self):
        assert self.redpanda and self.redpanda.s3_client
        self.redpanda.s3_client.empty_bucket(self.s3_bucket_name)

    @cluster(num_nodes=9)
    def test_write(self):
        self.start_workload()
        self._rand_consumer = KgoVerifierRandomConsumer(
            self.test_context, self.redpanda, self.topic, None,
            self.configuration["random_message_validate"], 5)
        self._rand_consumer.start()
        self.start_consumer(num_nodes=1,
                            redpanda_cluster=self.rr_cluster,
                            verify_offsets=False)
        while sum(self.producer.last_acked_offsets.copy().values()
                  ) < self.configuration["produce_offsets"]:
            last_acked_offsets = self.producer.last_acked_offsets.copy()
            self.logger.error(f"Consuming until: {last_acked_offsets}")
            self.await_consumed_offsets(last_acked_offsets, 120)
            self.consumer.stop()
            time.sleep(1)
            self.consumer.start()
        self.producer.stop()
        self._rand_consumer.wait()
        self.run_consumer_validation(enable_compaction=True)
