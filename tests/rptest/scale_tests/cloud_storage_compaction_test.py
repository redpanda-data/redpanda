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
from rptest.services.redpanda import CloudStorageType, RedpandaService, MetricsEndpoint, SISettings, get_cloud_storage_type
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import wait_until
from ducktape.mark import matrix

LOCAL_CONFIGURATION = {
    "partition_amount": 3,
    "segment_size": 1024 * 500,
    "compacted_log_segment_size": 1024 * 100,
    "max_compacted_log_segment_size": 1024 * 500,
    "throughput": 10000,
    "key_set_cardinality": 10,
    "retention.local.target.bytes": 5 * 1024,
    "produce_offsets": 1000000,
    "log_compaction_interval_ms": 1000,
    "cloud_storage_segment_max_upload_interval_sec": 5
}

CDT_CONFIGURATION = {
    "partition_amount": 20,
    "segment_size": 25 * 1024 * 1024,
    "compacted_log_segment_size": 5 * 1024 * 1024,
    "max_compacted_log_segment_size": 25 * 1024 * 1024,
    "throughput": 100000,
    "key_set_cardinality": 1000,
    "retention.local.target.bytes": 5 * 1024 * 1024,
    "produce_offsets": 10000000,
    "log_compaction_interval_ms": 1000,
    "cloud_storage_segment_max_upload_interval_sec": 5
}


class CloudStorageCompactionTest(EndToEndTest):
    topic = "panda-topic"
    num_brokers = 3

    def __init__(self, test_context, extra_rp_conf=None, environment=None):
        super(CloudStorageCompactionTest,
              self).__init__(test_context=test_context)

        if self.test_context.globals.get("dedicated_nodes", False):
            self.configuration = CDT_CONFIGURATION
        else:
            self.configuration = LOCAL_CONFIGURATION

        self.topic = CloudStorageCompactionTest.topic
        self.test_context = test_context

        self._init_redpanda(test_context, extra_rp_conf, environment)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def _init_redpanda(self, test_context, extra_rp_conf, environment):
        self.si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=5,
            log_segment_size=self.configuration["segment_size"],
            cloud_storage_readreplica_manifest_sync_timeout_ms=500,
            cloud_storage_segment_max_upload_interval_sec=self.
            configuration["cloud_storage_segment_max_upload_interval_sec"])

        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)
        if not extra_rp_conf:
            extra_rp_conf = {}
        extra_rp_conf.update({
            "enable_leader_balancer":
            False,
            "partition_autobalancing_mode":
            "off",
            "group_initial_rebalance_delay":
            300,
            "compacted_log_segment_size":
            self.configuration["compacted_log_segment_size"],
            "max_compacted_log_segment_size":
            self.configuration["max_compacted_log_segment_size"],
        })

        self.redpanda = RedpandaService(context=self.test_context,
                                        num_brokers=3,
                                        si_settings=self.si_settings,
                                        extra_rp_conf=extra_rp_conf,
                                        environment=environment)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set(
            "log_compaction_interval_ms",
            str(self.configuration["log_compaction_interval_ms"]))
        self._create_topic()
        self._setup_read_replica()

    def _create_topic(self):
        self.topic_spec = TopicSpec(
            name=self.topic,
            partition_count=self.configuration["partition_amount"],
            replication_factor=3,
            cleanup_policy="compact,delete",
            segment_bytes=self.configuration["segment_size"],
            retention_bytes=self.configuration["retention.local.target.bytes"],
            redpanda_remote_write=True,
            redpanda_remote_read=True,
            redpanda_remote_delete=True)
        self.kafka_tools.create_topic(self.topic_spec)

    def start_workload(self):
        self.start_producer(
            num_nodes=1,
            throughput=self.configuration["throughput"],
            repeating_keys=self.configuration["key_set_cardinality"])
        wait_until(lambda: len(self.producer.last_acked_offsets) != 0, 30)

    def _init_redpanda_read_replica(self):
        self.rr_si_settings = SISettings(
            self.test_context,
            bypass_bucket_creation=True,
            cloud_storage_max_connections=5,
            log_segment_size=self.configuration["segment_size"],
            cloud_storage_readreplica_manifest_sync_timeout_ms=500,
            cloud_storage_segment_max_upload_interval_sec=self.
            configuration["cloud_storage_segment_max_upload_interval_sec"])
        self.rr_si_settings.load_context(self.logger, self.test_context)
        self.rr_cluster = RedpandaService(self.test_context,
                                          num_brokers=3,
                                          si_settings=self.rr_si_settings)

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

    def _setup_read_replica(self):
        self._init_redpanda_read_replica()
        self.rr_cluster.start(start_si=False)
        wait_until(self._create_read_repica_topic_success,
                   timeout_sec=30,
                   backoff_sec=5)

    # Permit transient CRC errors, to protect this test from incidences of
    # https://github.com/redpanda-data/redpanda/issues/6631
    # TODO: remove this low allow-list when that issue is resolved.
    @cluster(num_nodes=9,
             log_allow_list=[
                 "Cannot validate Kafka record batch. Missmatching CRC",
                 "batch has invalid CRC"
             ])
    @matrix(
        cloud_storage_type=get_cloud_storage_type(docker_use_arbitrary=True))
    def test_read_from_replica(self, cloud_storage_type):
        self.start_workload()
        self.start_consumer(num_nodes=2,
                            redpanda_cluster=self.rr_cluster,
                            verify_offsets=False)
        while sum(self.producer.last_acked_offsets.copy().values()
                  ) < self.configuration["produce_offsets"]:
            last_acked_offsets = self.producer.last_acked_offsets.copy()
            self.logger.debug(f"Consuming until: {last_acked_offsets}")
            self.await_consumed_offsets(last_acked_offsets, 600)
            self.consumer.stop()
            time.sleep(10)
            self.consumer.start()
        self.producer.stop()
        self.run_consumer_validation(enable_compaction=True,
                                     consumer_timeout_sec=600)

        upload_sucess = sum([
            sample.value for sample in self.redpanda.metrics_sample(
                "successful_uploads",
                metrics_endpoint=MetricsEndpoint.METRICS).samples
        ])
        upload_fails = sum([
            sample.value for sample in self.redpanda.metrics_sample(
                "failed_uploads",
                metrics_endpoint=MetricsEndpoint.METRICS).samples
        ])
        download_sucess = sum([
            sample.value for sample in self.rr_cluster.metrics_sample(
                "successful_downloads",
                metrics_endpoint=MetricsEndpoint.METRICS).samples
        ])
        download_fails = sum([
            sample.value for sample in self.rr_cluster.metrics_sample(
                "failed_downloads",
                metrics_endpoint=MetricsEndpoint.METRICS).samples
        ])

        assert upload_sucess > 0
        assert download_sucess > 0
        assert download_sucess <= upload_sucess, \
            f"Downloaded {download_sucess}, uploaded {upload_sucess}"
        assert upload_fails == 0
        assert download_fails == 0
