# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from ducktape.mark import parametrize

from rptest.clients.default import DefaultClient
from rptest.services.redpanda import SISettings
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.rpk_producer import RpkProducer

from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.services.verifiable_consumer import VerifiableConsumer


class TestReadReplicaService(EndToEndTest):
    log_segment_size = 5048576  # 5MB
    log_compaction_interval_ms = 25000
    topic_name = "panda-topic"
    si_settings = SISettings(
        cloud_storage_reconciliation_interval_ms=500,
        cloud_storage_max_connections=5,
        log_segment_size=log_segment_size,
        cloud_storage_readreplica_manifest_sync_timeout_ms=500)

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=self.log_compaction_interval_ms)

        super(TestReadReplicaService,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)
        self.second_cluster = None

    def create_read_replica_topic(self):
        self.second_cluster = RedpandaService(self.test_context,
                                              num_brokers=3,
                                              si_settings=self.si_settings)
        self.second_cluster.start()

        rpk_second_cluster = RpkTool(self.second_cluster)
        conf = {
            'redpanda.remote.readreplica': 'true',
            'redpanda.remote.readreplica.bucket': self.s3_bucket_name,
        }
        rpk_second_cluster.create_topic(self.topic_name, config=conf)

    def start_workload(self):
        self.consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.second_cluster,
            topic=self.topic_name,
            group_id='consumer_test_group',
            on_record_consumed=self.on_record_consumed)
        self.consumer.start()

        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_name,
            throughput=1000,
            message_validator=is_int_with_prefix)
        self.producer.start()

    @cluster(num_nodes=8)
    def test_simple_end_to_end(self):
        partition_count = 10

        self.s3_bucket_name = self.si_settings.cloud_storage_bucket

        self.start_redpanda(3,
                            extra_rp_conf=self._extra_rp_conf,
                            si_settings=self.si_settings)
        spec = TopicSpec(name=self.topic_name,
                         partition_count=partition_count,
                         replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(spec.name, 'redpanda.remote.write', 'true')

        self.create_read_replica_topic()

        # start source producer & target consumer
        self.start_workload()

        self.run_validation(consumer_timeout_sec=120)
