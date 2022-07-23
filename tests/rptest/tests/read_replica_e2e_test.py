# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.services.cluster import cluster

from rptest.clients.default import DefaultClient
from rptest.services.redpanda import SISettings
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.util import expect_exception
from ducktape.mark import matrix

import json

from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.util import (
    wait_until, )


class TestReadReplicaService(EndToEndTest):
    log_segment_size = 1048576  # 5MB
    topic_name = "panda-topic"
    s3_bucket_name = "panda-bucket"
    si_settings = SISettings(
        cloud_storage_bucket=s3_bucket_name,
        cloud_storage_reconciliation_interval_ms=500,
        cloud_storage_max_connections=5,
        log_segment_size=log_segment_size,
        cloud_storage_readreplica_manifest_sync_timeout_ms=500,
        cloud_storage_segment_max_upload_interval_sec=5)

    def __init__(self, test_context):
        super(TestReadReplicaService, self).__init__(test_context=test_context)
        self.second_cluster = None

    def start_second_cluster(self):
        self.second_cluster = RedpandaService(self.test_context,
                                              num_brokers=3,
                                              si_settings=self.si_settings)
        self.second_cluster.start(start_si=False)

    def create_read_replica_topic(self):
        rpk_second_cluster = RpkTool(self.second_cluster)
        conf = {
            'redpanda.remote.readreplica': self.s3_bucket_name,
        }
        rpk_second_cluster.create_topic(self.topic_name, config=conf)

    def start_consumer(self):
        self.consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.second_cluster,
            topic=self.topic_name,
            group_id='consumer_test_group',
            on_record_consumed=self.on_record_consumed)
        self.consumer.start()

    def start_producer(self):
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_name,
            throughput=1000,
            message_validator=is_int_with_prefix)
        self.producer.start()

    @cluster(num_nodes=6)
    @matrix(partition_count=[10])
    def test_produce_is_forbidden(self, partition_count):
        # Create original topic
        self.start_redpanda(3, si_settings=self.si_settings)
        spec = TopicSpec(name=self.topic_name,
                         partition_count=partition_count,
                         replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)

        # Make original topic upload data to S3
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(spec.name, 'redpanda.remote.write', 'true')

        self.start_second_cluster()

        def create_read_replica_topic_success():
            try:
                self.create_read_replica_topic()
            except RpkException as e:
                if "The server experienced an unexpected error when processing the request" in str(
                        e):
                    return False
                else:
                    raise
            else:
                return True

        assert self.redpanda and self.redpanda.s3_client
        # wait until the read replica topic creation succeeds
        wait_until(
            create_read_replica_topic_success,
            timeout_sec=
            60,  #should be uploaded since cloud_storage_segment_max_upload_interval_sec=5
            backoff_sec=5,
            err_msg="Could not create read replica topic. Most likely " +
            "because topic manifest is not in S3, in S3 bucket: " +
            f"{list(self.redpanda.s3_client.list_objects(self.s3_bucket_name))}"
        )

        second_rpk = RpkTool(self.second_cluster)
        with expect_exception(
                RpkException, lambda e:
                "unable to produce record: INVALID_TOPIC_EXCEPTION: The request attempted to perform an operation on an invalid topic."
                in str(e)):
            second_rpk.produce(self.topic_name, "", "test payload")

    @cluster(num_nodes=8)
    @matrix(partition_count=[10], min_records=[10000])
    def test_simple_end_to_end(self, partition_count, min_records):
        # Create original topic, produce data to it
        self.start_redpanda(3, si_settings=self.si_settings)
        spec = TopicSpec(name=self.topic_name,
                         partition_count=partition_count,
                         replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)

        self.start_producer()
        wait_until(lambda: self.producer.num_acked > min_records,
                       timeout_sec=30,
                       err_msg="Producer failed to produce messages for %ds." %\
                       30)
        self.logger.info("Stopping producer after writing up to offsets %s" %\
                        str(self.producer.last_acked_offsets))
        self.producer.stop()

        # Make original topic upload data to S3
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(spec.name, 'redpanda.remote.write', 'true')

        assert self.redpanda and self.redpanda.s3_client  # drops optional types

        # Make sure all produced data is uploaded to S3
        def s3_has_all_data() -> bool:
            # pyright doesn't consider the assert in outer scope
            assert self.redpanda and self.redpanda.s3_client
            objects = list(
                self.redpanda.s3_client.list_objects(self.s3_bucket_name))
            total_uploaded = 0
            for o in objects:
                if o.Key.endswith(
                        "/manifest.json") and self.topic_name in o.Key:
                    data = self.redpanda.s3_client.get_object_data(
                        self.s3_bucket_name, o.Key)
                    manifest = json.loads(data)
                    last_upl_offset = manifest['last_offset']
                    total_uploaded += last_upl_offset
                    self.logger.info(
                        f"Found manifest at {o.Key}, last_offset is {last_upl_offset}"
                    )
            self.logger.info(
                f"Total uploaded: {total_uploaded}, num_acked: {self.producer.num_acked}"
            )
            return total_uploaded >= self.producer.num_acked

        wait_until(
            s3_has_all_data,
            timeout_sec=
            30,  #should be uploaded since cloud_storage_segment_max_upload_interval_sec=5
            backoff_sec=5,
            err_msg="Not all data is uploaded to S3 bucket: " +
            f"{list(self.redpanda.s3_client.list_objects(self.s3_bucket_name))}"
        )

        # Create read replica topic, consume from it and validate
        self.start_second_cluster()
        self.create_read_replica_topic()
        self.start_consumer()
        self.run_validation()
