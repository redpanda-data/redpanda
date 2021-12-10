# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.archival.s3_client import S3Client
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import (
    segments_count,
    wait_for_segments_removal,
)

import confluent_kafka as ck

import uuid
import random
from itertools import zip_longest


class ShadowIndexingTxTest(RedpandaTest):
    segment_size = 1048576  # 1 Mb
    s3_host_name = "minio-s3"
    s3_access_key = "panda-user"
    s3_secret_key = "panda-secret"
    s3_region = "panda-region"
    s3_topic_name = "panda-topic"
    topics = (TopicSpec(name='panda-topic',
                        partition_count=1,
                        replication_factor=3), )

    def __init__(self, test_context):
        self.s3_bucket_name = f"panda-bucket-{uuid.uuid1()}"
        extra_rp_conf = dict(
            log_segment_size=self.segment_size,
            cloud_storage_enabled=True,
            cloud_storage_access_key=self.s3_access_key,
            cloud_storage_secret_key=self.s3_secret_key,
            cloud_storage_region=self.s3_region,
            cloud_storage_bucket=self.s3_bucket_name,
            cloud_storage_disable_tls=True,
            cloud_storage_api_endpoint=self.s3_host_name,
            cloud_storage_api_endpoint_port=9000,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            enable_idempotence=True,
            enable_transactions=True,
            enable_leader_balancer=False,
            enable_auto_rebalance_on_node_add=False,
        )

        super(ShadowIndexingTxTest, self).__init__(test_context=test_context,
                                                   extra_rp_conf=extra_rp_conf)

        s3client = S3Client(
            region=self.s3_region,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            endpoint=f"http://{self.s3_host_name}:9000",
            logger=self.logger,
        )
        s3client.create_bucket(self.s3_bucket_name)

    def setUp(self):
        rpk = RpkTool(self.redpanda)
        super(ShadowIndexingTxTest, self).setUp()
        for topic in self.topics:
            rpk.alter_topic_config(topic.name, 'redpanda.remote.write', 'true')
            rpk.alter_topic_config(topic.name, 'redpanda.remote.read', 'true')

    @cluster(num_nodes=3)
    def test_shadow_indexing_aborted_txs(self):
        """Check that messages belonging to aborted transaction are not seen by clients
        when fetching from remote segments."""
        topic = self.topics[0]

        class Producer:
            def __init__(self, brokers, logger):
                self.keys = []
                self.cur_offset = 0
                self.brokers = brokers
                self.logger = logger
                self.num_aborted = 0
                self.reconnect()

            def reconnect(self):
                self.producer = ck.Producer({
                    'bootstrap.servers':
                    self.brokers,
                    'transactional.id':
                    'shadow-indexing-tx-test',
                })
                self.producer.init_transactions()

            def produce(self, topic):
                """produce some messages inside a transaction with increasing keys
                and random values. Then randomly commit/abort the transaction."""

                n_msgs = random.randint(50, 100)
                keys = []

                self.producer.begin_transaction()
                for _ in range(n_msgs):
                    val = ''.join(
                        map(chr, (random.randint(0, 256)
                                  for _ in range(random.randint(100, 1000)))))
                    self.producer.produce(topic.name, val,
                                          str(self.cur_offset))
                    keys.append(str(self.cur_offset).encode('utf8'))
                    self.cur_offset += 1

                self.logger.info(
                    f"writing {len(keys)} msgs: {keys[0]}-{keys[-1]}...")
                self.producer.flush()
                if random.random() < 0.1:
                    self.producer.abort_transaction()
                    self.num_aborted += 1
                    self.logger.info("aborted txn")
                else:
                    self.producer.commit_transaction()
                    self.keys.extend(keys)

        producer = Producer(self.redpanda.brokers(), self.logger)

        def done():
            for _ in range(100):
                try:
                    producer.produce(topic)
                except ck.KafkaException as err:
                    self.logger.warn(f"producer error: {err}")
                    producer.reconnect()
            self.logger.info("producer iteration complete")
            topic_partitions = segments_count(self.redpanda,
                                              topic.name,
                                              partition_idx=0)
            partitions = []
            for p in topic_partitions:
                partitions.append(p >= 10)
            return all(partitions)

        wait_until(done,
                   timeout_sec=120,
                   backoff_sec=1,
                   err_msg="producing failed")

        assert producer.num_aborted > 0

        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_BYTES: 3 * self.segment_size,
            },
        )
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=6)

        consumer = ck.Consumer(
            {
                'bootstrap.servers': self.redpanda.brokers(),
                'group.id': 'shadow-indexing-tx-test',
                'auto.offset.reset': 'earliest',
            },
            logger=self.logger)
        consumer.subscribe([topic.name])

        consumed = []
        while True:
            msgs = consumer.consume(timeout=5.0)
            if len(msgs) == 0:
                break
            consumed.extend([(m.key(), m.offset()) for m in msgs])

        first_mismatch = ''
        for p_key, (c_key, c_offset) in zip_longest(producer.keys, consumed):
            if p_key != c_key:
                first_mismatch = f"produced: {p_key}, consumed: {c_key} (offset: {c_offset})"
                break

        assert (not first_mismatch), (
            f"produced and consumed messages differ, "
            f"produced length: {len(producer.keys)}, consumed length: {len(consumed)}, "
            f"first mismatch: {first_mismatch}")
