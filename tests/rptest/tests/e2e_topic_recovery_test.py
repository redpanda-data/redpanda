# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.mark import matrix
from ducktape.cluster.cluster_spec import ClusterSpec
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.util import Scale, segments_count
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from ducktape.utils.util import wait_until
import time
import json
from itertools import zip_longest

from rptest.util import segments_count
from rptest.utils.si_utils import Producer

import confluent_kafka as ck


class EndToEndTopicRecovery(RedpandaTest):
    """
    The test produces data and makes sure that all data
    is uploaded to S3. After that the test recreates the
    cluster (or topic) and runs the verifier.
    """

    topics = (TopicSpec(), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            enable_idempotence=True,
            enable_transactions=True,
            enable_leader_balancer=False,
            partition_autobalancing_mode="off",
            group_initial_rebalance_delay=300,
        )
        si_settings = SISettings(
            log_segment_size=1024 * 1024,
            cloud_storage_segment_max_upload_interval_sec=5,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True)
        self.scale = Scale(test_context)
        self._bucket = si_settings.cloud_storage_bucket
        super().__init__(test_context=test_context,
                         si_settings=si_settings,
                         extra_rp_conf=extra_rp_conf)
        self._ctx = test_context
        self._producer = None
        self._consumer = None
        self._verifier_node = test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))[0]
        self.logger.info(f"Verifier node name: {self._verifier_node.name}")

    def init_producer(self, msg_size, num_messages):
        self._producer = KgoVerifierProducer(self._ctx, self.redpanda,
                                             self.topic, msg_size,
                                             num_messages,
                                             [self._verifier_node])

    def init_consumer(self, msg_size):
        self._consumer = KgoVerifierSeqConsumer(self._ctx, self.redpanda,
                                                self.topic, msg_size,
                                                [self._verifier_node])

    def free_nodes(self):
        super().free_nodes()
        wait_until(lambda: self.redpanda.sockets_clear(self._verifier_node),
                   timeout_sec=120,
                   backoff_sec=10)
        self.test_context.cluster.free_single(self._verifier_node)

    def _stop_redpanda_nodes(self):
        """Stop all redpanda nodes"""
        for node in self.redpanda.nodes:
            self.logger.info(f"Node {node.account.hostname} will be stopped")
            if not node is self._verifier_node:
                self.redpanda.stop_node(node)
        time.sleep(10)

    def _start_redpanda_nodes(self):
        """Start all redpanda nodes"""
        for node in self.redpanda.nodes:
            self.logger.info(f"Starting node {node.account.hostname}")
            if not node is self._verifier_node:
                self.redpanda.start_node(node)
        time.sleep(10)

    def _wipe_data(self):
        """Remove all data from redpanda cluster"""
        for node in self.redpanda.nodes:
            self.logger.info(
                f"All data will be removed from node {node.account.hostname}")
            self.redpanda.remove_local_data(node)

    def _restore_topic(self, topic_spec, overrides={}):
        """Restore individual topic"""
        self.logger.info(f"Restore topic called. Topic-manifest: {topic_spec}")
        conf = {
            'redpanda.remote.recovery': 'true',
            #'redpanda.remote.write': 'true',
        }
        conf.update(overrides)
        self.logger.info(f"Confg: {conf}")
        topic = topic_spec.name
        npart = topic_spec.partition_count
        nrepl = topic_spec.replication_factor
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic, npart, nrepl, conf)
        time.sleep(10)
        rpk.describe_topic(topic)
        rpk.describe_topic_configs(topic)

    def _s3_has_all_data(self, num_messages):
        objects = list(self.redpanda.get_objects_from_si())
        for o in objects:
            if o.Key.endswith("/manifest.json") and self.topic in o.Key:
                data = self.redpanda.s3_client.get_object_data(
                    self._bucket, o.Key)
                manifest = json.loads(data)
                last_upl_offset = manifest['last_offset']
                self.logger.info(
                    f"Found manifest at {o.Key}, last_offset is {last_upl_offset}"
                )
                # We have one partition so this invariant holds
                # it has to be changed when the number of partitions
                # will get larger. This will also require different
                # S3 check.
                return last_upl_offset >= num_messages
        return False

    @cluster(num_nodes=4)
    @matrix(message_size=[5000],
            num_messages=[100000],
            recovery_overrides=[{}, {
                'retention.bytes': 1024
            }])
    def test_restore(self, message_size, num_messages, recovery_overrides):
        """Write some data. Remove local data then restore
        the cluster."""

        if self.debug_mode:
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        self.init_producer(message_size, num_messages)
        self._producer.start(clean=False)

        self._producer.wait()
        assert self._producer.produce_status.acked >= num_messages

        time.sleep(10)
        wait_until(lambda: self._s3_has_all_data(num_messages),
                   timeout_sec=600,
                   backoff_sec=5,
                   err_msg=f"Not all data is uploaded to S3 bucket")

        # Whipe out the state on the nodes
        self._stop_redpanda_nodes()
        self._wipe_data()
        # Run recovery
        self._start_redpanda_nodes()
        for topic_spec in self.topics:
            self._restore_topic(topic_spec, recovery_overrides)

        # Start verifiable consumer
        self.init_consumer(message_size)
        self._consumer.start(clean=False)

        self._consumer.wait()

        produce_acked = self._producer.produce_status.acked
        consume_total = self._consumer.consumer_status.validator.total_reads
        consume_valid = self._consumer.consumer_status.validator.valid_reads
        self.logger.info(f"Producer acked {produce_acked} messages")
        self.logger.info(f"Consumer read {consume_total} messages")
        assert produce_acked >= num_messages
        assert consume_valid >= produce_acked, f"produced {produce_acked}, consumed {consume_valid}"

    @cluster(num_nodes=4)
    @matrix(recovery_overrides=[{}, {
        'retention.bytes': 1024,
        'redpanda.remote.write': True,
        'redpanda.remote.read': True,
    }])
    def test_restore_with_aborted_tx(self, recovery_overrides):
        """Produce data using transactions includin some percentage of aborted
        transactions. Run recovery and make sure that record batches generated
        by aborted transactions are not visible.
        This should work because rm_stm will rebuild its snapshot right after
        recovery. The SI will also supports aborted transctions via tx manifests.

        The test has two variants:
        - the partition is downloaded fully;
        - the partition is downloaded partially and SI is used to read from the start;
        """
        if self.debug_mode:
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        producer = Producer(self.redpanda.brokers(), "topic-recovery-tx-test",
                            self.logger)

        def done():
            for _ in range(100):
                try:
                    producer.produce(self.topic)
                except ck.KafkaException as err:
                    self.logger.warn(f"producer error: {err}")
                    producer.reconnect()
            self.logger.info("producer iteration complete")
            topic_partitions = segments_count(self.redpanda,
                                              self.topic,
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

        time.sleep(10)
        wait_until(lambda: self._s3_has_all_data(producer.cur_offset),
                   timeout_sec=300,
                   backoff_sec=5,
                   err_msg=f"Not all data is uploaded to S3 bucket")

        # Whipe out the state on the nodes
        self._stop_redpanda_nodes()
        self._wipe_data()
        # Run recovery
        self._start_redpanda_nodes()
        for topic_spec in self.topics:
            self._restore_topic(topic_spec, recovery_overrides)

        # Consume and validate
        consumer = ck.Consumer(
            {
                'bootstrap.servers': self.redpanda.brokers(),
                'group.id': 'topic-recovery-tx-test',
                'auto.offset.reset': 'earliest',
            },
            logger=self.logger)
        consumer.subscribe([self.topic])

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
