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
from rptest.util import Scale
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer
from ducktape.utils.util import wait_until
import time
import json


class EndToEndTopicRecovery(RedpandaTest):
    """
    The test produces data and makes sure that all data
    is uploaded to S3. After that the test recreates the
    cluster (or topic) and runs the verifier.
    """

    topics = (TopicSpec(), )

    def __init__(self, test_context):
        si_settings = SISettings(
            log_segment_size=1024 * 1024,
            cloud_storage_segment_max_upload_interval_sec=5,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True)
        self.scale = Scale(test_context)
        self._bucket = si_settings.cloud_storage_bucket
        super().__init__(test_context=test_context, si_settings=si_settings)
        self._ctx = test_context
        self._producer = None
        self._consumer = None
        self._verifier_node = test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))[0]
        self.logger.info(f"Verifier node name: {self._verifier_node.name}")

    def init_producer(self, msg_size, num_messages):
        self._producer = FranzGoVerifiableProducer(self._ctx, self.redpanda,
                                                   self.topic, msg_size,
                                                   num_messages,
                                                   [self._verifier_node])

    def init_consumer(self, msg_size):
        self._consumer = FranzGoVerifiableSeqConsumer(self._ctx, self.redpanda,
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

    @cluster(num_nodes=4)
    @matrix(message_size=[10000],
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

        def s3_has_all_data():
            objects = list(self.redpanda.get_objects_from_si())
            total_size = 0
            for o in objects:
                if o.Key.endswith("/manifest.json") and self.topic in o.Key:
                    data = self.redpanda.s3_client.get_object_data(
                        self._bucket, o.Key)
                    manifest = json.loads(data)
                    last_upl_offset = manifest['last_offset']
                    self.logger.info(
                        f"Found manifest at {o.Key}, last_offset is {last_upl_offset}"
                    )
                    return last_upl_offset >= num_messages
            return False

        time.sleep(10)
        wait_until(s3_has_all_data,
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

        # Start verifiable consumer
        self.init_consumer(message_size)
        self._consumer.start(clean=False)

        self._consumer.shutdown()
        self._consumer.wait()

        produce_acked = self._producer.produce_status.acked
        consume_total = self._consumer.consumer_status.total_reads
        consume_valid = self._consumer.consumer_status.valid_reads
        self.logger.info(f"Producer acked {produce_acked} messages")
        self.logger.info(f"Consumer read {consume_total} messages")
        assert produce_acked >= num_messages
        assert consume_valid >= produce_acked, f"produced {produce_acked}, consumed {consume_valid}"
