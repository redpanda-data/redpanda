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
from rptest.services.redpanda import CloudStorageType, RedpandaService, SISettings
from rptest.util import Scale, segments_count, wait_for_local_storage_truncate
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.utils.mode_checks import skip_debug_mode
from ducktape.mark import ok_to_fail
from ducktape.utils.util import wait_until
import time
import json
import re

ALLOWED_ERROR_LOG_LINES = [re.compile("Can't prepare pid.* - unknown session")]


class EndToEndTopicRecovery(RedpandaTest):
    """
    The test produces data and makes sure that all data
    is uploaded to S3. After that the test recreates the
    cluster (or topic) and runs the verifier.
    """

    topics = (TopicSpec(), )

    def __init__(self, test_context, enable_kerberos_listener=False):
        extra_rp_conf = dict(
            enable_leader_balancer=False,
            partition_autobalancing_mode="off",
            group_initial_rebalance_delay=300,
        )
        si_settings = SISettings(
            test_context,
            log_segment_size=1024 * 1024,
            cloud_storage_segment_max_upload_interval_sec=5,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True)
        self.scale = Scale(test_context)
        self._bucket = si_settings.cloud_storage_bucket
        super().__init__(test_context=test_context,
                         si_settings=si_settings,
                         enable_kerberos_listener=enable_kerberos_listener,
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
        self._consumer = KgoVerifierSeqConsumer(self._ctx,
                                                self.redpanda,
                                                self.topic,
                                                msg_size,
                                                nodes=[self._verifier_node])

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
            if o.key.endswith("/manifest.json") and self.topic in o.key:
                data = self.redpanda.cloud_storage_client.get_object_data(
                    self._bucket, o.key)
                manifest = json.loads(data)
                last_upl_offset = manifest['last_offset']
                self.logger.info(
                    f"Found manifest at {o.key}, last_offset is {last_upl_offset}"
                )
                # We have one partition so this invariant holds
                # it has to be changed when the number of partitions
                # will get larger. This will also require different
                # S3 check.
                return last_upl_offset >= num_messages
        return False

    @cluster(num_nodes=4)
    @matrix(num_messages=[2],
            cloud_storage_type=[CloudStorageType.ABS, CloudStorageType.S3])
    def test_restore_with_config_batches(self, num_messages,
                                         cloud_storage_type):
        """related to issue 6413: force the creation of remote segments containing only configuration batches,
        check that older data can be nonetheless recovered even if the total download size
         would exceed the property retention.local.target.bytes.
         in other words, only segments containing at least some data counts towards retention.local.target.bytes limit"""
        """1. generate some messages
        2. restart the cluster to generate some config-only remote segments
        3. restore topic with a small retention.local.target.bytes to force redpanda 
           to get over the limit and skip over configuration batches"""
        self.logger.info("start")
        self.init_producer(5000, num_messages)
        self._producer.start(clean=False)
        self._producer.wait()
        assert self._producer.produce_status.acked >= num_messages
        self.logger.info("waiting S3")
        wait_until(lambda: self._s3_has_all_data(num_messages),
                   timeout_sec=100,
                   backoff_sec=5,
                   err_msg="Not all data is uploaded to S3 bucket")

        for i in range(3):
            self.logger.info(f"iteration {i}")
            self._stop_redpanda_nodes()
            self._start_redpanda_nodes()

        self.logger.info("final iteration")
        self._stop_redpanda_nodes()
        self._wipe_data()

        # Run recovery
        self._start_redpanda_nodes()
        for topic_spec in self.topics:
            self._restore_topic(topic_spec,
                                {'retention.local.target.bytes': 512})

        self.init_consumer(5000)
        self._consumer.start(clean=False)
        # we just care for the consumer to receive some data
        self._consumer.wait(timeout_sec=100)

    @cluster(num_nodes=4)
    @matrix(message_size=[5000],
            num_messages=[100000],
            recovery_overrides=[{}, {
                'retention.local.target.bytes': 1024
            }],
            cloud_storage_type=[CloudStorageType.ABS, CloudStorageType.S3])
    def test_restore(self, message_size, num_messages, recovery_overrides,
                     cloud_storage_type):
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

        # Wipe out the state on the nodes
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

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/7955
    @cluster(num_nodes=5, log_allow_list=ALLOWED_ERROR_LOG_LINES)
    @matrix(recovery_overrides=[{}, {
        'retention.local.target.bytes': 1024,
        'redpanda.remote.write': True,
        'redpanda.remote.read': True,
    }],
            cloud_storage_type=[CloudStorageType.ABS, CloudStorageType.S3])
    @skip_debug_mode
    def test_restore_with_aborted_tx(self, recovery_overrides,
                                     cloud_storage_type):
        """Produce data using transactions including some percentage of aborted
        transactions. Run recovery and make sure that record batches generated
        by aborted transactions are not visible.
        This should work because rm_stm will rebuild its snapshot right after
        recovery. The SI will also supports aborted transctions via tx manifests.

        The test has two variants:
        - the partition is downloaded fully;
        - the partition is downloaded partially and SI is used to read from the start;
        """

        # Abort an unrealistically high fraction of transactions, to give a good probability
        # of exercising code paths that might e.g. seek to an abort marker
        # (reproduce https://github.com/redpanda-data/redpanda/issues/7043)
        msg_size = 16384
        msg_count = 10000
        per_transaction = 10
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       use_transactions=True,
                                       transaction_abort_rate=0.5,
                                       msgs_per_transaction=per_transaction,
                                       debug_logs=True)
        producer.start()
        producer.wait()
        pstatus = producer.produce_status
        self.logger.info(f"Producer status: {pstatus}")
        committed_messages = pstatus.acked - pstatus.aborted_transaction_messages
        assert pstatus.acked == msg_count
        # At least some were aborted, at least some were committed
        assert 0 < committed_messages < msg_count

        # Re-use the same node as the producer for consumers
        traffic_node = producer.nodes[0]

        rpk = RpkTool(self.redpanda)
        hwm = next(rpk.describe_topic(self.topic)).high_watermark
        self.logger.info(f"Produced to HWM {hwm}")

        # Each transaction is amplified with one tx start and one tx end marker
        assert hwm == msg_count + (msg_count / per_transaction) * 2

        wait_until(lambda: self._s3_has_all_data(hwm),
                   timeout_sec=600,
                   backoff_sec=5,
                   err_msg=f"Not all data is uploaded to S3 bucket")

        # Keep services alive, so that log-capturing gets their logs at the end
        consumers = []

        def validate():
            consumer = KgoVerifierSeqConsumer(self.test_context,
                                              self.redpanda,
                                              self.topic,
                                              msg_size,
                                              loop=False,
                                              nodes=[traffic_node],
                                              debug_logs=True,
                                              trace_logs=True)
            consumer.start(clean=False)
            consumers.append(consumer)
            consumer.wait()
            status = consumer.consumer_status
            self.logger.info(f"Consumer status: {status}")
            assert status.errors == 0
            assert status.validator.valid_reads == committed_messages

            # No aborted messages should be visible
            assert status.validator.invalid_reads == 0

            # No junk on the topic outside of the ranges our producer produced
            assert status.validator.out_of_scope_invalid_reads == 0

        # Validate that transactional reads of the produced data are correct
        # (we will validate several times before recovery, in order to ensure that if anything
        #  fails after recovery, it is recovery specific)
        validate()

        # Let local data age out validate that transaction reads that hit S3 are correct
        rpk.alter_topic_config(self.topic, "retention.local.target.bytes",
                               "1024")
        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        0,
                                        int(msg_size * msg_count * 0.5),
                                        timeout_sec=30)
        validate()

        # Restart, and check that transactional reads to S3 remain correct
        self.redpanda.restart_nodes(self.redpanda.nodes)
        validate()

        # Wipe out the state on the nodes
        self._stop_redpanda_nodes()
        self._wipe_data()
        # Run recovery
        self._start_redpanda_nodes()
        for topic_spec in self.topics:
            self._restore_topic(topic_spec, recovery_overrides)

        restore_hwm = next(rpk.describe_topic(self.topic)).high_watermark
        assert restore_hwm == hwm

        validate()


class Issue7758non_repro(EndToEndTopicRecovery):
    def __init__(self, test_context):
        super().__init__(test_context=test_context,
                         enable_kerberos_listener=True)

    @cluster(num_nodes=4)
    def test_case(self):
        num_messages = 2
        msg_size = 5000
        topic = TopicSpec()
        self.logger.info("start")

        self.init_producer(msg_size, num_messages)
        producer = self._producer
        producer.start(clean=False)
        producer.wait()
        assert producer.produce_status.acked >= num_messages
        self.logger.info("waiting S3")
        wait_until(lambda: self._s3_has_all_data(num_messages),
                   timeout_sec=100,
                   backoff_sec=5,
                   err_msg="Not all data is uploaded to S3 bucket")

        self.logger.info("final iteration")
        self._stop_redpanda_nodes()
        # wipe and recovery seems important
        #self._wipe_data()

        # Run recovery
        self._start_redpanda_nodes()
        #self._restore_topic(topic, {'retention.local.target.bytes': 512})

        self.init_consumer(msg_size)
        consumer = self._consumer
        consumer.start(clean=False)
        # we just care for the consumer to receive some data
        consumer.wait(timeout_sec=100)


class Issue7758repro(EndToEndTopicRecovery):
    def __init__(self, test_context):
        super().__init__(test_context=test_context,
                         enable_kerberos_listener=True)

    @cluster(num_nodes=4)
    def test_case(self):
        num_messages = 2
        msg_size = 5000
        topic = TopicSpec()
        self.logger.info("start")

        self.init_producer(msg_size, num_messages)
        producer = self._producer
        producer.start(clean=False)
        producer.wait()
        assert producer.produce_status.acked >= num_messages
        self.logger.info("waiting S3")
        wait_until(lambda: self._s3_has_all_data(num_messages),
                   timeout_sec=100,
                   backoff_sec=5,
                   err_msg="Not all data is uploaded to S3 bucket")

        self.logger.info("final iteration")
        self._stop_redpanda_nodes()
        # wipe and recovery seems important
        self._wipe_data()

        # Run recovery
        self._start_redpanda_nodes()
        self._restore_topic(topic, {'retention.local.target.bytes': 512})

        self.init_consumer(msg_size)
        consumer = self._consumer
        consumer.start(clean=False)
        # we just care for the consumer to receive some data
        consumer.wait(timeout_sec=100)
