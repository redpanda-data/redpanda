# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time
import itertools
import math

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.utils.si_utils import nodes_report_cloud_segments
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.redpanda import (RESTART_LOG_ALLOW_LIST, MetricsEndpoint,
                                      SISettings)
from rptest.services.kgo_verifier_services import (KgoVerifierProducer,
                                                   KgoVerifierRandomConsumer)
from rptest.util import firewall_blocked
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.utils.si_utils import nodes_report_cloud_segments
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.metrics_check import MetricCheck
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations


class TieredStorageWithLoadTest(PreallocNodesTest):
    # Redpanda is responsible for bounding its own startup time via
    # STORAGE_TARGET_REPLAY_BYTES.  The resulting walltime for startup
    # depends on the speed of the disk.  60 seconds is long enough
    # for an i3en.xlarge (and more than enough for faster instance types)
    EXPECT_START_TIME = 60

    LEADER_BALANCER_PERIOD_MS = 30000
    topic_name = "tiered_storage_topic"
    small_segment_size = 4 * 1024
    regular_segment_size = 512 * 1024 * 1024
    unscaled_data_bps = int(1.7 * 1024 * 1024 * 1024)
    unscaled_num_partitions = 1024
    num_brokers = 4
    scaling_factor = num_brokers / 13
    scaled_data_bps = int(unscaled_data_bps * scaling_factor)  # ~0.53 GiB/s
    scaled_num_partitions = int(unscaled_num_partitions *
                                scaling_factor)  # 315
    scaled_segment_size = int(regular_segment_size * scaling_factor)
    num_segments_per_partition = 1000
    unavailable_timeout = 60
    memory_per_broker_bytes = 96 * 1024 * 1024 * 1024  # 96 GiB
    msg_size = 128 * 1024

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(TieredStorageWithLoadTest, self).__init__(
            test_ctx,
            *args,
            num_brokers=self.num_brokers,
            node_prealloc_count=1,
            extra_rp_conf={
                # In testing tiered storage, we care about creating as many
                # cloud segments as possible. To that end, bounding the segment
                # size isn't productive.
                'cloud_storage_segment_size_min': 1,
                'log_segment_size_min': 1024,

                # Disable segment merging: when we create many small segments
                # to pad out tiered storage metadata, we don't want them to
                # get merged together.
                'cloud_storage_enable_segment_merging': False,
                'disable_batch_cache': True,
                'cloud_storage_cache_check_interval': 1000,
            },
            disable_cloud_storage_diagnostics=True,
            **kwargs)
        si_settings = SISettings(
            self.redpanda._context,
            log_segment_size=self.small_segment_size,
            cloud_storage_cache_size=10 * 1024 * 1024,
        )
        self.redpanda.set_si_settings(si_settings)
        self.rpk = RpkTool(self.redpanda)
        self.s3_port = si_settings.cloud_storage_api_endpoint_port

    def setup_cluster(self,
                      segment_bytes: int,
                      retention_local_bytes: int,
                      extra_cluster_props: dict = {}):
        self.redpanda.set_cluster_config(
            {
                'partition_autobalancing_node_availability_timeout_sec':
                self.unavailable_timeout,
                'partition_autobalancing_mode': 'continuous',
                'raft_learner_recovery_rate': 10 * 1024 * 1024 * 1024,
            } | extra_cluster_props)
        topic_config = {
            # Use a tiny segment size so we can generate many cloud segments
            # very quickly.
            'segment.bytes': segment_bytes,

            # Use infinite retention so there aren't sudden, drastic,
            # unrealistic GCing of logs.
            'retention.bytes': -1,

            # Keep the local retention low for now so we don't get bogged down
            # with an inordinate number of local segments.
            'retention.local.target.bytes': retention_local_bytes,
            'cleanup.policy': 'delete',
        }
        self.rpk.create_topic(self.topic_name,
                              partitions=self.scaled_num_partitions,
                              replicas=3,
                              config=topic_config)

    def load_many_segments(self):
        target_cloud_segments = self.num_segments_per_partition * self.scaled_num_partitions
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                self.small_segment_size,  # msg_size
                int(2 * target_cloud_segments),  # msg_count
                custom_node=[self.preallocated_nodes[0]])
            try:
                producer.start()
                wait_until(lambda: nodes_report_cloud_segments(
                    self.redpanda, target_cloud_segments),
                           timeout_sec=600,
                           backoff_sec=5)
            finally:
                producer.stop()
                producer.wait(timeout_sec=600)
        finally:
            self.free_preallocated_nodes()

        # Once some segments are generated, configure the topic to use more
        # realistic sizes.
        retention_bytes = int(self.scaled_data_bps * 60 * 60 * 6 /
                              self.scaled_num_partitions)
        self.rpk.alter_topic_config(self.topic_name, 'segment.bytes',
                                    self.regular_segment_size)
        self.rpk.alter_topic_config(self.topic_name,
                                    'retention.local.target.bytes',
                                    retention_bytes)

    def get_node(self, idx: int):
        node = self.redpanda.nodes[idx]
        node_id = self.redpanda.node_id(node)
        node_str = f"{node.account.hostname} (node_id: {node_id})"
        return node, node_id, node_str

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_restarts(self):
        self.setup_cluster(segment_bytes=self.small_segment_size,
                           retention_local_bytes=2 * self.small_segment_size)
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=128 * 1024,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=self.scaled_data_bps,
                custom_node=[self.preallocated_nodes[0]])
            try:
                producer.start()
                wait_until(lambda: producer.produce_status.acked > 10000,
                           timeout_sec=60,
                           backoff_sec=1.0)

                # Run a rolling restart.
                self.stage_rolling_restart()

                # Hard stop, then restart.
                self.stage_stop_wait_start(forced_stop=True, downtime=0)

                # Stop a node, wait for enough time for movement to occur, then
                # restart.
                self.stage_stop_wait_start(forced_stop=False,
                                           downtime=self.unavailable_timeout)

                # Stop a node and wait for really long time to collect a lot
                # of under-replicated msgs, then restart.
                # This is not to be run nightly so disabled for now
                #self.stage_stop_wait_start(forced_stop=False, downtime=60*30)

                # Block traffic to/from one node.
                self.stage_block_node_traffic()
            finally:
                producer.stop()
                producer.wait(timeout_sec=600)
        finally:
            self.free_preallocated_nodes()

    NOS3_LOG_ALLOW_LIST = [
        re.compile("s3 - .* - Accessing .*, unexpected REST API error "
                   " detected, code: RequestTimeout"),
    ]

    # Stages for the "test_restarts"

    def stage_rolling_restart(self):
        self.logger.info(f"Rolling restarting nodes")
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            start_timeout=600,
                                            stop_timeout=600)

    def stage_block_node_traffic(self):
        node, node_id, node_str = self.get_node(0)
        self.logger.info("Isolating node {node_str}")
        with FailureInjector(self.redpanda) as fi:
            fi.inject_failure(FailureSpec(FailureSpec.FAILURE_ISOLATE, node))
            try:
                wait_until(lambda: False, timeout_sec=120, backoff_sec=1)
            except:
                pass

        try:
            wait_until(lambda: False, timeout_sec=120, backoff_sec=1)
        except:
            pass
        wait_until(self.redpanda.healthy, timeout_sec=600, backoff_sec=1)

    def stage_stop_wait_start(self, forced_stop: bool, downtime: int):
        node, node_id, node_str = self.get_node(1)
        self.logger.info(
            f"Stopping node {node_str} {'ungracefully' if forced_stop else 'gracefully'}"
        )
        self.redpanda.stop_node(node,
                                forced=forced_stop,
                                timeout=60 if forced_stop else 180)

        self.logger.info(f"Node downtime {downtime} s")
        time.sleep(downtime)

        restart_timeout = 300 + int(900 * downtime / 60)
        self.logger.info(f"Restarting node {node_str} for {restart_timeout} s")
        self.redpanda.start_node(node, timeout=600)
        wait_until(self.redpanda.healthy,
                   timeout_sec=restart_timeout,
                   backoff_sec=1)

    @cluster(num_nodes=5, log_allow_list=NOS3_LOG_ALLOW_LIST)
    def test_disrupt_cloud_storage(self):
        """
        Make segments replicate to the cloud, then disrupt S3 connectivity
        and restore it
        """
        self.setup_cluster(
            # Segments should go into the cloud at a reasonable rate,
            # that's why it is smaller than it should be
            segment_bytes=int(self.scaled_segment_size / 2),
            retention_local_bytes=2 * self.scaled_segment_size,
        )

        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=128 * 1024,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=self.scaled_data_bps,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            wait_until(lambda: producer.produce_status.acked > 10000,
                       timeout_sec=60,
                       backoff_sec=1.0)

            # S3 up -> down -> up
            self.stage_block_s3()

        finally:
            producer.stop()
            producer.wait(timeout_sec=600)
            self.free_preallocated_nodes()

    def _cloud_storage_no_new_errors(self, redpanda, logger=None):
        num_errors = redpanda.metric_sum(
            "redpanda_cloud_storage_errors_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        increase = (num_errors -
                    self.last_num_errors) if self.last_num_errors > 0 else 0
        self.last_num_errors = num_errors
        if logger:
            logger.info(
                f"Cluster metrics report {increase} new cloud storage errors ({num_errors} overall)"
            )
        return increase == 0

    def stage_block_s3(self):
        self.logger.info(f"Getting the first 100 segments into the cloud")
        wait_until(lambda: nodes_report_cloud_segments(self.redpanda, 100),
                   timeout_sec=120,
                   backoff_sec=5)
        self.logger.info(f"Blocking S3 traffic for all nodes")
        self.last_num_errors = 0
        with firewall_blocked(self.redpanda.nodes, self.s3_port):
            # wait for the first cloud related failure + one minute
            wait_until(lambda: not self._cloud_storage_no_new_errors(
                self.redpanda, self.logger),
                       timeout_sec=600,
                       backoff_sec=10)
            time.sleep(60)
        # make sure nothing is crashed
        wait_until(self.redpanda.healthy, timeout_sec=60, backoff_sec=1)
        self.logger.info(f"Waiting for S3 errors to cease")
        wait_until(lambda: self._cloud_storage_no_new_errors(
            self.redpanda, self.logger),
                   timeout_sec=600,
                   backoff_sec=20)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommission_and_add(self):
        """
        Preloads cluster with large number of messages/segments that are also
        replicated to the cloud, and then runs the decommission-and-add a node
        stage. This could have been a part of 'test_combo_preloaded' but
        this stage alone is too heavy and long-lasting, so it's put into a
        separate test.
        """
        self.setup_cluster(segment_bytes=self.small_segment_size,
                           retention_local_bytes=2 * self.small_segment_size)
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=128 * 1024,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=self.scaled_data_bps,
                custom_node=[self.preallocated_nodes[0]])
            try:
                producer.start()
                wait_until(lambda: producer.produce_status.acked > 10000,
                           timeout_sec=60,
                           backoff_sec=1.0)

                # Decommission.
                # Add node (after stage_decommission has freed up a node)
                # and wait for partitions to move in.
                self.stage_decommission_and_add()
            finally:
                producer.stop()
                producer.wait(timeout_sec=600)
        finally:
            self.free_preallocated_nodes()
            
    def stage_decommission_and_add(self):
        node, node_id, node_str = self.get_node(1)

        def topic_partitions_on_node():
            try:
                parts = self.redpanda.partitions(self.topic_name)
            except StopIteration:
                return 0
            n = sum([
                1 if r.account == node.account else 0 for p in parts
                for r in p.replicas
            ])
            self.logger.debug(f"Partitions in the node-topic: {n}")
            return n
        
        nt_partitions_before = topic_partitions_on_node()

        self.logger.info(
            f"Decommissioning node {node_str}, partitions: {nt_partitions_before}"
        )
        decomm_time = time.monotonic()
        admin = self.redpanda._admin
        admin.decommission_broker(node_id)
        waiter = NodeDecommissionWaiter(self.redpanda,
                                        node_id,
                                        self.logger,
                                        progress_timeout=120)
        waiter.wait_for_removal()
        self.redpanda.stop_node(node)
        assert topic_partitions_on_node() == 0
        decomm_time = time.monotonic() - decomm_time

        self.logger.info(f"Adding a node")
        self.redpanda.clean_node(node,
                                 preserve_logs=True,
                                 preserve_current_install=True)
        self.redpanda.start_node(node,
                                 auto_assign_node_id=False,
                                 omit_seeds_on_idx_one=False)
        wait_until(self.redpanda.healthy, timeout_sec=600, backoff_sec=1)
        new_node_id = self.redpanda.node_id(node, force_refresh=True)

        self.logger.info(
            f"Node added, new node_id: {new_node_id}, waiting for {int(nt_partitions_before/2)} partitions to move there in {int(decomm_time)} s"
        )
        wait_until(lambda: topic_partitions_on_node() > nt_partitions_before / 2,
                   timeout_sec=max(60, decomm_time),
                   backoff_sec=2)
        self.logger.info(f"{topic_partitions_on_node()} partitions moved")

    @cluster(num_nodes=5, log_allow_list=NOS3_LOG_ALLOW_LIST)
    def test_cloud_cache_thrash(self):
        """
        Try to exhaust cloud cache by reading at random offsets with many
        consumers
        """
        segment_size = int(self.scaled_segment_size / 8)
        self.setup_cluster(segment_bytes=segment_size,
                           retention_local_bytes=2 * segment_size,
                           extra_cluster_props={
                               'cloud_storage_max_readers_per_shard': 256,
                           })

        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=128 * 1024,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=self.scaled_data_bps,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            wait_until(lambda: producer.produce_status.acked > 5000,
                       timeout_sec=60,
                       backoff_sec=1.0)
            target_cloud_segments = 10 * self.scaled_num_partitions
            wait_until(lambda: nodes_report_cloud_segments(
                self.redpanda, target_cloud_segments),
                       timeout_sec=600,
                       backoff_sec=5)
            producer.wait_for_offset_map()

            # Exhaust cloud cache with multiple consumers
            # reading at random offsets
            self.stage_cloud_cache_thrash()

        finally:
            producer.stop()
            producer.wait(timeout_sec=600)
            self.free_preallocated_nodes()

    def stage_cloud_cache_thrash(self):
        self.logger.info(f"Starting consumers")
        consumer = KgoVerifierRandomConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=128 * 1024,
            rand_read_msgs=1,
            parallel=4,
            nodes=[self.preallocated_nodes[0]],
            debug_logs=True,
        )
        try:
            consumer.start(clean=False)
            time.sleep(240)
        finally:
            self.logger.info(f"Stopping consumers")
            consumer.stop()
            consumer.wait(timeout_sec=600)

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_consume(self):
        self.setup_cluster(segment_bytes=self.small_segment_size,
                           retention_local_bytes=2 * self.small_segment_size)
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        producer = None
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=self.msg_size,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=self.scaled_data_bps,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            wait_until(lambda: producer.produce_status.acked > 10000,
                       timeout_sec=60,
                       backoff_sec=1.0)

            self.stage_lots_of_failed_consumers()
            self.stage_hard_restart(producer)

        finally:
            producer.stop()
            producer.wait(timeout_sec=600)
            self.free_preallocated_nodes()

    @cluster(num_nodes=10, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_ts_resource_utilization(self):
        self.stage_tiered_storage_consuming()

    def stage_lots_of_failed_consumers(self):
        # This stage sequentially starts 1,000 consumers. Then allows
        # them to consume ~10 messages before terminating them with
        # a SIGKILL.

        self.logger.info(f"Starting stage_lots_of_failed_consumers")

        consume_count = 10000

        def random_stop_check(consumer):
            if consumer.message_count >= min(10, consume_count):
                return True
            else:
                return False

        while consume_count > 0:
            consumer = RpkConsumer(self._ctx,
                                   self.redpanda,
                                   self.topic_name,
                                   offset="newest",
                                   num_msgs=consume_count)
            consumer.start()
            wait_until(lambda: random_stop_check(consumer),
                       timeout_sec=10,
                       backoff_sec=0.001)

            consumer.stop()
            consumer.free()

            consume_count -= consumer.message_count
            self.logger.warn(f"consumed {consumer.message_count} messages")

    def _consume_from_offset(self, topic_name: str, msg_count: int,
                             partition: int, starting_offset: str,
                             timeout_per_topic: int):
        def consumer_saw_msgs(consumer):
            self.logger.info(
                f"Consumer message_count={consumer.message_count} / {msg_count}"
            )
            # Tolerate greater-than, because if there were errors during production
            # there can have been retries.
            return consumer.message_count >= msg_count

        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               topic_name,
                               save_msgs=True,
                               partitions=[partition],
                               offset=starting_offset,
                               num_msgs=msg_count)
        consumer.start()
        wait_until(lambda: consumer_saw_msgs(consumer),
                   timeout_sec=timeout_per_topic,
                   backoff_sec=0.125)

        consumer.stop()
        consumer.free()

        if starting_offset.isnumeric():
            expected_offset = int(starting_offset)
            actual_offset = int(consumer.messages[0]['offset'])
            assert expected_offset == actual_offset, "expected_offset != actual_offset"

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_consume_miss_cache(self):
        self.setup_cluster(segment_bytes=self.small_segment_size,
                           retention_local_bytes=2 * self.small_segment_size)
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        producer = None
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=self.msg_size,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=self.scaled_data_bps,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            wait_until(lambda: producer.produce_status.acked > 10000,
                       timeout_sec=60,
                       backoff_sec=1.0)

            # this stage could have been a part of test_consume however one
            # of the checks requires nicely balanced replicas, and this is
            # something other test_consume stages could break
            self.stage_consume_miss_cache(producer)

        finally:
            producer.stop()
            producer.wait(timeout_sec=600)
            self.free_preallocated_nodes()

    def stage_consume_miss_cache(self, producer: KgoVerifierProducer):
        # This stage produces enough data to each RP node to fill up their batch caches
        # for a specific topic. Then it consumes from offsets known to not be in the
        # batch and verifies that these fetches occurred from disk and not the cache.

        self.logger.info(f"Starting stage_consume_miss_cache")

        # Get current offsets for topic. We'll use these for starting offsets
        # for consuming messages after we produce enough data to push them out
        # of the batch cache.
        last_offsets = [(p.id, p.high_watermark)
                        for p in self.rpk.describe_topic(self.topic_name)
                        if p.high_watermark is not None]

        partition_size_check: list[MetricCheck] = []
        partition_size_metric = "vectorized_storage_log_partition_size"

        for node in self.redpanda.nodes:
            partition_size_check.append(
                MetricCheck(self.logger,
                            self.redpanda,
                            node,
                            partition_size_metric,
                            reduce=sum))

        # wait for producer to produce enough to exceed the batch cache by some margin
        # For a test on 4x `is4gen.4xlarge` there is about 0.13 GiB/s of throughput per node.
        # This would mean we'd be waiting 90GiB / 0.13 GiB/s = 688s or 11.5 minutes to ensure
        # the cache has been filled by the producer.
        produce_rate_per_node_bytes_s = self.scaled_data_bps / self.num_brokers
        batch_cache_max_memory = self.memory_per_broker_bytes
        time_till_memory_full_per_node = batch_cache_max_memory / produce_rate_per_node_bytes_s
        required_wait_time_s = 1.5 * time_till_memory_full_per_node

        self.logger.info(
            f"Expecting to wait {required_wait_time_s} seconds. "
            f"time_till_memory_full_per_node: {time_till_memory_full_per_node}, "
            f"produce_rate_per_node_bytes_s: {produce_rate_per_node_bytes_s}")

        current_sent = producer.produce_status.sent
        expected_sent = math.ceil(
            (self.num_brokers * batch_cache_max_memory) / self.msg_size)

        self.logger.info(
            f"{current_sent} currently sent messages. Waiting for {expected_sent} messages to be sent"
        )

        def producer_complete():
            number_left = (current_sent +
                           expected_sent) - producer.produce_status.sent
            self.logger.info(f"{number_left} messages still need to be sent.")
            return number_left <= 0

        wait_until(producer_complete,
                   timeout_sec=required_wait_time_s,
                   backoff_sec=30)

        post_prod_offsets = [(p.id, p.high_watermark)
                             for p in self.rpk.describe_topic(self.topic_name)
                             if p.high_watermark is not None]

        offset_deltas = [
            (c[0][0], c[0][1] - c[1][1])
            for c in list(itertools.product(post_prod_offsets, last_offsets))
            if c[0][0] == c[1][0]
        ]
        avg_delta = sum([d[1] for d in offset_deltas]) / len(offset_deltas)

        self.logger.info(
            f"Finished waiting for batch cache to fill. Avg log offset delta: {avg_delta}"
        )

        # Report under-replicated status for the context in the case the following check fails
        s = self.redpanda.metrics_sample(
            "vectorized_cluster_partition_under_replicated_replicas")
        if sum(x.value for x in s.samples) == 0:
            self.logger.info(f"No under-replicated replicas")
        else:
            self.logger.info(f"Under-replicated replicas: {s.samples}")

        # Ensure total partition size increased by the amount expected.
        for check in partition_size_check:

            def check_partition_size(old_size, new_size):
                self.logger.info(
                    f"Total increase in size for partitions: {new_size-old_size}, "
                    f"checked from {check.node.name}")

                unreplicated_size_inc = (new_size - old_size) / 3
                return unreplicated_size_inc >= batch_cache_max_memory

            check.expect([(partition_size_metric, check_partition_size)])

        # Ensure there are metrics available for the topic we're consuming from
        for p_id, _ in last_offsets:
            self._consume_from_offset(self.topic_name, 1, p_id, "newest", 10)

        # Stop the producer temporarily as produce requests can cause
        # batch cache reads which causes false negatives on cache misses.
        producer.stop()

        check_batch_cache_reads = []
        cache_metrics = [
            "vectorized_storage_log_read_bytes_total",
            "vectorized_storage_log_cached_read_bytes_total",
        ]

        for node in self.redpanda.nodes:
            check_batch_cache_reads.append(
                MetricCheck(self.logger,
                            self.redpanda,
                            node,
                            cache_metrics,
                            reduce=sum))

        # start consuming at the offsets recorded before the wait.
        # at this point we can be sure they are not from the batch cache.
        messages_to_read = 1
        timeout_seconds = 60

        for p_id, p_hw in last_offsets:
            self._consume_from_offset(self.topic_name, messages_to_read, p_id,
                                      str(p_hw), timeout_seconds)

        def check_cache_bytes_ratio(old, new):
            log_diff = int(new[cache_metrics[0]]) - int(old[cache_metrics[0]])
            cache_diff = int(new[cache_metrics[1]]) - int(
                old[cache_metrics[1]])
            cache_hit_percent = cache_diff / log_diff

            self.logger.info(
                f"BYTES: log_diff: {log_diff} cache_diff: {cache_diff} cache_hit_percent: {cache_hit_percent}"
            )
            return cache_hit_percent <= 0.4

        for check in check_batch_cache_reads:
            ok = check.evaluate_groups([(cache_metrics,
                                         check_cache_bytes_ratio)])

            assert ok, "cache hit ratio is higher than expected"

    def _run_omb(self, produce_bps,
                 validator_overrides) -> OpenMessagingBenchmark:
        topic_count = 1
        partitions_per_topic = self.scaled_num_partitions
        workload = {
            "name": "StabilityTest",
            "topics": topic_count,
            "partitions_per_topic": partitions_per_topic,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 2,
            "producers_per_topic": 2,
            "producer_rate": int(produce_bps / (4 * 1024)),
            "message_size": 4 * 1024,
            "payload_file": "payload/payload-4Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 3,
            "warmup_duration_minutes": 1,
        }

        bench_node = self.preallocated_nodes[0]
        worker_nodes = self.preallocated_nodes[1:]

        benchmark = OpenMessagingBenchmark(
            self._ctx, self.redpanda, "SIMPLE_DRIVER",
            (workload, OMBSampleConfigurations.UNIT_TEST_LATENCY_VALIDATOR
             | validator_overrides))

        benchmark.start()
        return benchmark

    def stage_tiered_storage_consuming(self):
        # This stage starts two consume + produce workloads concurrently.
        # One workload being a usual one without tiered storage that is
        # consuming entirely from the batch cache. The other being a outlier
        # where the consumer is consuming entirely from S3. The stage then
        # ensures that the S3 workload doesn't impact the performance of the
        # usual workload too greatly.

        self.logger.info(f"Starting stage_tiered_storage_consuming")

        segment_size = 128 * 1024 * 1024  # 128 MiB
        consume_rate = 1 * 1024 * 1024 * 1024  # 1 GiB/s

        # create a new topic with low local retention.
        config = {
            'segment.bytes': segment_size,
            'retention.bytes': -1,
            'retention.local.target.bytes': 2 * segment_size,
            'cleanup.policy': 'delete',
            'partition_autobalancing_node_availability_timeout_sec':
            self.unavailable_timeout,
            'partition_autobalancing_mode': 'continuous',
            'raft_learner_recovery_rate': 10 * 1024 * 1024 * 1024,
        }
        self.rpk.create_topic(self.topic_name,
                              partitions=self.scaled_num_partitions,
                              replicas=3,
                              config=config)

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic_name,
                                       msg_size=self.msg_size,
                                       msg_count=5 * 1024 * 1024 * 1024 * 1024,
                                       rate_limit_bps=self.scaled_data_bps)
        producer.start()

        # produce 10 mins worth of consume data onto S3.
        produce_time_s = 4 * 60
        messages_to_produce = (produce_time_s * consume_rate) / self.msg_size
        time_to_wait = (messages_to_produce *
                        self.msg_size) / self.scaled_data_bps

        wait_until(
            lambda: producer.produce_status.acked >= messages_to_produce,
            timeout_sec=1.5 * time_to_wait,
            backoff_sec=5,
            err_msg=
            f"Could not ack production of {messages_to_produce} messages in {1.5 * time_to_wait} s"
        )
        # continue to produce the rest of the test

        validator_overrides = {
            OMBSampleConfigurations.E2E_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(51)],
            OMBSampleConfigurations.E2E_LATENCY_AVG:
            [OMBSampleConfigurations.lte(145)],
        }

        # Run a usual producer + consumer workload and a S3 producer + consumer workload concurrently
        # Ensure that the S3 workload doesn't effect the usual workload majorly.
        benchmark = self._run_omb(self.scaled_data_bps / 2,
                                  validator_overrides)

        # This consumer should largely be reading from S3
        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               self.topic_name,
                               offset="oldest",
                               num_msgs=messages_to_produce)
        consumer.start()
        wait_until(
            lambda: consumer.message_count >= messages_to_produce,
            timeout_sec=5 * produce_time_s,
            backoff_sec=5,
            err_msg=
            f"Could not consume {messages_to_produce} msgs in {5 * produce_time_s} s"
        )
        consumer.stop()
        consumer.free()

        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    def stage_hard_restart(self, producer):
        # This stage force stops all Redpanda nodes. It then
        # starts them all again and verifies the cluster is
        # healthy. Afterwards is runs some basic produce + consume
        # operations.

        self.logger.info(f"Starting stage_hard_restart")

        # hard stop all nodes
        self.logger.info("stopping all redpanda nodes")
        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node, forced=True)

        # start all nodes again
        self.logger.info("starting all redpanda nodes")
        for node in self.redpanda.nodes:
            self.redpanda.start_node(node, timeout=600)

        # wait until the cluster is health once more
        self.logger.info("waiting for RP cluster to be healthy")
        wait_until(self.redpanda.healthy, timeout_sec=600, backoff_sec=1)

        # verify basic produce and consume operations still work.

        self.logger.info("checking basic producer functions")
        current_sent = producer.produce_status.sent
        produce_count = 100

        def producer_complete():
            number_left = (current_sent +
                           produce_count) - producer.produce_status.sent
            self.logger.info(f"{number_left} messages still need to be sent.")
            return number_left <= 0

        wait_until(producer_complete, timeout_sec=60, backoff_sec=1)

        self.logger.info("checking basic consumer functions")
        current_sent = producer.produce_status.sent
        consume_count = 100
        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               self.topic_name,
                               offset="newest",
                               num_msgs=consume_count)
        consumer.start()
        wait_until(lambda: consumer.message_count >= consume_count,
                   timeout_sec=60,
                   backoff_sec=1,
                   err_msg=f"Could not consume {consume_count} msgs in 1 min")

        consumer.stop()
        consumer.free()
