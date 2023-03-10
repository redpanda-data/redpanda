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
import time


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
