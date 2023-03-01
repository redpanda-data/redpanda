# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.utils.si_utils import nodes_report_cloud_segments
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings
from rptest.services.kgo_verifier_services import KgoVerifierProducer
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
    unavailable_timeout = 60

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(TieredStorageWithLoadTest, self).__init__(
            test_ctx,
            *args,
            num_brokers=4,
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
            },
            disable_cloud_storage_diagnostics=True,
            **kwargs)
        si_settings = SISettings(self.redpanda._context,
                                 log_segment_size=self.small_segment_size)
        self.redpanda.set_si_settings(si_settings)
        self.rpk = RpkTool(self.redpanda)

    def load_many_segments(self):
        unscaled_data_bps = int(1.7 * 1024 * 1024 * 1024)
        unscaled_num_partitions = 1024
        scaling_factor = 4 / 13

        data_bps = int(unscaled_data_bps * scaling_factor)  # ~0.53 GiB/s
        num_partitions = int(unscaled_num_partitions * scaling_factor)  # 315
        num_segments_per_partition = 1000

        config = {
            # Use a tiny segment size so we can generate many cloud segments
            # very quickly.
            'segment.bytes': self.small_segment_size,

            # Use infinite retention so there aren't sudden, drastic,
            # unrealistic GCing of logs.
            'retention.bytes': -1,

            # Keep the local retention low for now so we don't get bogged down
            # with an inordinate number of local segments.
            'retention.local.target.bytes': 2 * self.small_segment_size,
            'cleanup.policy': 'delete',
            'partition_autobalancing_node_availability_timeout_sec':
            self.unavailable_timeout,
            'partition_autobalancing_mode': 'continuous',
            'raft_learner_recovery_rate': 10 * 1024 * 1024 * 1024,
        }
        self.rpk.create_topic(self.topic_name,
                              partitions=num_partitions,
                              replicas=3,
                              config=config)

        target_cloud_segments = num_segments_per_partition * num_partitions
        producer = None
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                self.small_segment_size,
                int(2 * num_segments_per_partition * num_partitions),
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            wait_until(lambda: nodes_report_cloud_segments(
                self.redpanda, target_cloud_segments),
                       timeout_sec=600,
                       backoff_sec=5)
        finally:
            producer.stop()
            producer.wait(timeout_sec=600)
            self.free_preallocated_nodes()

        # Once some segments are generated, configure the topic to use more
        # realistic sizes.
        retention_bytes = int(data_bps * 60 * 60 * 6 / num_partitions)
        self.rpk.alter_topic_config(self.topic_name,
                                    'retention.local.target.bytes',
                                    retention_bytes)
        self.rpk.alter_topic_config(self.topic_name, 'segment.bytes',
                                    512 * 1024 * 1024)

    def get_node(self, idx: int):
        node = self.redpanda.nodes[idx]
        node_id = self.redpanda.node_id(node)
        node_str = f"{node.account.hostname} (node_id: {node_id})"
        return node, node_id, node_str

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_restarts(self):
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        producer = None
        unscaled_data_bps = int(1.7 * 1024 * 1024 * 1024)
        scaling_factor = 4 / 13

        data_bps = int(unscaled_data_bps * scaling_factor)  # ~0.53 GiB/s
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=128 * 1024,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=data_bps,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            wait_until(lambda: producer.produce_status.acked > 10000,
                       timeout_sec=60,
                       backoff_sec=1.0)

            # Run a rolling restart.
            self.stage_rolling_restart()

            # Hard stop, then restart.
            self.stage_hard_stop_start()

            # Stop a node, wait for enough time for movement to occur, then
            # restart.
            self.stage_stop_wait_start()

            # Block traffic to/from one node.
            self.stage_block_node_traffic()

            # Decommission.
            self.stage_decommission()
        finally:
            producer.stop()
            producer.wait(timeout_sec=600)
            self.free_preallocated_nodes()

    # Stages for the "test_restarts"

    def stage_rolling_restart(self):
        self.logger.info(f"Rolling restarting nodes")
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes, start_timeout=600, stop_timeout=600)

    def stage_hard_stop_start(self):
        node, node_id, node_str = self.get_node(0)
        self.logger.info(f"Hard stopping and restarting node {node_str}")
        self.redpanda.stop_node(node, forced=True)
        self.redpanda.start_node(node, timeout=600)
        wait_until(self.redpanda.healthy, timeout_sec=600, backoff_sec=1)

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

    def stage_stop_wait_start(self):
        node, node_id, node_str = self.get_node(1)
        self.logger.info(f"Hard stopping node {node_str}")
        self.redpanda.stop_node(node, forced=True)
        time.sleep(60)
        self.logger.info(f"Restarting node {node_str}")
        self.redpanda.start_node(node, timeout=600)
        wait_until(self.redpanda.healthy, timeout_sec=600, backoff_sec=1)

    def stage_decommission(self):
        node, node_id, node_str = self.get_node(0)
        self.logger.info(f"Decommissioning node {node_str}")
        admin = self.redpanda._admin
        admin.decommission_broker(node_id)
        waiter = NodeDecommissionWaiter(self.redpanda, node_id, self.logger)
        waiter.wait_for_removal()
        self.redpanda.stop_node(node)
