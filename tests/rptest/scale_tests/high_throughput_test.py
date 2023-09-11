# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import itertools
import math
import re
import time
from typing import Optional

from ducktape.mark import ignore, ok_to_fail
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.kgo_verifier_services import (KgoVerifierProducer,
                                                   KgoVerifierRandomConsumer)
from rptest.services.metrics_check import MetricCheck
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda import (RESTART_LOG_ALLOW_LIST,
                                      AdvertisedTierConfig, CloudTierName,
                                      MetricsEndpoint, SISettings)
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import firewall_blocked
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.utils.si_utils import nodes_report_cloud_segments

KiB = 1024
MiB = KiB * KiB
GiB = KiB * MiB
minutes = 60
hours = 60 * minutes


class CloudTierConfig:
    def __init__(self, ingress_rate: float, egress_rate: float,
                 num_brokers: int, num_brokers_scaled: int, segment_size: int,
                 cloud_cache_size: int, partitions_min: int,
                 partitions_max: int, connections_limit: Optional[int],
                 rpo: int, memory_per_broker: int) -> None:
        self.ingress_rate = int(ingress_rate)
        self.egress_rate = int(egress_rate)
        self.num_brokers = num_brokers
        self.num_brokers_scaled = num_brokers_scaled
        self.segment_size = segment_size
        self.cloud_cache_size = cloud_cache_size
        self.partitions_min = partitions_min
        self.partitions_max = partitions_max
        self.connections_limit = connections_limit
        self.rpo = rpo
        self.memory_per_broker = memory_per_broker
        self.scaling_factor = num_brokers_scaled / num_brokers

    @property
    def segment_size_scaled(self):
        return int(self.segment_size * self.scaling_factor)

    @property
    def partitions_max_scaled(self):
        return int(self.partitions_max * self.scaling_factor)

    @property
    def ingress_rate_scaled(self):
        return int(self.ingress_rate * self.scaling_factor)

    @property
    def egress_rate_scaled(self):
        return int(self.egress_rate * self.scaling_factor)

    @property
    def connections_limit_scaled(self):
        if self.connections_limit is None:
            return None
        return int(self.connections_limit * self.scaling_factor)


# yapf: disable
CloudTierConfigs = {
    # In-order parameters value:
    #  1  ingress
    #  2  egress
    #  3  num_brokers
    #  4  num_brokers_scaled
    #  5  segment_size
    #  6  cloud_cache_size
    #  7  partitions_min
    #  8  partitions_max
    #  9  connections
    #  10 rpo
    #  11 memory_per_broker
    "Upscale-1": CloudTierConfig(
        1.7*GiB, 8.5*GiB, 13,  4, 512*MiB,   10*MiB, 1024, 1024,  None, 1*hours, 96*GiB
    ),
    "tier-1-aws": CloudTierConfig(
         25*MiB,  75*MiB,  3,  3, 512*MiB,  300*GiB,   20, 1000,  1500, 1*hours, 16*GiB
    ),
    "tier-2-aws": CloudTierConfig(
         50*MiB, 150*MiB,  3,  3, 512*MiB,  500*GiB,   50, 2000,  3750, 1*hours, 32*GiB
    ),
    "tier-3-aws": CloudTierConfig(
        100*MiB, 200*MiB,  6,  6, 512*MiB,  500*GiB,  100, 5000,  7500, 1*hours, 32*GiB
    ),
    "tier-4-aws": CloudTierConfig(
        200*MiB, 400*MiB,  6,  6,   1*GiB, 1000*GiB,  100, 5000, 15000, 1*hours, 96*GiB
    ),
    "tier-5-aws": CloudTierConfig(
        300*MiB, 600*MiB,  9,  9,   1*GiB, 1000*GiB,  150, 7500, 22500, 1*hours, 96*GiB
    ),
    "tier-1-gcp": CloudTierConfig(
         25*MiB,  60*MiB,  3,  3, 512*MiB,  150*GiB,   20,  500,  1500, 1*hours,  8*GiB
    ),
    "tier-2-gcp": CloudTierConfig(
         50*MiB, 150*MiB,  3,  3, 512*MiB,  300*GiB,   50, 1000,  3750, 1*hours, 32*GiB
    ),
    "tier-3-gcp": CloudTierConfig(
        100*MiB, 200*MiB,  6,  6, 512*MiB,  320*GiB,  100, 3000,  7500, 1*hours, 32*GiB
    ),
    "tier-4-gcp": CloudTierConfig(
        200*MiB, 400*MiB,  9,  9, 512*MiB,  350*GiB,  100, 5000, 15000, 1*hours, 32*GiB
    ),
    "tier-5-gcp": CloudTierConfig(
        400*MiB, 600*MiB, 12, 12,   1*GiB,  750*GiB,  100, 7500, 22500, 1*hours, 32*GiB
    ),
}
# yapf: enable

NoncloudTierConfigs = {
    #   ingress|          segment size|       partitions max|
    #             egress|       cloud cache size|connections # limit|
    #           # of brokers|           partitions min|           memory per broker|
    "docker-local":
    AdvertisedTierConfig(3 * MiB, 9 * MiB, 3, 128 * MiB, 20 * GiB, 1, 100, 100,
                         2 * GiB),
}


class HighThroughputTest2(PreallocNodesTest):
    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        self._ctx = test_ctx

        cloud_tier_str = test_ctx.globals.get("cloud_tier", "docker-local")
        if cloud_tier_str in NoncloudTierConfigs.keys():
            cloud_tier = None
            self.tier_config = NoncloudTierConfigs[cloud_tier_str]
            extra_rp_conf = {
                'log_segment_size': self.tier_config.segment_size,
                'cloud_storage_cache_size': self.tier_config.cloud_cache_size,
                # Disable segment merging: when we create many small segments
                # to pad out tiered storage metadata, we don't want them to
                # get merged together.
                'cloud_storage_enable_segment_merging': False,
                'disable_batch_cache': True,
            }
            num_brokers = self.tier_config.num_brokers

        else:
            try:
                cloud_tier = CloudTierName(cloud_tier_str)
            except ValueError:
                raise RuntimeError(
                    f"Unknown cloud tier specified: {cloud_tier_str}. Supported tiers: {CloudTierName.list()+NoncloudTierConfigs.keys()}"
                )
            extra_rp_conf = None
            num_brokers = None

        super(HighThroughputTest2,
              self).__init__(test_ctx,
                             1,
                             *args,
                             num_brokers=num_brokers,
                             extra_rp_conf=extra_rp_conf,
                             cloud_tier=cloud_tier,
                             disable_cloud_storage_diagnostics=True,
                             **kwargs)

        if cloud_tier is not None:
            self.tier_config = self.redpanda.advertised_tier_config
        test_ctx.logger.info(f"Cloud tier {cloud_tier}: {self.tier_config}")

        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_throughput_simple(self):

        ingress_rate = self.tier_config.ingress_rate
        self.topics = [
            TopicSpec(partition_count=self.tier_config.partitions_min)
        ]
        super(HighThroughputTest2, self).setUp()

        target_run_time = 15
        target_data_size = target_run_time * ingress_rate
        msg_count = int(math.sqrt(target_data_size) / 2)
        msg_size = target_data_size // msg_count
        self.logger.info(
            f"Producing {msg_count} messages of {msg_size} B, "
            f"{msg_count*msg_size} B total, target rate {ingress_rate} B/s")

        # if this is a redpanda cloud cluster,
        # use the default test superuser user/pass
        security_config = self.redpanda.security_config()
        username = security_config.get('sasl_plain_username', None)
        password = security_config.get('sasl_plain_password', None)
        enable_tls = security_config.get('enable_tls', False)
        producer0 = KgoVerifierProducer(self.test_context,
                                        self.redpanda,
                                        self.topic,
                                        msg_size=msg_size,
                                        msg_count=msg_count,
                                        username=username,
                                        password=password,
                                        enable_tls=enable_tls)
        producer0.start()
        start = time.time()

        producer0.wait()
        time0 = time.time() - start

        producer0.stop()
        producer0.free()

        rate0 = msg_count * msg_size / time0
        self.logger.info(f"Producer: {time0} s, {rate0} B/s")
        assert rate0 / ingress_rate > 0.5, f"Producer rate is too low. measured: {rate0} B/s, expected: {ingress_rate} B/s"

    def _stage_setup_cluster(self, name, partitions):
        """
        Setup cluster for tests. FMC/BYOC
        """
        topic_config = {
            # Use a tiny segment size so we can generate many cloud segments
            # very quickly.
            'segment.bytes': 256 * MiB,

            # Use infinite retention so there aren't sudden, drastic,
            # unrealistic GCing of logs.
            'retention.bytes': -1,

            # Keep the local retention low for now so we don't get bogged down
            # with an inordinate number of local segments.
            'retention.local.target.bytes': 2 * 256 * MiB,
            'cleanup.policy': 'delete',
        }

        self.logger.info("Setting up cluster")
        # Returned object is 'map', convert it to list
        _li = list(self.rpk.list_topics())
        if name in _li:
            self.logger.info("Recreating test topic")
            self.rpk.delete_topic(name)
        else:
            self.logger.info("Creating test topic")
        self.rpk.create_topic(name,
                              partitions=partitions,
                              replicas=1,
                              config=topic_config)
        return

    def _get_swarm_connections_count(self, nodes):
        _list = []
        for sw_node in nodes:
            for node in sw_node.nodes:
                _current = int(
                    node.account.ssh_output(
                        "netstat -pan -t tcp | grep ESTABLISHED "
                        "| grep ':9092' | grep 'client-swarm' | wc -l").decode(
                            "utf-8").strip())
                _list.append(_current)
        return _list

    @cluster(num_nodes=3)
    def test_max_connections(self):
        # Consts
        TOPIC_NAME = "maxconnections-check"
        PRODUCER_TIMEOUT_MS = 5000

        tier_config = self.redpanda.advertised_tier_config
        _partition_count = tier_config.num_brokers

        # Prepare cluster
        self._stage_setup_cluster(TOPIC_NAME, _partition_count)

        # setup ProducerSwarm parameters
        producer_kwargs = {}
        producer_kwargs['min_record_size'] = 64
        producer_kwargs['max_record_size'] = 64

        effective_msg_size = producer_kwargs['min_record_size'] + (
            producer_kwargs['max_record_size'] -
            producer_kwargs['min_record_size']) // 2

        # connections per node at max (tier-5) is ~3700
        # We sending 10% above the limit to reach target
        _target_per_node = int(tier_config.connections_limit //
                               len(self.cluster.nodes))
        _conn_per_node = int(_target_per_node * 1.1)
        # calculate message rate based on 10 MB/s per node
        # 10 MiB = 3 messages per producer per sec
        msg_rate = int((0.1 * MiB) // effective_msg_size)
        messages_per_sec_per_producer = msg_rate // _conn_per_node
        # single producer runtime
        target_runtime_s = 30
        # for 50 MiB total messag30e count is 3060
        records_per_producer = messages_per_sec_per_producer * target_runtime_s

        producer_kwargs[
            'messages_per_second_per_producer'] = messages_per_sec_per_producer

        # Initialize all 3 nodes with proper values
        swarm = []
        for idx in range(len(self.cluster.nodes), 0, -1):
            # First one will be longest, last shortest
            _records = records_per_producer * idx
            _swarm_node = ProducerSwarm(self._ctx,
                                        self.redpanda,
                                        TOPIC_NAME,
                                        int(_conn_per_node),
                                        int(_records),
                                        timeout_ms=PRODUCER_TIMEOUT_MS,
                                        **producer_kwargs)

            swarm.append(_swarm_node)

        # Start producing
        self.logger.warn(f"Start swarming from {len(swarm)} nodes: "
                         f"{_conn_per_node} connections per node, "
                         f"{records_per_producer} msg each producer")
        # Total connections
        _total = 0
        connectMax = 0
        _start = time.time()
        # Current swarm node started
        idx = 1
        while idx <= len(swarm):
            # Start first node
            self.logger.warn(f"Starting swarm node {idx}")
            swarm[idx - 1].start()
            # Next swarm node
            idx += 1

        # Track Connections
        _now = time.time()
        idx = len(swarm)
        while (_now - _start) < target_runtime_s:
            _now = time.time()
            _connections = self._get_swarm_connections_count(swarm)
            _total = sum(_connections)
            self.logger.warn(f"{_total} connections "
                             f"({'/'.join(map(str, _connections))}) "
                             f"at {_now - _start:.3f}s")
            # Save maximum
            connectMax = _total if connectMax < _total else connectMax
            # Once reaches _conn_per_node * idx,
            # proceed to another one
            _target = (_target_per_node * idx)
            # Check if target reached and break out if yes
            if _total >= _target:
                self.logger.warn(f"Reached target of {_target} "
                                 f"connections ({_total})")
                break
            else:
                # sleep before next measurement
                time.sleep(10)

        # wait for the end
        self.logger.warn("Waiting for swarm to finish")
        for node in swarm:
            node.wait(timeout_sec=1200)
        _now = time.time()
        self.logger.warn(f"Done swarming after {_now - _start}s")

        # Check message count
        _hwm = 0
        expected_msg_count = sum([
            records_per_producer * _conn_per_node * idx
            for idx in range(len(self.cluster.nodes), 0, -1)
        ])
        for partition in self.rpk.describe_topic(TOPIC_NAME):
            # Add currect high watermark for topic
            _hwm += partition.high_watermark

        # Check that all messages make it through
        # Since there is +10% on connections set
        # Message count should be > expected * 0.9
        reasonably_expected = int(expected_msg_count * 0.9)
        self.logger.warn(f"Expected more than {reasonably_expected} messages "
                         f"out of {expected_msg_count}, actual {_hwm}")
        assert _hwm >= expected_msg_count

        # stage delete
        self.logger.warn("Deleting topic")
        self.rpk.delete_topic(TOPIC_NAME)

        # Assert that target connection count is reached
        self.logger.warn(
            f"Reached {connectMax} of {tier_config.connections_limit} needed")
        assert connectMax >= tier_config.connections_limit

        return


class HighThroughputTest(PreallocNodesTest):
    # Redpanda is responsible for bounding its own startup time via
    # STORAGE_TARGET_REPLAY_BYTES.  The resulting walltime for startup
    # depends on the speed of the disk.  60 seconds is long enough
    # for an i3en.xlarge (and more than enough for faster instance types)
    EXPECT_START_TIME = 60

    LEADER_BALANCER_PERIOD_MS = 30000
    topic_name = "tiered_storage_topic"
    small_segment_size = 4 * KiB
    num_segments_per_partition = 1000
    unavailable_timeout = 60
    msg_size = 128 * KiB

    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        self._ctx = test_ctx

        cloud_tier = test_ctx.globals.get("cloud_tier",
                                          "tier-1-aws").removeprefix("Tier-")
        self.config = CloudTierConfigs.get(cloud_tier)
        assert not self.config is None, f"Unknown cloud tier specified: {cloud_tier}. "\
            f"Supported tiers: {CloudTierConfigs.keys()}"
        test_ctx.logger.info(
            f"Cloud tier {cloud_tier}: {self.config.__dict__}")

        extra_rp_conf = {
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
        }
        if not self.config.connections_limit_scaled is None:
            extra_rp_conf |= {
                'kafka_connections_max': self.config.connections_limit_scaled
            }

        super(HighThroughputTest,
              self).__init__(test_ctx,
                             *args,
                             num_brokers=self.config.num_brokers_scaled,
                             node_prealloc_count=1,
                             extra_rp_conf=extra_rp_conf,
                             disable_cloud_storage_diagnostics=True,
                             **kwargs)
        si_settings = SISettings(
            self.redpanda._context,
            log_segment_size=self.small_segment_size,
            cloud_storage_cache_size=self.config.cloud_cache_size,
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
                'raft_learner_recovery_rate': 10 * GiB,
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
                              partitions=self.config.partitions_max_scaled,
                              replicas=3,
                              config=topic_config)

    def load_many_segments(self):
        target_cloud_segments = self.num_segments_per_partition * self.config.partitions_max_scaled
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
        retention_bytes = int(self.config.ingress_rate_scaled * 6 * hours /
                              self.config.partitions_max_scaled)
        self.rpk.alter_topic_config(self.topic_name, 'segment.bytes',
                                    self.config.segment_size_scaled)
        self.rpk.alter_topic_config(self.topic_name,
                                    'retention.local.target.bytes',
                                    retention_bytes)

    def get_node(self, idx: int):
        node = self.redpanda.nodes[idx]
        node_id = self.redpanda.node_id(node)
        node_str = f"{node.account.hostname} (node_id: {node_id})"
        return node, node_id, node_str

    @ok_to_fail
    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_combo_preloaded(self):
        """
        Preloads cluster with large number of messages/segments that are also
        replicated to the cloud, and then run various different test stages
        on the cluster:
        - rolling restart
        - stop and start single node, various scenarios
        - isolate and restore a node
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
                msg_size=self.msg_size,
                msg_count=5_000_000_000_000,
                rate_limit_bps=self.config.ingress_rate_scaled,
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
        re.compile("cloud_storage - .* - Exceeded cache size limit!"),
    ]

    # Stages for the "test_restarts"

    def stage_rolling_restart(self):
        self.logger.info(f"Rolling restarting nodes")
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            start_timeout=600,
                                            stop_timeout=600)

    def stage_block_node_traffic(self):
        node, node_id, node_str = self.get_node(0)
        self.logger.info(f"Isolating node {node_str}")
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

    @ok_to_fail
    @cluster(num_nodes=5, log_allow_list=NOS3_LOG_ALLOW_LIST)
    def test_disrupt_cloud_storage(self):
        """
        Make segments replicate to the cloud, then disrupt S3 connectivity
        and restore it
        """
        segment_size = int(self.config.segment_size_scaled / 8)
        self.setup_cluster(segment_bytes=segment_size,
                           retention_local_bytes=2 * segment_size)

        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic_name,
                msg_size=self.msg_size,
                msg_count=5_000_000_000_000,
                rate_limit_bps=self.config.ingress_rate_scaled,
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
        self.logger.info(
            f"Getting the first {self.config.partitions_max_scaled} segments into the cloud"
        )
        wait_until(lambda: nodes_report_cloud_segments(
            self.redpanda, self.config.partitions_max_scaled),
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

    @ignore
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
                msg_size=self.msg_size,
                msg_count=5_000_000_000_000,
                rate_limit_bps=self.config.ingress_rate_scaled,
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
            f"Node added, new node_id: {new_node_id}, waiting for {int(nt_partitions_before/2)} partitions to move there in {int(decomm_time*2)} s"
        )
        wait_until(
            lambda: topic_partitions_on_node() > nt_partitions_before / 2,
            timeout_sec=max(60, decomm_time * 2),
            backoff_sec=2,
            err_msg=
            f"{int(nt_partitions_before/2)} partitions failed to move to node {new_node_id} in {max(60, decomm_time*2)} s"
        )
        self.logger.info(f"{topic_partitions_on_node()} partitions moved")

    @cluster(num_nodes=5, log_allow_list=NOS3_LOG_ALLOW_LIST)
    def test_cloud_cache_thrash(self):
        """
        Try to exhaust cloud cache by reading at random offsets with many
        consumers
        """
        segment_size = int(self.config.segment_size_scaled / 8)
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
                msg_size=self.msg_size,
                msg_count=5_000_000_000_000,
                rate_limit_bps=self.config.ingress_rate_scaled,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            wait_until(lambda: producer.produce_status.acked > 10000,
                       timeout_sec=60,
                       backoff_sec=1.0)
            wait_until(lambda: nodes_report_cloud_segments(
                self.redpanda, self.config.partitions_max_scaled),
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
            msg_size=self.msg_size,
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

    @ignore
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
                msg_count=5_000_000_000_000,
                rate_limit_bps=self.config.ingress_rate_scaled,
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

    # The test is ignored because it can hardly achieve 1 GiB/s
    # in the regular CDT environment, the test is designed for 3*is4gen.4xlarge
    @ignore
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

    # The testcase occasionally fails on various parts:
    # - toing on `_consume_from_offset(self.topic_name, 1, p_id, "newest", 30)`
    # - failing to ensure all manifests are in the cloud in `stop_and_scrub_object_storage`
    @ignore
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
                rate_limit_bps=self.config.ingress_rate_scaled,
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
        produce_rate_per_node_bytes_s = self.config.ingress_rate_scaled / self.config.num_brokers_scaled
        batch_cache_max_memory = self.config.memory_per_broker
        time_till_memory_full_per_node = batch_cache_max_memory / produce_rate_per_node_bytes_s
        required_wait_time_s = 1.5 * time_till_memory_full_per_node

        self.logger.info(
            f"Expecting to wait {required_wait_time_s} seconds. "
            f"time_till_memory_full_per_node: {time_till_memory_full_per_node}, "
            f"produce_rate_per_node_bytes_s: {produce_rate_per_node_bytes_s}")

        current_sent = producer.produce_status.sent
        expected_sent = math.ceil(
            (self.config.num_brokers_scaled * batch_cache_max_memory) /
            self.msg_size)

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
            self._consume_from_offset(self.topic_name, 1, p_id, "newest", 30)

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
        partitions_per_topic = self.config.partitions_max_scaled
        workload = {
            "name": "StabilityTest",
            "topics": topic_count,
            "partitions_per_topic": partitions_per_topic,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 2,
            "producers_per_topic": 2,
            "producer_rate": int(produce_bps / (4 * KiB)),
            "message_size": 4 * KiB,
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

        segment_size = 128 * MiB  # 128 MiB
        consume_rate = 1 * GiB  # 1 GiB/s

        # create a new topic with low local retention.
        config = {
            'segment.bytes': segment_size,
            'retention.bytes': -1,
            'retention.local.target.bytes': 2 * segment_size,
            'cleanup.policy': 'delete',
            'partition_autobalancing_node_availability_timeout_sec':
            self.unavailable_timeout,
            'partition_autobalancing_mode': 'continuous',
            'raft_learner_recovery_rate': 10 * GiB,
        }
        self.rpk.create_topic(self.topic_name,
                              partitions=self.config.partitions_max_scaled,
                              replicas=3,
                              config=config)

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=self.msg_size,
            msg_count=5_000_000_000_000,
            rate_limit_bps=self.config.ingress_rate_scaled)
        producer.start()

        # produce 10 mins worth of consume data onto S3.
        produce_time_s = 4 * 60
        messages_to_produce = (produce_time_s * consume_rate) / self.msg_size
        time_to_wait = (messages_to_produce *
                        self.msg_size) / self.config.ingress_rate_scaled

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
        benchmark = self._run_omb(self.config.ingress_rate_scaled / 2,
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
        wait_until(self.redpanda.healthy, timeout_sec=900, backoff_sec=1)

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

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_ht004_minpartomb(self):
        validator_overrides = {
            OMBSampleConfigurations.E2E_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(51)],
            OMBSampleConfigurations.E2E_LATENCY_AVG:
            [OMBSampleConfigurations.lte(145)],
        }
        partitions_per_topic = self.config.partitions_max_scaled
        workload = {
            "name": "HT004-MINPARTOMB",
            "topics": 1,
            "partitions_per_topic": partitions_per_topic,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 3,
            "producers_per_topic": 1,
            "producer_rate": int(self.config.ingress_rate_scaled / 8),
            "message_size": 8 * KiB,
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 1,
            "warmup_duration_minutes": 1,
            "use_randomized_payloads": True,
            "random_bytes_ratio": 0.5,
            "randomized_payload_pool_size": 100,
        }

        benchmark = OpenMessagingBenchmark(
            self._ctx, self.redpanda, "SIMPLE_DRIVER",
            (workload, OMBSampleConfigurations.UNIT_TEST_LATENCY_VALIDATOR
             | validator_overrides))

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 1
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
