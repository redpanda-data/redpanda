# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import itertools
import math
import re
import time
import json

from ducktape.mark import ignore, ok_to_fail
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from contextlib import contextmanager
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer, KgoVerifierProducer,
    KgoVerifierRandomConsumer)
from rptest.services.metrics_check import MetricCheck
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda_cloud import AdvertisedTierConfigs, CloudTierName
from rptest.services.redpanda import (RESTART_LOG_ALLOW_LIST, MetricsEndpoint,
                                      SISettings, RedpandaServiceCloud)
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import firewall_blocked
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.utils.si_utils import nodes_report_cloud_segments

KiB = 1024
MiB = KiB * KiB
GiB = KiB * MiB
minutes = 60
hours = 60 * minutes


class HighThroughputTestTrafficGenerator:
    """
    Generic traffic generator that skews the consumer load to be a multiple
    of the produce load. This is because the predefined cloud v2 tiers have
    higher egress maximum allowed load then ingress, by a factor of at most
    3x in some tiers.
    """
    def __init__(self, context, redpanda, topic, msg_size, asymetry=3):
        t = time.time()
        self._logger = redpanda.logger
        self._producer_start_time = t
        self._producer_end_time = t
        self._consumer_start_time = t
        self._consumer_end_time = t
        self._msg_size = msg_size
        self._msg_count = 5_000_000_000_000
        self._producer = KgoVerifierProducer(context,
                                             redpanda,
                                             topic,
                                             msg_size=self._msg_size,
                                             msg_count=self._msg_count)

        # Level of asymetry is equal to the number of parallel consumers
        self._consumer = KgoVerifierConsumerGroupConsumer(
            context,
            redpanda,
            topic,
            self._msg_size,
            asymetry,
            loop=True,
            max_msgs=self._msg_count,
            debug_logs=True,
            trace_logs=True)

    def wait_for_traffic(self, acked=1, timeout_sec=30):
        wait_until(lambda: self._producer.produce_status.acked >= acked,
                   timeout_sec=timeout_sec,
                   backoff_sec=1.0)

    def start(self):
        self._logger.info("Starting producer")
        self._producer.start()
        self._producer_start_time = time.time()
        self.wait_for_traffic(acked=1, timeout_sec=10)
        # Give the producer a head start
        time.sleep(3)  # TODO: Edit maybe make this configurable?
        self._logger.info("Starting consumer")
        self._consumer.start()
        self._consumer_start_time = time.time()
        wait_until(
            lambda: self._consumer.consumer_status.validator.total_reads >= 1,
            timeout_sec=30)

    def stop(self):
        self._logger.info("Stopping all traffic generation")
        self._producer.stop()
        self._consumer.stop()
        self._producer.wait()
        self._logger.info("Producer stopped")
        self._producer_stop_time = time.time()
        self._consumer.wait()
        self._logger.info("Consumer stopped")
        self._consumer_stop_time = time.time()

    def throughput(self):
        producer_total_time = self._producer_stop_time - self._producer_start_time
        consumer_total_time = self._consumer_stop_time - self._consumer_start_time
        if producer_total_time == 0 or consumer_total_time == 0:
            return {}
        producer_status = self._producer.produce_status
        consumer_status = self._consumer.consumer_status
        producer_bytes_written = self._msg_size * producer_status.acked
        consumer_bytes_read = self._msg_size * consumer_status.validator.total_reads
        return {
            'producer': {
                'errors': producer_status.bad_offsets,
                'total_successful_requests': producer_status.acked,
                'bytes_written': producer_bytes_written,
                'ingress_throughput':
                producer_bytes_written / producer_total_time
            },
            'consumer': {
                'errors': consumer_status.errors,
                'total_successful_requests':
                consumer_status.validator.total_reads,
                'bytes_read': consumer_bytes_read,
                'egress_throughput': consumer_bytes_read / consumer_total_time
            }
        }


@contextmanager
def traffic_generator(context, redpanda, tier_cfg, *args, **kwargs):
    tgen = HighThroughputTestTrafficGenerator(context, redpanda, *args,
                                              **kwargs)
    tgen.start()
    try:
        yield tgen
    except:
        redpanda.logger.exception("Exception within traffic_generator method")
        raise
    finally:
        tgen.stop()
        throughput = tgen.throughput()
        redpanda.logger.info(
            f'HighThroughputTrafficGenerator reported throughput: {json.dumps(throughput, sort_keys=True, indent=4)}'
        )
        producer_throughput = throughput['producer']['ingress_throughput']
        consumer_throughput = throughput['consumer']['egress_throughput']
        assert (
            producer_throughput / 0.1
        ) >= tier_cfg.ingress_rate, f"Observed producer throughput {producer_throughput} too low, expected: {tier_cfg.ingress_rate}"
        assert (
            consumer_throughput / 0.1
        ) >= tier_cfg.egress_rate, f"Observed consumer throughput {consumer_throughput} too low, expected: {tier_cfg.egress_rate}"


class HighThroughputTest(RedpandaTest):
    small_segment_size = 4 * KiB
    unavailable_timeout = 60
    msg_size = 128 * KiB

    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        self._ctx = test_ctx

        # Default set to tier-1-aws is a temporary work around for
        # https://github.com/redpanda-data/cloudv2/issues/7903
        cloud_tier_str = test_ctx.globals.get("cloud_tier", "tier-1-aws")
        cloud_tier = CloudTierName(cloud_tier_str)
        extra_rp_conf = None
        num_brokers = None

        if cloud_tier == CloudTierName.DOCKER:
            # TODO: Bake the docker config into a higher layer that will
            # automatically load these settings upon call to make_rp_service
            config = AdvertisedTierConfigs[CloudTierName.DOCKER]
            num_brokers = config.num_brokers
            extra_rp_conf = {
                'log_segment_size': config.segment_size,
                'cloud_storage_cache_size': config.cloud_cache_size,
                'kafka_connections_max': config.connections_limit,
            }

        super(HighThroughputTest,
              self).__init__(test_ctx,
                             *args,
                             num_brokers=num_brokers,
                             extra_rp_conf=extra_rp_conf,
                             cloud_tier=cloud_tier,
                             disable_cloud_storage_diagnostics=True,
                             **kwargs)

        self.tier_config = self.redpanda.advertised_tier_config
        if cloud_tier == CloudTierName.DOCKER:
            si_settings = SISettings(
                test_ctx,
                log_segment_size=self.small_segment_size,
                cloud_storage_cache_size=self.tier_config.cloud_cache_size,
            )
            self.redpanda.set_si_settings(si_settings)
            self.s3_port = si_settings.cloud_storage_api_endpoint_port

        test_ctx.logger.info(f"Cloud tier {cloud_tier}: {self.tier_config}")

        self.rpk = RpkTool(self.redpanda)
        self.topics = [
            TopicSpec(partition_count=self.tier_config.partitions_upper_limit,
                      replication_factor=3,
                      retention_bytes=-1)
        ]

    def tearDown(self):
        # These tests may run on cloud ec2 instances where between each test
        # the same cluster is used. Therefore state between runs will still exist,
        # remove the topic after each test run to clean out old state.
        if RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG in self._ctx.globals:
            self.rpk.delete_topic(self.topic)

    def load_many_segments(self):
        """
        This methods intended use is to pre-load the cluster (and S3) with
        small segments to bootstrap a test enviornment that would stress
        the tiered storage subsytem.
        """
        cloud_segment_size = self.msg_size * 4
        num_segments_per_partition = 1000
        target_cloud_segments = num_segments_per_partition * self.tier_config.partitions_upper_limit
        total_bytes_to_produce = target_cloud_segments * cloud_segment_size
        total_messages = int((total_bytes_to_produce / self.msg_size) * 1.2)
        self.redpanda.logger.info(
            f"Total bytes: {total_bytes_to_produce} total messages: {total_messages}"
        )
        assert cloud_segment_size >= self.msg_size
        producer = KgoVerifierProducer(self.test_context, self.redpanda,
                                       self.topic, self.msg_size,
                                       total_messages)

        self.adjust_topic_segment_properties(cloud_segment_size,
                                             cloud_segment_size * 2)

        # Wait for at least as long as it would take to produce the data to the cluster
        # at the given tiers expected ingress rate. Add in a little more time to account
        # for any network hiccups or transitent errors
        estimated_produce_time_secs = int(
            (total_bytes_to_produce / self.tier_config.ingress_rate) * 1.2)
        try:
            producer.start()
            wait_until(lambda: nodes_report_cloud_segments(
                self.redpanda, target_cloud_segments),
                       timeout_sec=estimated_produce_time_secs,
                       backoff_sec=5)
        finally:
            producer.stop()
            producer.wait(timeout_sec=600)

        # Once some segments are generated, configure the topic to use more
        # realistic sizes.
        retention_bytes = int(self.tier_config.ingress_rate * 6 * hours /
                              self.tier_config.partitions_upper_limit)
        self.adjust_topic_segment_properties(self.tier_config.segment_size,
                                             retention_bytes)

    def adjust_topic_segment_properties(self, segment_bytes: int,
                                        retention_local_bytes: int):
        self.rpk.alter_topic_config(self.topic,
                                    TopicSpec.PROPERTY_SEGMENT_SIZE,
                                    segment_bytes)
        self.rpk.alter_topic_config(
            self.topic, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
            retention_local_bytes)

    def get_node(self):
        idx = random.randrange(len(self.cluster.nodes))
        return self.cluster.nodes[idx]

    @cluster(num_nodes=2)
    def test_throughput_simple(self):
        with traffic_generator(self.test_context, self.redpanda,
                               self.tier_config, self.topic,
                               self.msg_size) as _:
            # Test will assert if advertised throughput isn't met
            time.sleep(15)

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
        PRODUCER_TIMEOUT_MS = 5000

        tier_config = self.redpanda.advertised_tier_config

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
                                        self.topic,
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
        for partition in self.rpk.describe_topic(self.topic):
            # Add currect high watermark for topic
            _hwm += partition.high_watermark

        # Check that all messages make it through
        # Since there is +10% on connections set
        # Message count should be > expected * 0.9
        reasonably_expected = int(expected_msg_count * 0.9)
        self.logger.warn(f"Expected more than {reasonably_expected} messages "
                         f"out of {expected_msg_count}, actual {_hwm}")
        assert _hwm >= expected_msg_count

        # Assert that target connection count is reached
        self.logger.warn(
            f"Reached {connectMax} of {tier_config.connections_limit} needed")
        assert connectMax >= tier_config.connections_limit

        return

    COMBO_PRELOADED_LOG_ALLOW_LIST = [
        re.compile('storage - .* - Stopping parser, short read. .*')
    ] + RESTART_LOG_ALLOW_LIST

    @cluster(num_nodes=2, log_allow_list=COMBO_PRELOADED_LOG_ALLOW_LIST)
    def test_ht003_kgofailure(self):
        """
        Generates the maximum possible load to the cluster onto a topic that
        is replicated to the cloud, and then run various different test
        stages on the cluster:
        - rolling restart
        - stop and start single node, various scenarios
        - isolate and restore a node
        """
        # Generate a realistic number of segments per partition.
        with traffic_generator(self.test_context, self.redpanda,
                               self.tier_config, self.topic,
                               self.msg_size) as tgen:
            # Wait until theres some traffic
            tgen.wait_for_traffic(acked=10000, timeout_sec=60)

            # Run a rolling restart.
            self.stage_rolling_restart()

            # Hard stop, then restart.
            self.stage_stop_wait_start(forced_stop=True, downtime=0)

            # Stop a node, wait for enough time for movement to occur, then
            # restart.
            self.stage_stop_wait_start(forced_stop=False,
                                       downtime=self.unavailable_timeout)

            # Block traffic to/from one node.
            self.stage_block_node_traffic()

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
        wait_time = 120
        node = self.get_node()
        self.logger.info(f"Isolating node {node.name}")
        with FailureInjector(self.redpanda) as fi:
            fi.inject_failure(FailureSpec(FailureSpec.FAILURE_ISOLATE, node))
            self.logger.info(
                f"Running for {wait_time}s while failure injected")
            time.sleep(wait_time)

        self.logger.info(
            f"Running for {wait_time}s after injected failure removed")
        time.sleep(wait_time)
        self.logger.info(
            f"Waiting for the cluster to return to a healthy state")
        wait_until(self.redpanda.healthy, timeout_sec=600, backoff_sec=1)

    def stage_stop_wait_start(self, forced_stop: bool, downtime: int):
        node = self.get_node()
        self.logger.info(
            f"Stopping node {node.name} {'ungracefully' if forced_stop else 'gracefully'}"
        )
        self.redpanda.stop_node(node,
                                forced=forced_stop,
                                timeout=60 if forced_stop else 180)

        self.logger.info(f"Node downtime {downtime} s")
        time.sleep(downtime)

        restart_timeout = 300 + int(900 * downtime / 60)
        self.logger.info(
            f"Restarting node {node.name} for {restart_timeout} s")
        self.redpanda.start_node(node, timeout=600)
        wait_until(self.redpanda.healthy,
                   timeout_sec=restart_timeout,
                   backoff_sec=1)

    # Temporary ignore until TS metrics can be queried via public_metrics
    @ignore  # https://github.com/redpanda-data/cloudv2/issues/8845
    @cluster(num_nodes=2, log_allow_list=NOS3_LOG_ALLOW_LIST)
    def test_disrupt_cloud_storage(self):
        """
        Make segments replicate to the cloud, then disrupt S3 connectivity
        and restore it
        """
        segment_size = int(self.tier_config.segment_size / 8)
        self.adjust_topic_segment_properties(segment_bytes=segment_size,
                                             retention_local_bytes=2 *
                                             segment_size)
        with traffic_generator(self.test_context, self.redpanda,
                               self.tier_config, self.topic,
                               self.msg_size) as _:
            # S3 up -> down -> up
            self.stage_block_s3()

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
            f"Getting the first {self.tier_config.partitions_upper_limit} segments into the cloud"
        )
        wait_until(lambda: nodes_report_cloud_segments(
            self.redpanda, self.tier_config.partitions_upper_limit),
                   timeout_sec=600,
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
        self.adjust_topic_segment_properties(
            segment_bytes=self.small_segment_size,
            retention_local_bytes=2 * self.small_segment_size)
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic,
                msg_size=self.msg_size,
                msg_count=5_000_000_000_000,
                rate_limit_bps=self.tier_config.ingress_rate,
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
        node, node_id, node_str = self.get_node()

        def topic_partitions_on_node():
            try:
                parts = self.redpanda.partitions(self.topic)
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

    # Temporary ignore until TS metrics can be queried via public_metrics
    @ignore  # https://github.com/redpanda-data/cloudv2/issues/8845
    @cluster(num_nodes=3, log_allow_list=NOS3_LOG_ALLOW_LIST)
    def test_cloud_cache_thrash(self):
        """
        Try to exhaust cloud cache by reading at random offsets with many
        consumers
        """
        segment_size = int(self.tier_config.segment_size / 8)
        self.adjust_topic_segment_properties(segment_bytes=segment_size,
                                             retention_local_bytes=2 *
                                             segment_size)

        with traffic_generator(self.test_context, self.redpanda,
                               self.tier_config, self.topic,
                               self.msg_size) as tgen:
            tgen.wait_for_traffic(acked=10000, timeout_sec=60)
            wait_until(lambda: nodes_report_cloud_segments(
                self.redpanda, self.tier_config.partitions_upper_limit),
                       timeout_sec=600,
                       backoff_sec=5)
            tgen._producer.wait_for_offset_map()

            # Exhaust cloud cache with multiple consumers
            # reading at random offsets
            self.logger.info(f"Starting thrashing consumers")
            consumer = KgoVerifierRandomConsumer(self.test_context,
                                                 self.redpanda,
                                                 self.topic,
                                                 msg_size=self.msg_size,
                                                 rand_read_msgs=1,
                                                 parallel=4,
                                                 debug_logs=True)
            try:
                consumer.start(clean=False)
                time.sleep(240)
            finally:
                self.logger.info(f"Stopping thrashing consumers")
                consumer.stop()
                consumer.wait(timeout_sec=600)

    @ignore
    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_consume(self):
        self.adjust_topic_segment_properties(
            segment_bytes=self.small_segment_size,
            retention_local_bytes=2 * self.small_segment_size)
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        producer = None
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic,
                msg_size=self.msg_size,
                msg_count=5_000_000_000_000,
                rate_limit_bps=self.tier_config.ingress_rate,
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
                                   self.topic,
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
    # - toing on `_consume_from_offset(self.topic, 1, p_id, "newest", 30)`
    # - failing to ensure all manifests are in the cloud in `stop_and_scrub_object_storage`
    @ignore
    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_consume_miss_cache(self):
        self.adjust_topic_segment_properties(
            segment_bytes=self.small_segment_size,
            retention_local_bytes=2 * self.small_segment_size)
        # Generate a realistic number of segments per partition.
        self.load_many_segments()
        producer = None
        try:
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic,
                msg_size=self.msg_size,
                msg_count=5 * 1024 * 1024 * 1024 * 1024,
                rate_limit_bps=self.tier_config.ingress_rate,
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
                        for p in self.rpk.describe_topic(self.topic)
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
        produce_rate_per_node_bytes_s = self.tier_config.ingress_rate / self.tier_config.num_brokers
        batch_cache_max_memory = self.tier_config.memory_per_broker
        time_till_memory_full_per_node = batch_cache_max_memory / produce_rate_per_node_bytes_s
        required_wait_time_s = 1.5 * time_till_memory_full_per_node

        self.logger.info(
            f"Expecting to wait {required_wait_time_s} seconds. "
            f"time_till_memory_full_per_node: {time_till_memory_full_per_node}, "
            f"produce_rate_per_node_bytes_s: {produce_rate_per_node_bytes_s}")

        current_sent = producer.produce_status.sent
        expected_sent = math.ceil(
            (self.tier_config.num_brokers * batch_cache_max_memory) /
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
                             for p in self.rpk.describe_topic(self.topic)
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
            self._consume_from_offset(self.topic, 1, p_id, "newest", 30)

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
            self._consume_from_offset(self.topic, messages_to_read, p_id,
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
        partitions_per_topic = self.tier_config.partitions_upper_limit
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
        self.rpk.create_topic(
            self.topic,
            partitions=self.tier_config.partitions_upper_limit,
            replicas=3,
            config=config)

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.msg_size,
            msg_count=5_000_000_000_000,
            rate_limit_bps=self.tier_config.ingress_rate)
        producer.start()

        # produce 10 mins worth of consume data onto S3.
        produce_time_s = 4 * 60
        messages_to_produce = (produce_time_s * consume_rate) / self.msg_size
        time_to_wait = (messages_to_produce *
                        self.msg_size) / self.tier_config.ingress_rate

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
        benchmark = self._run_omb(self.tier_config.ingress_rate / 2,
                                  validator_overrides)

        # This consumer should largely be reading from S3
        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               self.topic,
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
                               self.topic,
                               offset="newest",
                               num_msgs=consume_count)
        consumer.start()
        wait_until(lambda: consumer.message_count >= consume_count,
                   timeout_sec=60,
                   backoff_sec=1,
                   err_msg=f"Could not consume {consume_count} msgs in 1 min")

        consumer.stop()
        consumer.free()
