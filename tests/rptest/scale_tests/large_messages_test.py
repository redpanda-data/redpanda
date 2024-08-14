# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import threading
import time
import concurrent.futures

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, LoggingConfig, \
    MetricsEndpoint, PandaproxyConfig, SchemaRegistryConfig
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.consumer_swarm import ConsumerSwarm
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.scale_parameters import ScaleParameters

HTTP_GET_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

HTTP_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}


class LargeMessagesTest(RedpandaTest):
    # Max time to wait for the cluster to be healthy once more.
    HEALTHY_WAIT_SECONDS = 20 * 60

    # Up to 5 min to stop the node with a lot of topics
    STOP_TIMEOUT = 60 * 5

    # Progress wait timeout
    PROGRESS_TIMEOUT = 60 * 3

    LEADER_BALANCER_PERIOD_MS = 30000

    def __init__(self, *args, **kwargs):
        # Specific configs
        # With big messages, there will be big partitions
        self.mib_per_partition = 4
        # TODO: Check if we can increase later
        self.topic_partitions_per_shard = 1000
        # Turn off S3
        self.tiered_storage_enabled = False
        # messages
        self.message_count = 100
        # 1MB
        self.message_size = 2**20
        # Topics
        self.n_topics = 20
        self.n_partitions = 2
        self.replication_factor = 3
        self.topic_prefix = "large-messages"
        self.topic_names = [
            f"{self.topic_prefix}-{i}" for i in range(self.n_topics)
        ]

        # Prepare RP
        super().__init__(
            *args,
            num_brokers=3,
            extra_rp_conf={
                # Disable leader balancer initially, to enable us to check for
                # stable leadership during initial elections and post-restart
                # elections.  We will switch it on later, to exercise it during
                # the traffic stress test.
                # 'enable_leader_balancer': False,

                # Avoid waiting for 5 minutes for leader balancer to activate
                "leader_balancer_idle_timeout": self.LEADER_BALANCER_PERIOD_MS,
                "leader_balancer_mute_timeout": self.LEADER_BALANCER_PERIOD_MS,

                # Tweak storage related params
                'reclaim_batch_cache_min_free': 256000000,
                'storage_read_buffer_size': 32768,
                'storage_read_readahead_count': 2,
                'disable_metrics': True,
                'disable_public_metrics': False,
                'append_chunk_size': 32768,
                'kafka_rpc_server_tcp_recv_buf': 131072,
                'kafka_rpc_server_tcp_send_buf': 131072,
                'kafka_rpc_server_stream_recv_buf': 32768,

                # Tuning
                'kafka_batch_max_bytes': 10485760 * 2,

                # Enable all the rate limiting things we would have in
                # production, to ensure their effect is accounted for,
                # but with high enough limits that we do
                # not expect to hit them.
                'kafka_connection_rate_limit': 10000,
                'kafka_connections_max': 50000,

                # In testing tiered storage, we care about creating as many
                # cloud segments as possible. To that end, bounding the segment
                # size isn't productive.
                'cloud_storage_segment_size_min': 1,
                'log_segment_size_min': 1024,

                # Disable segment merging: when we create many small segments
                # to pad out tiered storage metadata, we don't want them to
                # get merged together.
                'cloud_storage_enable_segment_merging': False,

                # We don't scrub tiered storage in this test because it is slow
                # (on purpose) and takes unreasonable amount of time for a CI
                # job. We should figure out how to make it faster for this
                # use-case.
                'cloud_storage_enable_scrubbing': False,
            },
            # Reduce per-partition log spam
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'storage': 'warn',
                                         'storage-gc': 'warn',
                                         'raft': 'warn',
                                         'offset_translator': 'warn'
                                     }),
            pandaproxy_config=PandaproxyConfig(),
            schema_registry_config=SchemaRegistryConfig(),
            **kwargs)

        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.thread_local = threading.Lock()

        # Ongoing test vars
        self.scale = self._init_scale_config()

    def setUp(self):
        # defer redpanda startup to the test, it might want to tweak
        # ResourceSettings based on its parameters.
        pass

    def _init_scale_config(self):
        # Scale tests are not run on debug builds
        assert not self.debug_mode

        scale = ScaleParameters(
            self.redpanda,
            self.replication_factor,
            self.mib_per_partition,
            self.topic_partitions_per_shard,
            tiered_storage_enabled=self.tiered_storage_enabled)

        n_topics = 1

        # Partitions per topic
        n_partitions = int(scale.partition_limit / n_topics)

        self.logger.info(
            f"Running large messages ({self.message_size}) scale test "
            f"with {n_partitions} partitions on {n_topics} topics")
        if scale.si_settings:
            self.redpanda.set_si_settings(scale.si_settings)

        # Enable large node-wide throughput limits to verify they work at scale
        # To avoid affecting the result of the test with the limit, set them
        # somewhat above expect_bandwidth value per node.
        #
        # We skip setting them on tiered storage where `expect_bandwidth` is
        # calculated for the worst case scenario and we don't want to limit the
        # best case one.
        if not scale.tiered_storage_enabled:
            self.redpanda.add_extra_rp_conf({
                'kafka_throughput_limit_node_in_bps':
                int(scale.expect_bandwidth / len(self.redpanda.nodes) * 3),
                'kafka_throughput_limit_node_out_bps':
                int(scale.expect_bandwidth / len(self.redpanda.nodes) * 3)
            })

        self.redpanda.add_extra_rp_conf({
            'topic_partitions_per_shard':
            self.topic_partitions_per_shard,
            'topic_memory_per_partition':
            self.mib_per_partition * 1024 * 1024,
        })

        return scale

    def _create_topics(self):
        self.logger.info("Entering topic creation")
        for tn in self.topic_names:
            self.logger.info(
                f"Creating topic {tn} with {self.n_partitions} partitions")
            config = {
                'segment.bytes': self.scale.segment_size,
                'retention.bytes': self.scale.retention_bytes,
                'cleanup.policy': 'delete',
                'num.replica.fetchers': 16,
                'replica.fetch.max.bytes': 10485760 * 2,
                'num.network.threads': 16,
                'num.io.threads': 16,
            }
            if self.scale.local_retention_bytes:
                config['retention.local.target.bytes'] = \
                        self.scale.local_retention_bytes

            self.rpk.create_topic(tn,
                                  partitions=self.n_partitions,
                                  replicas=self.replication_factor,
                                  config=config)

    def _wait_until_cluster_healthy(self, include_underreplicated=True):
        """
        Waits until the cluster is reporting no under-replicated
        or leaderless partitions.
        """
        def is_healthy():
            unavailable_count = self.redpanda.metric_sum(
                'redpanda_cluster_unavailable_partitions',
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
                nodes=self.redpanda.started_nodes())
            under_replicated_count = self.redpanda.metric_sum(
                'vectorized_cluster_partition_under_replicated_replicas',
                nodes=self.redpanda.started_nodes())
            self.logger.info(
                f"under-replicated partitions count: {under_replicated_count} "
                f"unavailable_count: {unavailable_count}")
            return unavailable_count == 0 and \
                (under_replicated_count == 0 or not include_underreplicated)

        wait_until(
            lambda: is_healthy(),
            timeout_sec=self.HEALTHY_WAIT_SECONDS,
            backoff_sec=30,
            err_msg=f"couldn't reach under-replicated count target: {0}")

    def _run_producers_with_constant_rate(self, topic_count, topic_prefixes,
                                          message_rps):
        swarm_node_producers = []
        for topic in topic_prefixes:
            swarm_producer = ProducerSwarm(
                self.test_context,
                self.redpanda,
                topic,
                topic_count,
                self.message_count,
                unique_topics=True,
                messages_per_second_per_producer=message_rps,
                min_record_size=self.message_size,
                max_record_size=self.message_size)
            swarm_node_producers.append(swarm_producer)

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_producers:
            self.logger.info(f"Starting swarm client on node {swarm_client}")
            swarm_client.start()

        return swarm_node_producers

    def _run_consumers_with_constant_rate(self, topic_count, topic_prefixes,
                                          group):
        swarm_node_consumers = []
        node_message_count = int(0.95 * (self.message_count * topic_count))
        for topic in topic_prefixes:
            swarm_producer = ConsumerSwarm(self.test_context,
                                           self.redpanda,
                                           topic,
                                           group,
                                           topic_count,
                                           node_message_count,
                                           unique_topics=True,
                                           unique_groups=True)
            swarm_node_consumers.append(swarm_producer)

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_consumers:
            self.logger.info(f"Starting swarm client on node {swarm_client}")
            swarm_client.start()

        return swarm_node_consumers

    def _wait_workload_progress(self, swarm_nodes):
        def _check():
            metrics = []
            for node in swarm_nodes:
                metrics.append(node.get_metrics_summary(seconds=20).p50)
            total_rate = sum(metrics)
            _m = [str(m) for m in metrics]
            self.logger.debug(f"...last 20 sec rate is {total_rate} "
                              f"({', '.join(_m)})")
            return total_rate >= target_rate

        # Value for progress checks is 20 sec
        # Since we expect slowdowns with big messages,
        # expect at least one message per 20 sec
        target_rate = 1
        self.redpanda.wait_until(
            _check,
            timeout_sec=self.PROGRESS_TIMEOUT,
            backoff_sec=5,
            err_msg="Producer Swarm nodes not making progress")

    def _get_rw_metrics(self):
        # label options: kafka, internal
        def _get_samples(name, label='kafka'):
            metrics = self.redpanda.metrics_sample(
                name, metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
            if metrics is not None:
                samples = [
                    s.value for s in metrics.samples
                    if s.labels['redpanda_server'] == label
                ]
            else:
                samples = []
            total = sum(samples)
            return samples, total

        read_metric_name = "redpanda_rpc_received_bytes"
        sent_metric_name = "redpanda_rpc_sent_bytes"

        read_samples, read_bytes = _get_samples(read_metric_name)
        sent_samples, sent_bytes = _get_samples(sent_metric_name)
        sent_bytes = self.redpanda.metric_sum(
            metric_name=sent_metric_name,
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
            namespace='kafka')

        return read_bytes, sent_bytes

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_large_messages_throughput(self):
        """Test creates 9 topics, and uses client-swarm to
        generate 100 messages with parametrized rate of msg/sec rate
        to each topic and validates high watermark values

        Returns:
            None
        """

        messages_per_second_per_producer = 1

        # Start kafka
        self.redpanda.start()

        # Do create topics stage
        self._create_topics()

        # Do the healthcheck on RP
        # to make sure that all topics are settle down and have their leader
        self._wait_until_cluster_healthy()

        # Calculate some values
        total_bytes = self.n_topics * self.message_count * self.message_size
        total_mb = total_bytes / 1024 / 1024
        if messages_per_second_per_producer:
            running_time_sec = \
                self.message_count // messages_per_second_per_producer
            expected_min_throughput = total_bytes / \
                (self.message_count // messages_per_second_per_producer)
        else:
            running_time_sec = self.message_count * self.message_size // 2**10
            # By default, we expect not less than 20MB per sec
            expected_min_throughput = 20 * (2**10)

        expected_throughput_mb = expected_min_throughput / 1024 / 1024
        self.logger.info(f"Total data: {total_mb:.2f}MB ({total_bytes}b), "
                         f"Expected throughput: {expected_throughput_mb}MB/s")

        # Run swarm producers
        swarm_producers = self._run_producers_with_constant_rate(
            self.n_topics, [self.topic_prefix],
            messages_per_second_per_producer)

        # # Run swarm consumers
        # _group = "large_messages_group"
        # swarm_consumers = self._run_consumers_with_constant_rate(
        #     self.n_topics, [self.topic_prefix], _group)

        # Wait for all messages to be produced
        self.logger.info(f"Sleeping for {running_time_sec} sec (running time)")

        # Measure bandwidth each 2 seconds
        # if no new bytes received by RP, check swarm and exit
        # if at least one finished
        interval = 2
        seconds_spent = 0
        last_read = 0
        while seconds_spent < running_time_sec:
            read, sent = self._get_rw_metrics()
            bytes_per_sec = (read - last_read) / 2
            mb_per_sec = bytes_per_sec / 1024 / 1024
            self.logger.debug(f"Bytes read: {read} ({mb_per_sec:.2f}MB/sec), "
                              f"Bytes sent: {sent}")

            # If no new bytes received, check swarm nodes
            if last_read == read:
                if any([not s.is_alive() for s in swarm_producers]):
                    # At least one is done, exit
                    break

            last_read = read
            time.sleep(interval)
            seconds_spent += interval

        # Run checks if swarm nodes finished
        self.logger.info("Make sure that swarm node producers are finished")
        for s in swarm_producers:
            # account for up to one-third delays
            s.wait(running_time_sec * 2)
        self.logger.info("Make sure that swarm node consumers are finished")
        # for s in swarm_consumers:
        #     # account for up to one-third delays
        #     s.wait(running_time_sec * 2)

        self.logger.info("Calculating high watermarks for all topics")

        # Topic hwm getter
        def _get_hwm(topic):
            _hwm = 0
            for partition in self.rpk.describe_topic(topic):
                # Add currect high watermark for topic
                _hwm += partition.high_watermark
            return _hwm

        hwms = []
        # Use Thread pool to speed things up
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as exec:
            swarmnode_hwms = sum(exec.map(_get_hwm, self.topic_names))
        # save watermark for node
        hwms.append(swarmnode_hwms)

        assert all([hwm >= self.message_count for hwm in hwms]), \
            f"Message counts per swarm node mismatch: " \
            f"target={self.message_count}, " \
            f"swarm_nodes='''{', '.join([str(num) for num in hwms])}'''"

        return
