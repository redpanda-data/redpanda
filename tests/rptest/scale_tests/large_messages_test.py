# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures
from enum import StrEnum
import math
import numpy
import time

from ducktape.mark import ignore, parametrize
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
from ducktape.mark import matrix


class Mode(StrEnum):
    TEN_TOPICS = 'TEN_TOPICS'
    MANY_PARTS = 'MANY_PARTS'


class LargeMessagesTest(RedpandaTest):
    # Max time to wait for the cluster to be healthy once more.
    HEALTHY_WAIT_SECONDS = 20 * 60

    # Up to 5 min to stop the node with a lot of topics
    STOP_TIMEOUT = 60 * 5

    # Progress wait timeout
    PROGRESS_TIMEOUT = 60 * 3

    # Leader balancer timeout time
    LEADER_BALANCER_PERIOD_MS = 30000

    # default max batch is 1 MiB but due to the batch overhead we can't
    # actually accept a message payload of 1 MIB.
    MAX_DEFAULT_MSG_SIZE_MIB = 0.9

    def __init__(self, *args, **kwargs):
        # Topics
        # Prepare RP
        super().__init__(
            *args,
            num_brokers=3,
            extra_rp_conf={
                # Enable some of the rate limiting things we would have in
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

        # not supported for now
        self.tiered_storage_enabled = False

    def setUp(self):
        # defer redpanda startup to the test, it might want to tweak
        # ResourceSettings based on its parameters.
        pass

    def _create_topics(self):
        self.logger.info("Entering topic creation")
        for tn in self.topic_names:
            self.logger.info(
                f"Creating topic {tn} with {self.n_partitions} partitions")
            config = {
                'segment.bytes': self.scale.segment_size,
                'retention.bytes': self.scale.retention_bytes,
                'cleanup.policy': 'delete',
            }

            # Set the batch size up if needed, we don't just do this conditionally
            # in order to check that things work at default configs for the expected
            # range of sizes.
            if self.message_size / 2**20 > self.MAX_DEFAULT_MSG_SIZE_MIB:
                config |= {'max.message.bytes': self.message_size + 1000}

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

        wait_until(lambda: is_healthy(),
                   timeout_sec=self.HEALTHY_WAIT_SECONDS,
                   backoff_sec=30,
                   err_msg="couldn't reach under-replicated count target of 0")

    def _run_unlimited_producers(self):
        swarm_node_producers: list[ProducerSwarm] = []
        for topic in self.topic_prefixes:
            swarm_node_producers.append(
                ProducerSwarm(self.test_context,
                              self.redpanda,
                              topic,
                              self.n_clients,
                              self.message_count,
                              unique_topics=self.unique,
                              messages_per_second_per_producer=0,
                              min_record_size=self.message_size,
                              max_record_size=self.message_size))

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_producers:
            self.logger.info("Starting swarm client (producers) on node "
                             f"{swarm_client}")
            swarm_client.start()

        return swarm_node_producers

    def _run_consumers(self, group):
        swarm_node_consumers: list[ConsumerSwarm] = []
        node_message_count = int(0.95 * (self.message_count * self.n_clients))

        for topic in self.topic_prefixes:
            swarm_node_consumers.append(
                ConsumerSwarm(self.test_context,
                              self.redpanda,
                              topic,
                              group,
                              self.n_clients,
                              node_message_count,
                              unique_topics=self.unique,
                              unique_groups=self.unique))

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_consumers:
            self.logger.info("Starting swarm client (consumers) on node "
                             f"{swarm_client}")
            swarm_client.start()

        return swarm_node_consumers

    def _wait_workload_progress(self, swarm_nodes):
        def _check_at_least_one():
            metrics = []
            for node in swarm_nodes:
                metrics.append(node.get_metrics_summary(seconds=20).p50)
            total_rate = sum(metrics)
            _m = [str(m) for m in metrics]
            self.logger.debug(f"...last 20 sec rate is {total_rate} "
                              f"({', '.join(_m)})")
            return total_rate >= 1

        # Value for progress checks is 20 sec
        # Since we expect slowdowns with big messages,
        # expect at least one message per 20 sec
        self.redpanda.wait_until(
            _check_at_least_one,
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

        _, read_bytes = _get_samples(read_metric_name)
        _, sent_bytes = _get_samples(sent_metric_name)
        return read_bytes, sent_bytes

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(message_size_mib=[8, 16],
            apply_throughput_limits=[False, True],
            mode=[Mode.MANY_PARTS, Mode.TEN_TOPICS])
    # @parametrize(message_size_mib=16, apply_throughput_limits=True)
    def test_large_messages_throughput(self, message_size_mib: float,
                                       apply_throughput_limits: bool,
                                       mode: Mode):
        """Test creates 10 topics, and uses client-swarm to
        generate 100 messages with parametrized size and sends this count
        to each topic and validates high watermark values along with expected
        throughput.

        Returns:
            None
        """

        self.message_size = int(message_size_mib * 2**20)
        self.replication_factor = 3
        self.swarm_nodes = 2

        # Scale tests are not run on debug builds
        assert not self.debug_mode

        # Init scale settings in the RP cluster
        self.scale = ScaleParameters(
            self.redpanda,
            self.replication_factor,
            tiered_storage_enabled=self.tiered_storage_enabled)

        if self.scale.si_settings:
            self.redpanda.set_si_settings(self.scale.si_settings)

        if mode == Mode.TEN_TOPICS:
            self.n_topics = 10
            self.n_partitions = 1
            self.n_clients = self.n_topics
            self.unique = True
        elif mode == Mode.MANY_PARTS:
            self.n_topics = 1
            self.n_partitions = int(
                self.scale.partition_limit * 0.9) // self.swarm_nodes
            self.n_clients = self.scale.node_cpus * self.scale.node_count * 4
            self.unique = False
        else:
            assert False

        self.topic_prefix_template = "large-messages"
        self.topic_prefixes = [
            f"{self.topic_prefix_template}-n{i}"
            for i in range(self.swarm_nodes)
        ]
        if self.unique:
            self.topic_names = [
                f"{t}-{i}" for i in range(self.n_topics)
                for t in self.topic_prefixes
            ]
        else:
            self.topic_names = self.topic_prefixes

        # we size everything to this target runtime, 5 minutes, this will result in very
        # different amounts of total message volume in different environments with different
        # scale parameters.
        target_runtime_sec = 300

        # every increase of 1 in message_count increases total bytes written by this amount
        bytes_per_message_count = self.n_clients * self.message_size * self.swarm_nodes

        self.expected_throughput = self.scale.expect_bandwidth

        # size message_count to hit the target runtime
        self.message_count = math.ceil(
            self.expected_throughput * target_runtime_sec /
            bytes_per_message_count) + 1
        assert self.message_count > 2, f"message count too low: {self.message_count}"

        # Enable large node-wide throughput limits to verify they work at scale
        # To avoid affecting the result of the test with the limit, set them
        # somewhat above the expect_bandwidth value per node.
        if apply_throughput_limits:
            per_broker_throttle = self.expected_throughput // len(
                self.redpanda.nodes) * 3
            self.redpanda.add_extra_rp_conf({
                'kafka_throughput_limit_node_in_bps':
                per_broker_throttle,
                'kafka_throughput_limit_node_out_bps':
                per_broker_throttle
            })

        # Start redpanda
        self.redpanda.start()

        # Do create topics stage
        self._create_topics()

        # Do the healthcheck on RP
        # to make sure that all topics are settle down and have their leader
        self._wait_until_cluster_healthy()

        # Calculate some values
        total_bytes = self.n_clients * self.message_count * self.message_size * self.swarm_nodes

        self.logger.info(
            f"Total data: {total_bytes/1e6:.2f} MB, "
            f"message count: {self.message_count}, "
            f"Expected throughput >= {self.expected_throughput/1e6:5.2f} MB/s, "
            f"running_time_sec: {target_runtime_sec}")

        # # Run swarm consumers
        swarm_consumers = self._run_consumers("large_messages_group")

        # capture the metrics here so we don't miss any production that happens
        # as soon as we start the producers
        last_read, last_sent = self._get_rw_metrics()

        # Run swarm producers
        swarm_producers = self._run_unlimited_producers()

        for consumer in swarm_consumers:
            self.logger.info(f"Waiting for first message: {consumer}")
            still_running = consumer.await_first(
                timeout_sec=120,
                err_msg=
                f"client-swarm did not read any messages after 120s, check swarm logs"
            )

            if not still_running:
                # consumer finished, the others are probably finished too
                self.logger.info(f"Consumer stopped, not waiting for any more")
                break

        # Wait for all messages to be produced
        self.logger.info("Measuring bandwidth")
        # Measure bandwidth each 2 seconds
        # if no new bytes received by RP, check swarm and exit
        # if at least one finished
        bandwidth_in = []
        bandwidth_out = []
        backoff_interval = 5
        total_elapsed = total_bytes_read = total_bytes_sent = 0
        overall_start_sec = interval_start_sec = time.time()
        producers_alive = len(swarm_producers)
        while total_elapsed < (target_runtime_sec *
                               2) and producers_alive or len(bandwidth_in) < 2:
            time.sleep(backoff_interval)

            interval_end_sec = time.time()
            total_elapsed = interval_end_sec - overall_start_sec

            read, sent = self._get_rw_metrics()

            elapsed_sec = interval_end_sec - interval_start_sec
            # Calculate ingress BW
            bytes_per_sec_in = (read - last_read) / elapsed_sec
            # Calculate egress BW
            bytes_per_sec_out = (sent - last_sent) / elapsed_sec

            # overall BW
            total_bytes_read += (read - last_read)
            total_bytes_sent += (sent - last_sent)

            self.logger.info(
                f"Bytes read: {read/1e6:6.2f} MB ({bytes_per_sec_in /1e6:6.2f}/{total_bytes_read/total_elapsed/1e6:6.2f} MB/s interval/overall), "
                f"Bytes sent: {sent/1e6:6.2f} MB ({bytes_per_sec_out/1e6:6.2f}/{total_bytes_sent/total_elapsed/1e6:6.2f} MB/s interval/overall)"
            )

            producers_alive = sum(s.is_alive() for s in swarm_producers)
            self.logger.info(
                f"{producers_alive}/{len(swarm_producers)} producers alive")

            # Save measurements
            bandwidth_in.append(bytes_per_sec_in)
            bandwidth_out.append(bytes_per_sec_out)

            last_read = read
            last_sent = sent

            interval_start_sec = interval_end_sec

        # Run checks if swarm nodes finished
        swarms: list[tuple[list[ProducerSwarm] | list[ConsumerSwarm],
                           str]] = [(swarm_producers, "producers"),
                                    (swarm_consumers, "consumers")]
        for swarm, swarm_name in swarms:
            self.logger.info(f"Stopping swarm {swarm_name}")
            for s in swarm:
                s.stop()

        self.logger.info("Calculating high watermarks for all topics")

        # Once again, do the healthcheck on RP
        # to make sure that all messages got delivered
        self._wait_until_cluster_healthy()

        # Topic hwm getter
        def _get_hwm(topic):
            _hwm = 0
            for partition in self.rpk.describe_topic(topic):
                # Add correct high watermark for topic
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

        # Remove first measurement as it is more of a ramp up
        bandwidth_in = bandwidth_in[1:]
        bandwidth_out = bandwidth_out[1:]
        # Calculate overall throughput percentiles
        bw_in_perc = numpy.percentile(bandwidth_in, [50, 90, 99])
        bw_out_perc = numpy.percentile(bandwidth_out, [50, 90, 99])
        # Prettify for log
        str_in = []
        str_out = []
        for val in bw_in_perc:
            str_in.append(f"{val / 1E6:.02f} MB/sec")
        for val in bw_out_perc:
            str_out.append(f"{val / 1E6:.02f} MB/sec")
        self.logger.info(f"Measured bandwidth (avg, P90, P99):\n"
                         f"RPC in: {', '.join(str_in)}\n"
                         f"RPC out: {', '.join(str_out)}")

        # Check that measured BW is not lower than expected
        recv_tput_avg = total_bytes_read / total_elapsed
        send_tput_avg = total_bytes_sent / total_elapsed

        def check_tput(tput: float, tname: str):
            assert tput > self.expected_throughput, (
                f"Measured {tname} bandwidth is lower than expected: "
                f"{tput} vs {self.expected_throughput}")

        check_tput(recv_tput_avg, "input")
        check_tput(send_tput_avg, "output")

        return
