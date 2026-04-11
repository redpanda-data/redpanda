# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures
import math
import time
from enum import Enum
from typing import Any

from typing_extensions import assert_never

import numpy
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.consumer_swarm import ConsumerSwarm
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda import (
    CLOUD_TOPICS_CONFIG_STR,
    RESTART_LOG_ALLOW_LIST,
    LoggingConfig,
    MetricsEndpoint,
    PandaproxyConfig,
    SchemaRegistryConfig,
    SISettings,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.scale_parameters import ScaleParameters


class Mode(str, Enum):
    TEN_TOPICS = "TEN_TOPICS"
    MANY_PARTS = "MANY_PARTS"


class LargeMessagesTest(RedpandaTest):
    # Max time to wait for the cluster to be healthy once more.
    HEALTHY_WAIT_SECONDS: int = 20 * 60

    # Up to 5 min to stop the node with a lot of topics
    STOP_TIMEOUT: int = 60 * 5

    # Progress wait timeout
    PROGRESS_TIMEOUT: int = 60 * 3

    # Leader balancer timeout time
    LEADER_BALANCER_PERIOD_MS: int = 30000

    # default max batch is 1 MiB but due to the batch overhead we can't
    # actually accept a message payload of 1 MIB.
    MAX_DEFAULT_MSG_SIZE_MIB: float = 0.9

    # The maximum response size a client will tell RP its willing to receive.
    FETCH_MAX_BYTES_MIB: int = 90

    def __init__(self, *args: Any, **kwargs: Any) -> None:
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
                "kafka_connection_rate_limit": 10000,
                "kafka_connections_max": 50000,
                # In testing tiered storage, we care about creating as many
                # cloud segments as possible. To that end, bounding the segment
                # size isn't productive.
                "cloud_storage_segment_size_min": 1,
                "log_segment_size_min": 1024,
                # Disable segment merging: when we create many small segments
                # to pad out tiered storage metadata, we don't want them to
                # get merged together.
                "cloud_storage_enable_segment_merging": False,
                # We don't scrub tiered storage in this test because it is slow
                # (on purpose) and takes unreasonable amount of time for a CI
                # job. We should figure out how to make it faster for this
                # use-case.
                "cloud_storage_enable_scrubbing": False,
                # Raise the broker-imposed fetch max bytes to allow for fetches
                # to return more than just the obligatory read.
                "fetch_max_bytes": self.FETCH_MAX_BYTES_MIB * 2**20,
                # Similar to above, is deprecated in later versions of RP.
                "kafka_max_bytes_per_fetch": self.FETCH_MAX_BYTES_MIB * 2**20,
                # Set this property to something less than the shutdown timeout(30s).
                # This way additional debug information will be in the logs when the
                # shutdown timeout is exceeded.
                "partition_manager_shutdown_watchdog_timeout": 5 * 1000,  # 5s
            },
            # Reduce per-partition log spam
            log_config=LoggingConfig(
                "info",
                logger_levels={
                    "storage": "warn",
                    "storage-gc": "warn",
                    "raft": "warn",
                    "offset_translator": "warn",
                },
            ),
            pandaproxy_config=PandaproxyConfig(),
            schema_registry_config=SchemaRegistryConfig(),
            **kwargs,
        )

        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

        # not supported for now
        self.tiered_storage_enabled = False

    def setUp(self) -> None:
        # defer redpanda startup to the test, it might want to tweak
        # ResourceSettings based on its parameters.
        pass

    def _create_topics(self, cloud_topics: bool = False) -> None:
        self.logger.info("Entering topic creation")
        for tn in self.topic_names:
            self.logger.info(f"Creating topic {tn} with {self.n_partitions} partitions")
            config = {
                "segment.bytes": self.scale.segment_size,
                "retention.bytes": self.scale.retention_bytes,
                "cleanup.policy": "delete",
            }

            # Set the batch size up if needed, we don't just do this conditionally
            # in order to check that things work at default configs for the expected
            # range of sizes.
            if self.message_size / 2**20 > self.MAX_DEFAULT_MSG_SIZE_MIB:
                config |= {"max.message.bytes": self.message_size + 1000}

            if not cloud_topics and self.scale.local_retention_bytes:
                config["retention.local.target.bytes"] = (
                    self.scale.local_retention_bytes
                )

            if cloud_topics:
                config[TopicSpec.PROPERTY_STORAGE_MODE] = TopicSpec.STORAGE_MODE_CLOUD

            self.rpk.create_topic(
                tn,
                partitions=self.n_partitions,
                replicas=self.replication_factor,
                config=config,
            )

    def _wait_until_cluster_healthy(self, include_underreplicated: bool = True) -> None:
        """
        Waits until the cluster is reporting no under-replicated
        or leaderless partitions.
        """

        def is_healthy() -> bool:
            unavailable_count = self.redpanda.metric_sum(
                "redpanda_cluster_unavailable_partitions",
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
                nodes=self.redpanda.started_nodes(),
            )
            under_replicated_count = self.redpanda.metric_sum(
                "vectorized_cluster_partition_under_replicated_replicas",
                nodes=self.redpanda.started_nodes(),
            )
            self.logger.info(
                f"under-replicated partitions count: {under_replicated_count} "
                f"unavailable_count: {unavailable_count}"
            )
            return unavailable_count == 0 and (
                under_replicated_count == 0 or not include_underreplicated
            )

        wait_until(
            lambda: is_healthy(),
            timeout_sec=self.HEALTHY_WAIT_SECONDS,
            backoff_sec=30,
            err_msg="couldn't reach under-replicated count target of 0",
        )

    def _run_unlimited_producers(self) -> list[ProducerSwarm]:
        swarm_node_producers: list[ProducerSwarm] = []
        for topic in self.topic_prefixes:
            swarm_node_producers.append(
                ProducerSwarm(
                    self.test_context,
                    self.redpanda,
                    topic,
                    self.n_clients,
                    self.message_count,
                    unique_topics=self.unique,
                    messages_per_second_per_producer=0,
                    min_record_size=self.message_size,
                    max_record_size=self.message_size,
                )
            )

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_producers:
            self.logger.info(
                f"Starting swarm client (producers) on node {swarm_client}"
            )
            swarm_client.start()

        return swarm_node_producers

    def _run_consumers(self, group: str) -> list[ConsumerSwarm]:
        swarm_node_consumers: list[ConsumerSwarm] = []
        node_message_count = int(0.95 * (self.message_count * self.n_clients))
        max_fetch_bytes = min(self.FETCH_MAX_BYTES_MIB * 2**20, 4 * self.message_size)
        # Set properties to allow for more than just the obligatory read to be returned.
        properties = {
            "fetch.max.bytes": max_fetch_bytes,
            "max.partition.fetch.bytes": min(2 * self.message_size, max_fetch_bytes),
        }

        for topic in self.topic_prefixes:
            swarm_node_consumers.append(
                ConsumerSwarm(
                    self.test_context,
                    self.redpanda,
                    topic,
                    group,
                    self.n_clients,
                    node_message_count,
                    unique_topics=self.unique,
                    unique_groups=self.unique,
                    properties=properties if not self.default_consumer_config else {},
                )
            )

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_consumers:
            self.logger.info(
                f"Starting swarm client (consumers) on node {swarm_client}"
            )
            swarm_client.start()

        return swarm_node_consumers

    def _wait_workload_progress(
        self, swarm_nodes: list[ProducerSwarm] | list[ConsumerSwarm]
    ) -> None:
        def _check_at_least_one() -> bool:
            metrics: list[float] = []
            for node in swarm_nodes:
                metrics.append(node.get_metrics_summary(seconds=20).p50)
            total_rate = sum(metrics)
            _m = [str(m) for m in metrics]
            self.logger.debug(f"...last 20 sec rate is {total_rate} ({', '.join(_m)})")
            return total_rate >= 1

        # Value for progress checks is 20 sec
        # Since we expect slowdowns with big messages,
        # expect at least one message per 20 sec
        self.redpanda.wait_until(
            _check_at_least_one,
            timeout_sec=self.PROGRESS_TIMEOUT,
            backoff_sec=5,
            err_msg="Producer Swarm nodes not making progress",
        )

    def _get_rw_metrics(self) -> tuple[float, float]:
        # label options: kafka, internal
        def _get_samples(name: str, label: str = "kafka") -> tuple[list[float], float]:
            metrics = self.redpanda.metrics_sample(
                name=name, metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS
            )
            assert metrics is not None
            samples: list[float] = [
                s.value for s in metrics.samples if s.labels["redpanda_server"] == label
            ]
            total = sum(samples)
            return samples, total

        read_metric_name = "redpanda_rpc_received_bytes"
        sent_metric_name = "redpanda_rpc_sent_bytes"

        _, read_bytes = _get_samples(read_metric_name)
        _, sent_bytes = _get_samples(sent_metric_name)
        return read_bytes, sent_bytes

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(
        message_size_mib=[8, 32],
        apply_throughput_limits=[False, True],
        mode=[Mode.MANY_PARTS, Mode.TEN_TOPICS],
        default_consumer_config=[False, True],
    )
    def test_large_messages_throughput(
        self,
        message_size_mib: float,
        apply_throughput_limits: bool,
        mode: Mode,
        default_consumer_config: bool,
    ) -> None:
        """Test creates 10 topics, and uses client-swarm to
        generate 100 messages with parametrized size and sends this count
        to each topic and validates high watermark values along with expected
        throughput.
        """

        self.message_size = int(message_size_mib * 2**20)
        self.replication_factor = 3
        self.swarm_nodes = 2
        self.default_consumer_config = default_consumer_config

        # Scale tests are not run on debug builds
        assert not self.debug_mode

        # Init scale settings in the RP cluster
        self.scale = ScaleParameters(
            self.redpanda,
            self.replication_factor,
            tiered_storage_enabled=self.tiered_storage_enabled,
        )

        if self.scale.si_settings:
            self.redpanda.set_si_settings(self.scale.si_settings)

        # Enable large node-wide throughput limits to verify they work at scale
        # To avoid affecting the result of the test with the limit, set them
        # somewhat above the expect_bandwidth value per node.
        per_broker_throttle: int | None = None
        if apply_throughput_limits:
            per_broker_throttle = int(
                float(self.scale.expect_bandwidth) // len(self.redpanda.nodes) * 3
            )
            self.redpanda.add_extra_rp_conf(
                {
                    "kafka_throughput_limit_node_in_bps": per_broker_throttle,
                    "kafka_throughput_limit_node_out_bps": per_broker_throttle,
                }
            )

        # Start redpanda
        self.redpanda.start()

        throttle_str = (
            per_broker_throttle if per_broker_throttle is not None else "disabled"
        )
        self._test_large_messages(
            mode=mode,
            extra_log_fields=(
                f"apply_throughput_limits={apply_throughput_limits}, "
                f"per_broker_throttle={throttle_str}, "
            ),
        )

    def _setup_mode(self, mode: Mode) -> None:
        if mode == Mode.TEN_TOPICS:
            self.n_topics = 10
            self.n_partitions = 1
            self.n_clients = self.n_topics
            self.unique = True
        elif mode == Mode.MANY_PARTS:
            self.n_topics = 1
            self.n_partitions = (
                int(self.scale.partition_limit * 0.9) // self.swarm_nodes
            )
            clients_per_cpu = 4 if self.redpanda.dedicated_nodes else 1
            self.n_clients = (
                self.scale.node_cpus * self.scale.node_count * clients_per_cpu
            )
            self.unique = False
        else:
            assert_never(mode)  # pyright: ignore[reportUnreachable]

    def _setup_topic_names(self, topic_prefix: str) -> None:
        self.topic_prefix_template = topic_prefix
        self.topic_prefixes = [
            f"{self.topic_prefix_template}-n{i}" for i in range(self.swarm_nodes)
        ]
        if self.unique:
            self.topic_names = [
                f"{t}-{i}" for i in range(self.n_topics) for t in self.topic_prefixes
            ]
        else:
            self.topic_names = self.topic_prefixes

    def _test_large_messages(
        self,
        mode: Mode,
        cloud_topics: bool = False,
        extra_log_fields: str = "",
    ) -> None:
        """Shared workload driver for large message throughput tests.

        Callers are responsible for configuring self.message_size,
        self.replication_factor, self.swarm_nodes, self.default_consumer_config,
        self.scale, and starting redpanda before calling this.
        """

        self._setup_mode(mode)
        self._setup_topic_names("large-messages")

        # we size everything to this target runtime, 3 minutes, this will
        # result in very different amounts of total message volume in different
        # environments with different scale parameters.
        target_runtime_sec = 180

        # every increase of 1 in message_count increases total bytes written
        # by this amount
        bytes_per_message_count = self.n_clients * self.message_size * self.swarm_nodes

        self.expected_throughput: float = float(self.scale.expect_bandwidth)

        # size message_count to hit the target runtime
        self.message_count = (
            math.ceil(
                self.expected_throughput * target_runtime_sec / bytes_per_message_count
            )
            + 1
        )

        # Do create topics stage
        self._create_topics(cloud_topics=cloud_topics)

        # Do the healthcheck on RP
        # to make sure that all topics are settle down and have their leader
        self._wait_until_cluster_healthy()

        # Calculate some values
        total_bytes = (
            self.n_clients * self.message_count * self.message_size * self.swarm_nodes
        )

        self.logger.info(
            f"LargeMessagesTest parameters: "
            f"mode={mode.value}, cloud_topics={cloud_topics}, "
            f"message_size={self.message_size} bytes ({self.message_size / 2**20:.2f} MiB), "
            f"n_topics={self.n_topics}, n_partitions={self.n_partitions}, replication_factor={self.replication_factor}, "
            f"n_clients={self.n_clients}, swarm_nodes={self.swarm_nodes}, unique={self.unique}, "
            f"total_data={total_bytes / 1e6:.2f} MB, message_count={self.message_count}, "
            f"expected_throughput>={self.expected_throughput / 1e6:5.2f} MB/s, "
            f"{extra_log_fields}"
            f"target_runtime_sec={target_runtime_sec}"
        )

        assert self.message_count > 2, f"message count too low: {self.message_count}"

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
                err_msg="client-swarm did not read any messages after 120s, check swarm logs",
            )

            if not still_running:
                # consumer finished, the others are probably finished too
                self.logger.info("Consumer stopped, not waiting for any more")
                break

        # Wait for all messages to be produced
        self.logger.info("Measuring bandwidth")
        # Measure bandwidth each 2 seconds
        # if no new bytes received by RP, check swarm and exit
        # if at least one finished
        bandwidth_in: list[float] = []
        bandwidth_out: list[float] = []
        backoff_interval = 5
        total_elapsed = total_bytes_read = total_bytes_sent = 0
        overall_start_sec = interval_start_sec = time.time()
        producers_alive = len(swarm_producers)
        while (
            total_elapsed < (target_runtime_sec * 2)
            and producers_alive
            or len(bandwidth_in) < 2
        ):
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
            total_bytes_read += read - last_read
            total_bytes_sent += sent - last_sent

            self.logger.info(
                f"Bytes read: {read / 1e6:6.2f} MB ({bytes_per_sec_in / 1e6:6.2f}/{total_bytes_read / total_elapsed / 1e6:6.2f} MB/s interval/overall), "
                f"Bytes sent: {sent / 1e6:6.2f} MB ({bytes_per_sec_out / 1e6:6.2f}/{total_bytes_sent / total_elapsed / 1e6:6.2f} MB/s interval/overall)"
            )

            producers_alive = sum(s.is_alive() for s in swarm_producers)
            self.logger.info(
                f"{producers_alive}/{len(swarm_producers)} producers alive"
            )

            # Save measurements
            bandwidth_in.append(bytes_per_sec_in)
            bandwidth_out.append(bytes_per_sec_out)

            last_read = read
            last_sent = sent

            interval_start_sec = interval_end_sec

        # Run checks if swarm nodes finished
        swarms: list[tuple[list[ProducerSwarm] | list[ConsumerSwarm], str]] = [
            (swarm_producers, "producers"),
            (swarm_consumers, "consumers"),
        ]
        for swarm, swarm_name in swarms:
            self.logger.info(f"Stopping swarm {swarm_name}")
            for s in swarm:
                s.stop()

        self.logger.info("Calculating high watermarks for all topics")

        # Once again, do the healthcheck on RP
        # to make sure that all messages got delivered
        self._wait_until_cluster_healthy()

        # Topic hwm getter
        def _get_hwm(topic: str) -> int:
            _hwm = 0
            for partition in self.rpk.describe_topic(topic):
                # Add correct high watermark for topic
                _hwm += partition.high_watermark
            return _hwm

        hwms: list[int] = []
        # Use Thread pool to speed things up
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as exec:
            swarmnode_hwms = sum(exec.map(_get_hwm, self.topic_names))
        # save watermark for node
        hwms.append(swarmnode_hwms)

        assert all([hwm >= self.message_count for hwm in hwms]), (
            f"Message counts per swarm node mismatch: "
            f"target={self.message_count}, "
            f"swarm_nodes='''{', '.join([str(num) for num in hwms])}'''"
        )

        # Remove first measurement as it is more of a ramp up
        bandwidth_in = bandwidth_in[1:]
        bandwidth_out = bandwidth_out[1:]
        # Calculate overall throughput percentiles
        bw_in_perc = numpy.percentile(bandwidth_in, [50, 90, 99])
        bw_out_perc = numpy.percentile(bandwidth_out, [50, 90, 99])
        # Prettify for log
        str_in: list[str] = []
        str_out: list[str] = []
        for val in bw_in_perc:
            str_in.append(f"{val / 1e6:.02f} MB/sec")
        for val in bw_out_perc:
            str_out.append(f"{val / 1e6:.02f} MB/sec")
        self.logger.info(
            f"Measured bandwidth (avg, P90, P99):\n"
            f"RPC in: {', '.join(str_in)}\n"
            f"RPC out: {', '.join(str_out)}"
        )

        # Check that measured BW is not lower than expected
        recv_tput_avg = total_bytes_read / total_elapsed
        send_tput_avg = total_bytes_sent / total_elapsed

        def check_tput(tput: float, tname: str) -> None:
            assert tput > self.expected_throughput, (
                f"Measured {tname} bandwidth is lower than expected: "
                f"{tput} vs {self.expected_throughput}"
            )

        check_tput(recv_tput_avg, "input")
        check_tput(send_tput_avg, "output")

        # The lines below were added to debug a shutdown hang.
        # Remove once CORE-13977 is resolved.
        self.redpanda._admin.set_log_level("raft", "debug")

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(
        message_size_mib=[8, 32],
        mode=[Mode.MANY_PARTS, Mode.TEN_TOPICS],
    )
    def test_cloud_topics_large_messages_throughput(
        self,
        message_size_mib: float,
        mode: Mode,
    ) -> None:
        """Cloud topics variant of the large messages throughput test.

        Validates that cloud topics can handle large messages (8-32 MiB)
        at acceptable throughput. Uses the same workload structure as the
        regular test but with all topics created as cloud topics.
        """

        self.message_size = int(message_size_mib * 2**20)
        self.replication_factor = 3
        self.swarm_nodes = 2
        self.default_consumer_config = False

        assert not self.debug_mode

        # Cloud topics require SISettings for the object storage backend,
        # but we don't use ScaleParameters' tiered_storage_enabled since
        # that tunes for tiny segments (32KB) which is wrong for large
        # messages.
        si_settings = SISettings(
            self.test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.add_extra_rp_conf(
            {
                CLOUD_TOPICS_CONFIG_STR: True,
                "enable_cluster_metadata_upload_loop": False,
            }
        )

        self.scale = ScaleParameters(
            self.redpanda,
            self.replication_factor,
            tiered_storage_enabled=False,
        )

        self.redpanda.start()

        self._test_large_messages(
            mode=mode,
            cloud_topics=True,
        )
