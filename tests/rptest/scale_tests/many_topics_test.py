# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ast import main
import threading
import json
import requests
import time
import random
import subprocess
import sys
import concurrent.futures
import numpy

from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

from confluent_kafka import KafkaError, KafkaException

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, LoggingConfig, MetricsEndpoint, PandaproxyConfig, SchemaRegistryConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.node_operations import NodeOpsExecutor
from rptest.util import inject_remote_script, firewall_blocked
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.consumer_swarm import ConsumerSwarm
from rptest.scale_tests.topic_scale_profiles import TopicScaleProfileManager
from rptest.clients.python_librdkafka import PythonLibrdkafka

HTTP_GET_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

HTTP_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}


class ManyTopicsTest(RedpandaTest):

    LEADER_BALANCER_PERIOD_MS = 60 * 1_000  # 60s

    # Max time to wait for the cluster to be healthy once more.
    HEALTHY_WAIT_SECONDS = 20 * 60

    # Max topic count supported by single client-swarm node
    MAX_SWARM_NODE_TOPIC_COUNT = 15_000

    # Up to 5 min to stop the node with a lot of topics
    STOP_TIMEOUT = 60 * 5

    # Progress wait timeout
    PROGRESS_TIMEOUT = 60 * 3

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            num_brokers=10,
            # This configuration allows dangerously high partition counts. That's okay
            # because we want to stress the controller itself, so we won't apply
            # produce load.
            extra_rp_conf={
                # Avoid having to wait 5 minutes for leader balancer to activate
                "leader_balancer_idle_timeout": self.LEADER_BALANCER_PERIOD_MS,
                "leader_balancer_mute_timeout": self.LEADER_BALANCER_PERIOD_MS,

                # Increase connections limit to well above what this test reaches
                "kafka_connections_max": 100_000,
                "kafka_connections_max_per_ip": 100_000,

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
        self.node_ops_exec = NodeOpsExecutor(
            self.redpanda,
            self.logger,
            self.thread_local,
            progress_timeout=self.HEALTHY_WAIT_SECONDS)
        # Ongoing test vars
        self._current_profile = None
        self._swarm_producers = []
        self._target_port = None
        self._target_downtime_sec = 300

    def setUp(self):
        # start the nodes manually
        pass

    def _start_initial_broker_set(self):
        seed_nodes = self.redpanda.nodes[0:-1]
        self._standby_broker = self.redpanda.nodes[-1]

        self.redpanda.set_seed_servers(seed_nodes)
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)

    def _try_parse_json(self, node, jsondata):
        try:
            return json.loads(jsondata)
        except ValueError:
            self.logger.debug(
                f"{str(node.account)}: Could not parse as json: {str(jsondata)}"
            )
            return None

    def _produce_messages_to_random_topics(self, kclient, message_count,
                                           num_topics, topic_names):
        """Select random num_topics from the list topic_names
        and send message_count to it with consecutive numbers as values

        Args:
            kclient (_type_): python_librdkafka client
            message_count (_type_): number of messages to produce
            num_topics (_type_): number of random topics to produce to
            topic_names (_type_): list of topic names

        return values:
            used_topics_list, ununsed_topics_list, errors
        """
        def _send_messages(topic):
            """
                Simple function that sends indices as messages
            """
            errors = []

            def acked(err, msg):
                """
                    Simple and unsafe callback
                """
                if err is not None:
                    errors.append(f"FAIL: {str(msg)}: {str(err)}")

            # Sent indices as values
            p = kclient.get_producer()
            sent_count = 0
            for idx in range(1, message_count + 1):
                # Async message sending func
                p.produce(topic,
                          key=f"key_{idx}",
                          value=f"{idx:04}",
                          callback=acked)
                # Make sure message sent, aka sync
                p.flush()
                sent_count += 1
            # Return stats and errors
            return sent_count, errors

        # Pick random topics to send messages to
        total_topics = len(topic_names) - 1
        if total_topics > num_topics:
            random_topic_indices = [
                random.randint(0, total_topics) for i in range(num_topics)
            ]
            next_topic_batch = []
            while len(random_topic_indices) > 0:
                next_topic_batch.append(
                    topic_names[random_topic_indices.pop()])
            new_topic_names = list(set(topic_names) - set(next_topic_batch))
        else:
            next_topic_batch = topic_names
            new_topic_names = []

        # Send messages
        messages_sent = 0
        errors = []
        with concurrent.futures.ThreadPoolExecutor(16) as executor:
            for c, thread_errors in executor.map(_send_messages,
                                                 next_topic_batch):
                messages_sent += c
                errors += thread_errors
        self.logger.info(f"Total of {messages_sent} messages sent")

        total_errors = len(errors)
        if total_errors > 0:
            self.logger.error(f"{total_errors} Errors detected "
                              "while sending messages")

        return next_topic_batch, new_topic_names, errors

    def _consume_messages_from_random_topic(self,
                                            kclient,
                                            message_count,
                                            topic_names,
                                            timeout_sec=300):
        """Consume message_count from random topic in the list topic_names

        Args:
            kclient (_type_): python_librdkafka client
            message_count (_type_): number of messages to consume
            topic_names (_type_): list of topic names to select from
            timeout_sec (int, optional): Timeout. Defaults to 300.

        Raises:
            KafkaException: On Kafka transport errors
            RuntimeError: On timeout consuming messages
            RuntimeError: On non-consecutive values in messages

        Returns: None
        """

        # Function checks if numbers in list are consecutive
        def check_consecutive(numbers_list):
            n = len(numbers_list) - 1
            # Calculate iterative difference for the array. It should be 1.
            return (sum(numpy.diff(sorted(numbers_list)) == 1) >= n)

        # Select random topic from the list
        target_topic = topic_names[random.randint(0, len(topic_names))]
        # Consumer specific config
        consumer_extra_config = {
            "auto.offset.reset": "smallest",
            "group.id": "topic_swarm_group"
        }
        self.logger.info(f"Start consuming from {target_topic}")
        # Consumer
        start_time_s = time.time()
        consumer = kclient.get_consumer(consumer_extra_config)
        numbers = []
        elapsed = 0.0
        # Message consuming loop
        try:
            consumer.subscribe([target_topic])
            while True:
                # calculate elapsed time
                elapsed = time.time() - start_time_s

                # Exit on target number reached or timeout
                if len(numbers) == message_count or elapsed > timeout_sec:
                    break

                # Poll for the message
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                # On error, check for the EOF
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.logger.info(f"Consumer of '{msg.topic()}' "
                                         f"[{msg.partition()}] reached "
                                         f"end at offset {msg.offset()}")
                        break
                    # If not EOF, raise it
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Save value from the message
                    numbers.append(int(msg.value()))
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()

        self.logger.info(f"Consumed {len(numbers)} messages")
        # Check that we received all numbers
        if elapsed > timeout_sec:
            raise RuntimeError("Timeout consuming messages "
                               f"from {target_topic}")
        elif not check_consecutive(numbers):
            raise RuntimeError("Produced and consumed messages mismatch "
                               f"for {target_topic}")

        return

    def _write_and_random_read_many_topics(self, message_count, num_topics,
                                           topic_names):
        """
            Test checks that each of the many topics can be written to.
            This produce/consume implementation will check actual data
            of the messages to ensure that all of the messages are delivered

            Pick X number of topics, write 100 messages in each
            Pick random one among them, consume all messages
            Iterate.
        """
        # Prepare librdkafka python client
        kclient = PythonLibrdkafka(self.redpanda)
        self.logger.info(
            f"Producing {message_count} messages to {num_topics} random topics"
        )

        # Produce messages
        used_topics, unused_topics, errors = \
            self._produce_messages_to_random_topics(kclient, message_count,
                                                    num_topics, topic_names)

        # Consume messages
        # Will raise RuntimeException on timeout
        # or non-consecutive message values
        self._consume_messages_from_random_topic(kclient,
                                                 message_count,
                                                 used_topics,
                                                 timeout_sec=300)

        # Return list of topics that was not used
        return unused_topics, errors

    def _create_many_topics(self,
                            brokers,
                            node,
                            topic_name_prefix,
                            topic_count,
                            batch_size,
                            num_partitions,
                            num_replicas,
                            use_kafka_batching,
                            topic_name_length=200,
                            skip_name_randomization=False):
        """Function uses batched topic creation approach.
        Its either single topic per request using ThreadPool in batches or
        the whole batch in single kafka request.

        Args:
            topic_count (int): Number of topics to create
            batch_size (int): Batch size for one create operation
            topic_name_length (int): Total topic length for randomization
            num_partitions (int): Number of partitions per topic
            num_replicas (int): Number of replicas per topic
            use_kafka_batching (bool): on True sends whole batch as a single
            request.

        Raises:
            RuntimeError: Underlying creation script generated error

        Returns:
            list: topic names and timing data
        """
        def log_timings_with_percentiles(timings):
            # Extract data
            created_count = timings.get('count_created', -1)

            # Add min/max time to the log
            tmin = timings.get('creation-time-min', 0)
            tmax = timings.get('creation-time-max', 0)
            tp_str = f"...{created_count} topics: " \
                "min = {:>7,.3f}s, " \
                "max = {:>7,.3f}s, ".format(tmin, tmax)

            # Calculate percentiles for latest batch
            prc = [25, 50, 75, 90, 95, 99]
            creation_times = timings.get('creation_times', [])
            tprc = numpy.percentile(creation_times, prc)
            for i in range(len(prc)):
                tp_str += "p{} ={:7,.3f}s, ".format(prc[i], tprc[i])
            # Log them
            self.logger.debug(tp_str)

        # Prepare command
        remote_script_path = inject_remote_script(node, "topic_operations.py")
        cmd = f"python3 {remote_script_path} "
        cmd += f"--brokers '{brokers}' "
        cmd += f"--batch-size '{batch_size}' "
        cmd += "create "
        cmd += f"--topic-prefix '{topic_name_prefix}' "
        cmd += f"--topic-count {topic_count} "
        cmd += "--kafka-batching " if use_kafka_batching else ""
        cmd += f"--topic-name-length {topic_name_length} "
        cmd += f"--partitions {num_partitions} "
        cmd += f"--replicas {num_replicas} "
        cmd += "--skip-randomize-names" if skip_name_randomization else ""
        hostname = node.account.hostname
        self.logger.info(f"Starting topic creation script on '{hostname}")
        self.logger.debug(f"...cmd: {cmd}")

        data = {}
        for line in node.account.ssh_capture(cmd):
            # The script will produce jsons one, per line
            # And it will have timings only
            self.logger.debug(f"received {sys.getsizeof(line)}B "
                              f"from '{hostname}'.")
            data = self._try_parse_json(node, line.strip())
            if data is not None:
                if 'error' in data:
                    self.logger.warning(f"Node '{hostname}' reported "
                                        f"error:\n{data['error']}")
                    raise RuntimeError(data['error'])
                else:
                    # Extract data
                    timings = data.get('timings', {})
                    log_timings_with_percentiles(timings)
            else:
                data = {}

        topic_details = data.get('topics', [])
        current_count = len(topic_details)
        self.logger.info(f"Created {current_count} topics")
        assert len(topic_details) == topic_count, \
            f"Topic count not reached: {current_count}/{topic_count}"

        return topic_details

    def _wait_for_topic_count(self, count):
        def has_count_topics():
            num_topics = self.redpanda.metric_sum(
                'redpanda_cluster_topics',
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
                nodes=self.redpanda.started_nodes())
            self.logger.info(
                f"current topic count: {num_topics} target count: {count}")
            return num_topics >= count

        wait_until(lambda: has_count_topics(),
                   timeout_sec=self.HEALTHY_WAIT_SECONDS,
                   backoff_sec=30,
                   err_msg=f"couldn't reach topic count target: {0}")

    def _wait_until_cluster_healthy(self, include_underreplicated=True):
        """
        Waits until the cluster is reporting no under-replicated or leaderless partitions.
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

    def _add_standby_node(self):
        # Add a new node in the cluster replace the one that will soon be decomissioned.
        self.logger.debug(
            f"Adding node {self._standby_broker.name} to the cluster")
        self.redpanda.clean_node(self._standby_broker)
        self.redpanda.start_node(self._standby_broker,
                                 first_start=True,
                                 auto_assign_node_id=True)
        wait_until(lambda: self.redpanda.registered(self._standby_broker),
                   timeout_sec=60,
                   backoff_sec=5)

    def _remove_standby_node(self):
        # Add a new node in the cluster replace the one that will soon be decomissioned.
        self.logger.debug(
            f"Removing node {self._standby_broker.name} to the cluster")
        self.redpanda.stop_node(self._standby_broker,
                                timeout=self.STOP_TIMEOUT)
        self.redpanda.clean_node(self._standby_broker)

    def _decommission_node_unsafely(self):
        """
        Simulates a common failure of a node dying and a new
        node being created to replace it.
        """
        # select a node at random from the current broker set to decom.
        node_to_decom = random.choice(self.redpanda.started_nodes())
        self.logger.debug(f"Force stopping node {node_to_decom.name}")
        node_id = self.redpanda.node_id(node_to_decom, force_refresh=True)
        self.redpanda.stop_node(node_to_decom, forced=True)

        # clean node so we can re-used it as the "newly" created replacement node:
        self.logger.debug(f"Adding node {node_to_decom.name} to the cluster")
        self.redpanda.clean_node(node_to_decom)
        self.redpanda.start_node(node_to_decom,
                                 first_start=True,
                                 auto_assign_node_id=True)
        self.node_ops_exec.decommission(self.redpanda.idx(node_to_decom),
                                        node_id=node_id)

    def _decommission_node_safely(self):
        """
        Starts `self._standby_broker` and decomissions a random existing broker from the cluster.
        Replaces `self._standby_broker` with the broker that was removed from the cluster.
        """
        node_to_decom = random.choice(self.redpanda.started_nodes())

        # Add a new node in the cluster replace the one that will soon be decomissioned.
        self._add_standby_node()

        # select a node at random from the current broker set to decom.
        self.logger.debug(f"Decommissioning node {node_to_decom.name}")
        node_to_decom_idx = self.redpanda.idx(node_to_decom)
        node_to_decom_id = self.redpanda.node_id(node_to_decom)
        self.node_ops_exec.decommission(node_to_decom_idx)
        self.node_ops_exec.wait_for_removed(node_to_decom_id)
        self.node_ops_exec.stop_node(node_to_decom_idx)

        self._standby_broker = node_to_decom

    def _get_partition_count(self):
        return self.redpanda.metric_sum(
            'redpanda_cluster_partitions',
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS,
            nodes=self.redpanda.started_nodes())

    def _wait_for_leadership_balanced(self):
        def is_balanced(threshold=0.1):
            leaders_per_node = [
                self.redpanda.metric_sum('vectorized_cluster_partition_leader',
                                         nodes=[n])
                for n in self.redpanda.started_nodes()
            ]
            stddev = numpy.std(leaders_per_node)
            error = stddev / (self._get_partition_count() /
                              len(self.redpanda.started_nodes()))
            self.logger.info(
                f"leadership info (stddev: {stddev:.2f}; want error {error:.2f} < {threshold})"
            )

            return error < threshold

        wait_until(is_balanced,
                   timeout_sec=self.HEALTHY_WAIT_SECONDS,
                   backoff_sec=30)

    def _in_maintenance_mode(self, node):
        status = self.admin.maintenance_status(node)
        return status["draining"]

    def _enable_maintenance_mode(self, node):
        self.admin.maintenance_start(node)
        wait_until(lambda: self._in_maintenance_mode(node),
                   timeout_sec=30,
                   backoff_sec=5)

        def has_drained_leadership():
            status = self.admin.maintenance_status(node)
            self.logger.debug(f"Maintenance status for {node.name}: {status}")
            if all([
                    key in status
                    for key in ['finished', 'errors', 'partitions']
            ]):
                return status["finished"] and not status["errors"] and \
                        status["partitions"] > 0
            else:
                return False

        self.logger.debug(f"Waiting for node {node.name} leadership to drain")
        wait_until(has_drained_leadership,
                   timeout_sec=self.HEALTHY_WAIT_SECONDS,
                   backoff_sec=30)

    def _disable_maintenance_mode(self, node):
        self.admin.maintenance_stop(node)

        wait_until(lambda: not self._in_maintenance_mode(node),
                   timeout_sec=self.HEALTHY_WAIT_SECONDS,
                   backoff_sec=10)

    def _rolling_restarts(self, safe=True):
        """
        Simulates a cluster upgrade by doing a rolling restart of all started nodes.
        Each node is put in maintenance mode and is restarted only after all leadership
        has been drained from it.
        """
        def restart_node(node):
            if safe:
                self._enable_maintenance_mode(node)
            self.redpanda.restart_nodes(node)
            if safe:
                self._disable_maintenance_mode(node)
                self._wait_for_leadership_balanced()

        for node in self.redpanda.started_nodes():
            self.logger.debug(f"Starting restart for node {node.name}")
            restart_node(node)

    def _stage_create_topics(self, profile):
        # Brokers list suitable for script arguments
        brokers = ",".join(self.redpanda.brokers_list())

        # With current settings, there should be single available node
        node = self.cluster.alloc(ClusterSpec.simple_linux(1))[0]

        # Call function to create the topics
        topic_details = self._create_many_topics(
            brokers,
            node,
            profile.topic_name_prefix,
            profile.topic_count,
            profile.batch_size,
            profile.num_partitions,
            profile.num_replicas,
            profile.use_kafka_batching,
            topic_name_length=profile.topic_name_length,
            skip_name_randomization=False)

        # Free node that used to create topics
        self.cluster.free_single(node)

        return topic_details

    def _wait_workload_progress(self):
        def _check():
            metrics = []
            for node in self._swarm_producers:
                metrics.append(node.get_metrics_summary(seconds=20).p50)
            total_rate = sum(metrics)
            _m = [str(m) for m in metrics]
            self.logger.debug(f"...last 20 sec rate is {total_rate} "
                              f"({', '.join(_m)})")
            return total_rate >= target_rate

        # Value for progress checks is 20 sec
        # We'll have exactly one node here, so the rate should be exactly as configured
        target_rate = 1
        # Safely try to pull the current profile rate
        if self._current_profile is not None:
            target_rate = self._current_profile.messages_per_second_per_producer

        self.redpanda.wait_until(
            _check,
            timeout_sec=self.PROGRESS_TIMEOUT,
            backoff_sec=5,
            err_msg="Producer Swarm nodes not making progress")

    def _wait_for_leadership_stabilized(self, maintenance_node):
        def is_stabilized(threshold=0.1):
            leaders_per_node = [
                self.redpanda.metric_sum('vectorized_cluster_partition_leader',
                                         nodes=[n]) for n in nodes
            ]
            stddev = numpy.std(leaders_per_node)
            error = stddev / (self._get_partition_count() / len(nodes))
            self.logger.info(
                f"leadership info (stddev: {stddev:.2f}; want error {error:.2f} < {threshold})"
            )

            return error < threshold

        # get nodes w/o maintenance one
        nodes = [
            n for n in self.redpanda.started_nodes() if n != maintenance_node
        ]
        wait_until(is_stabilized,
                   timeout_sec=self.HEALTHY_WAIT_SECONDS,
                   backoff_sec=30)

        return

    def _select_random_node(self):
        ids = [self.redpanda.node_id(n) for n in self.redpanda.started_nodes()]
        return self.redpanda.get_node_by_id(random.choice(ids))

    def _restart_safely(self):
        # Put node in maintenance
        maintenance_node = self._select_random_node()
        self.logger.info("Selected maintenance node is "
                         f"'{maintenance_node.account.hostname}'")
        self._enable_maintenance_mode(maintenance_node)

        # Wait for healthy status
        self.logger.info("Waiting for leadership stabilization ")
        self._wait_for_leadership_stabilized(maintenance_node)
        self._wait_until_cluster_healthy()

        # Check workload progress
        self.logger.info("Waiting for progress on producers")
        self._wait_workload_progress()

        # Stop node in maintenance
        self.logger.info("Stopping maintenance node of "
                         f"'{maintenance_node.account.hostname}'")
        self.redpanda.stop_node(maintenance_node, timeout=self.STOP_TIMEOUT)

        # Again, wait for healthy cluster
        # This time underreplicated partion count should spike, ignore that
        # until restart
        self.logger.info("Making sure that cluster is healthy")
        self._wait_until_cluster_healthy(include_underreplicated=False)

        # Check workload progress
        self.logger.info("Waiting for progress on producers")
        self._wait_workload_progress()

        # Restart node
        self.logger.info("Starting maintenance node of "
                         f"'{maintenance_node.account.hostname}'")
        self.redpanda.start_node(maintenance_node)
        self.logger.info("Disabling maintenance on node "
                         f"'{maintenance_node.account.hostname}'")
        self._disable_maintenance_mode(maintenance_node)

    def _restart_unsafely(self):
        # Stop node in maintenance
        maintenance_node = self._select_random_node()
        self.logger.info("Selected node for hard stop is "
                         f"'{maintenance_node.account.hostname}'")
        self.redpanda.stop_node(maintenance_node,
                                timeout=self.STOP_TIMEOUT,
                                forced=True)

        # Again, wait for healthy cluster
        # There will be underreplicated partitions spike, ignore it
        self.logger.info("Making sure that cluster is healthy")
        self._wait_until_cluster_healthy(include_underreplicated=False)

        # Check workload progress
        self.logger.info("Waiting for progress on producers")
        self._wait_workload_progress()

        # Restart node
        self.redpanda.start_node(maintenance_node)
        self.logger.info("Disabling maintenance on node "
                         f"'{maintenance_node.account.hostname}'")
        self._disable_maintenance_mode(maintenance_node)

        # Check workload progress
        self.logger.info("Waiting for progress on producers")
        self._wait_workload_progress()

    def _isolate(self, nodes):
        with firewall_blocked(nodes, self._target_port, full_block=True):
            self.logger.info("Waiting for cluster to acknoledge isolation")
            time.sleep(30)

            self.logger.info("Ensure workloads is progressing")
            self._wait_workload_progress()

            self.logger.info(f"Simulating {self._target_downtime_sec} sec "
                             "isolation")
            time.sleep(self._target_downtime_sec)

            self.logger.info("Ensure leadership stays balanced")
            self._wait_for_leadership_balanced()

            self.logger.info("Ensure workloads is progressing")
            self._wait_workload_progress()

            self.logger.info("Ensure cluster healthy, "
                             "ignoring underreplicated partitions")
            self._wait_until_cluster_healthy(include_underreplicated=False)

    def _isolate_all_nodes(self):
        if self._target_port is None:
            raise RuntimeError("Isolation port not selected")

        self.logger.info("Isolating all redpanda nodes on port "
                         f"{self._target_port}")
        self._isolate(self.redpanda.nodes)

        # Clean out isolation port
        self._target_port = None

    def _isolate_random_node(self):
        if self._target_port is None:
            raise RuntimeError("Isolation port not selected")

        # Pick random node
        isolated_node = self._select_random_node()
        self.logger.info("Selected node for isolation "
                         f"'{isolated_node.account.hostname}'")

        # Isolate node using specified port
        self._isolate([isolated_node])

        # Clean out isolation port
        self._target_port = None

    @skip_debug_mode
    @cluster(num_nodes=11, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommission_safely(self):
        self._start_initial_broker_set()

        tsm = TopicScaleProfileManager()
        profile = tsm.get_profile("topic_profile_t40k_p1")

        # Create topics
        topic_details = self._stage_create_topics(profile)
        # Wait for topic count and do a health check
        self._wait_for_topic_count(profile.topic_count)
        self._wait_until_cluster_healthy()

        #
        # Traffic checks

        topics_to_go = [t['name'] for t in topic_details]
        # Messages to produce
        message_count = 100
        self.logger.info(
            f"Starting Produce/Consume stage for {len(topics_to_go)} topics")
        producer_errors = []
        while len(topics_to_go) > 0:
            topics_to_go, errors = self._write_and_random_read_many_topics(
                message_count, profile.batch_size, topics_to_go)
            producer_errors += errors
            self.logger.info(
                f"iteration complete, topics left {len(topics_to_go)}")

        total_errors = len(producer_errors)
        if total_errors > 0:
            _errors_str = '\n'.join(producer_errors)
            self.logger.error(f"Producer errors:\n{_errors_str}")
        assert total_errors < 1, \
            f"{total_errors} errors detected while sending messages"
        self.logger.info("Produce/Consume stage complete")

        unsafe_start = time.time()
        self._decommission_node_safely()
        self._wait_until_cluster_healthy()
        unsafe_end = time.time()

        self.logger.warn(
            f"Time it took to replace node {unsafe_end - unsafe_start}")

    @cluster(num_nodes=11, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_topic_swarm(self):
        """Test creates 11950 topics, validates partitions and replicas,
        produces 100 messages to each topic using batches, consumes
        messages from random topic in each batch and validates message content

        Returns:
            None
        """

        # Max number of partitions for i3en.xlarge
        # default settings is 1000 partitions per shard/cpu
        # (9 nodes × 4 vcpus/shards × 1000) / 3 replicas
        # 12000
        tsm = TopicScaleProfileManager()
        profile = tsm.get_profile("topic_profile_t10k_p1")

        # Start kafka
        self.redpanda.start()

        # Brokers list suitable for script arguments
        brokers = ",".join(self.redpanda.brokers_list())

        # With current settings, there should be single available node
        node = self.cluster.alloc(ClusterSpec.simple_linux(1))[0]

        # Call function to create the topics
        topic_details = self._create_many_topics(
            brokers,
            node,
            profile.topic_name_prefix,
            profile.topic_count,
            profile.batch_size,
            profile.num_partitions,
            profile.num_replicas,
            profile.use_kafka_batching,
            topic_name_length=profile.topic_name_length,
            skip_name_randomization=False)

        # Validate topics
        topics = self.rpk.list_topics(detailed=True)

        # Validate topic creation
        self.logger.info("Validating created topics")
        topics_ok = []
        topics_failed = []

        # Searches for a topic in our list
        def get_topic_index(name):
            for idx in range(len(topic_details)):
                if name == topic_details[idx]['name']:
                    return idx
            return -1

        # Validate RP topics against requested details
        for name, partitions, replicas in topics:
            err = "ok"
            idx = get_topic_index(name)
            if idx < 0:
                # RP has topic that is not ours
                # Cluster was clean and it is an issue
                err = f"Unexpected topic found: {name}"
            elif int(replicas) != topic_details[idx]['replicas']:
                # Replication factor is wrong
                err = f"Replication factor error: current {replicas}, " \
                f"target {topic_details[idx]['replicas']}"
            elif int(partitions) != topic_details[idx]['partitions']:
                # Partitions number is wrong
                err = f"Partitions number error: current {partitions}, " \
                f"target {topic_details[idx]['partitions']}"
            else:
                # Topic passed the check
                topics_ok.append(topic_details[idx]['name'])
                continue
            # one of errors happened, add topic to failed list
            # And add error message
            failed_topic = (topic_details[idx], err)
            self.logger.warning(f"Topic {topic_details[idx]['name']}, {err}")
            topics_failed.append(failed_topic)

        # Check that no errors happened
        failed = len(topics_failed)
        assert failed < 1, f"{failed} RP topics has errors"

        topics_available = []
        topics_unavailable = []
        # Validate our list against RP
        for topic in topic_details:
            # make sure topic is present in RP
            if topic['name'] not in topics_ok:
                topics_unavailable.append(topic['name'])
            else:
                topics_available.append(topic['name'])
        unavailable = len(topics_unavailable)
        assert unavailable < 1, f"{unavailable} topics are missing in RP"

        # Free node that used to create topics
        self.cluster.free_single(node)

        # Move on to traffic checks
        topics_to_go = topics_available
        # Messages to produce
        message_count = 100
        self.logger.info(
            f"Starting Produce/Consume stage for {len(topics_to_go)} topics")
        producer_errors = []
        while len(topics_to_go) > 0:
            topics_to_go, errors = self._write_and_random_read_many_topics(
                message_count, profile.batch_size, topics_to_go)
            producer_errors += errors
            self.logger.info(
                f"iteration complete, topics left {len(topics_to_go)}")

        total_errors = len(producer_errors)
        if total_errors > 0:
            _errors_str = '\n'.join(producer_errors)
            self.logger.error(f"Producer errors:\n{_errors_str}")
        assert total_errors < 1, \
            f"{total_errors} errors detected while sending messages"
        self.logger.info("Produce/Consume stage complete")

        return

    def _run_producers_with_constant_rate(self, profile, node_topic_count,
                                          topics):
        swarm_node_producers = []
        for topic in topics:
            swarm_producer = ProducerSwarm(
                self.test_context,
                self.redpanda,
                topic,
                node_topic_count,
                profile.message_count,
                unique_topics=True,
                messages_per_second_per_producer=profile.
                messages_per_second_per_producer)
            swarm_node_producers.append(swarm_producer)

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_producers:
            self.logger.info(f"Starting swarm client on node {swarm_client}")
            swarm_client.start()

        return swarm_node_producers

    def _run_consumers_with_constant_rate(self, profile, node_topic_count,
                                          topics, group):
        swarm_node_consumers = []
        node_message_count = int(0.95 *
                                 (profile.message_count * node_topic_count))
        for topic in topics:
            swarm_producer = ConsumerSwarm(self.test_context,
                                           self.redpanda,
                                           topic,
                                           group,
                                           node_topic_count,
                                           node_message_count,
                                           unique_topics=True,
                                           unique_groups=True)
            swarm_node_consumers.append(swarm_producer)

        # Run topic swarm for each topic group
        for swarm_client in swarm_node_consumers:
            self.logger.info(f"Starting swarm client on node {swarm_client}")
            swarm_client.start()

        return swarm_node_consumers

    def _adjust_topic_count(self, node_topic_count, nodes_available,
                            max_topics):
        # swarm node can generate traffic for 4.5k topics max due to network limitations
        new_node_topic_count = node_topic_count
        if new_node_topic_count > self.MAX_SWARM_NODE_TOPIC_COUNT:
            self.logger.warning(
                f"Cluster supports up to {max_topics}"
                " topics, nodes available for swarm "
                f"{nodes_available}, topics per swarm node would be "
                f"{node_topic_count}, which is more than one "
                f"node can handle ({self.MAX_SWARM_NODE_TOPIC_COUNT})")
            new_node_topic_count = self.MAX_SWARM_NODE_TOPIC_COUNT
            self.logger.warning("Setting swarm topic count to MAX of "
                                f"{self.MAX_SWARM_NODE_TOPIC_COUNT} per swarm "
                                f"node ({nodes_available} swarm nodes * "
                                f"{self.MAX_SWARM_NODE_TOPIC_COUNT} = "
                                f"{new_node_topic_count * nodes_available})")
        else:
            self.logger.warning("Using swarm topic count of "
                                f"{node_topic_count} per swarm node "
                                f"({nodes_available} swarm nodes * "
                                f"{node_topic_count} = "
                                f"{new_node_topic_count * nodes_available})")

        return new_node_topic_count

    def _stage_create_topics_adjusted(self, profile, producer_node_only=False):
        # Brokers list suitable for script arguments
        brokers = ",".join(self.redpanda.brokers_list())

        nodes_available = self.test_context.cluster.available().size()
        if producer_node_only:
            produce_nodes = nodes_available
            consume_nodes = 0
            # get topic count
            produce_node_topic_count = profile.topic_count // produce_nodes
            consume_node_topic_count = 0
        else:
            if nodes_available < 2:
                raise RuntimeError("Not enough nodes for producers and "
                                   f"consumers. Available {nodes_available}")
            # Divide available nodes between producers and consumers
            produce_nodes = nodes_available // 2
            consume_nodes = nodes_available // 2

            # get the target topic counts
            # It is understood that these numbers will be the same
            # But for code readability, it is divided into producers and consumers
            produce_node_topic_count = profile.topic_count // produce_nodes
            consume_node_topic_count = profile.topic_count // consume_nodes

        # Calculate topic counts
        # This test by default designed for 12 nodes, 9 node cluster, 4 cpus each node
        # I.e. 12k topics max
        num_cpus = self.redpanda.get_node_cpu_count()
        # 1000 partitions per shard
        max_supported_topics = num_cpus * 1000 * len(self.redpanda.nodes) // 3
        # Account for system level topics
        max_supported_topics -= 50

        # uncomment for manual max topics
        # node_topic_count = max_supported_topics // nodes_available

        if produce_node_topic_count > 0:
            self.logger.warning("Checking produce swarm nodes topic count")
            produce_node_topic_count = self._adjust_topic_count(
                produce_node_topic_count, produce_nodes, max_supported_topics)
        if consume_node_topic_count > 0:
            self.logger.warning("Checking consume swarm nodes topic count")
            consume_node_topic_count = self._adjust_topic_count(
                consume_node_topic_count, consume_nodes, max_supported_topics)

        # Grab node to run creation script on it.
        node = self.cluster.alloc(ClusterSpec.simple_linux(1))[0]
        topic_prefixes = []
        # Create unique topics for each swarm node
        for idx in range(produce_nodes):
            node_topic_name_prefix = f"{profile.topic_name_prefix}-{idx}"
            # Call function to create the topics
            topic_details = self._create_many_topics(
                brokers,
                node,
                node_topic_name_prefix,
                produce_node_topic_count,
                profile.batch_size,
                profile.num_partitions,
                profile.num_replicas,
                profile.use_kafka_batching,
                topic_name_length=profile.topic_name_length,
                skip_name_randomization=True)

            self.logger.info(f"Created {len(topic_details)} topics with "
                             f"prefix of '{node_topic_name_prefix}'")

            topic_prefixes.append(node_topic_name_prefix)
        # Free node that used to create topics
        self.cluster.free_single(node)

        return topic_prefixes, produce_node_topic_count, consume_node_topic_count

    def _lifecycle_test_impl(self, test):
        self._start_initial_broker_set()

        tsm = TopicScaleProfileManager()
        profile = tsm.get_custom_profile("topic_profile_t40k_p1",
                                         {"message_count": 25 * 60})
        self._current_profile = profile

        ##
        # Create topics
        #

        topic_prefixes, pnode_topic_count, cnode_topic_count = \
            self._stage_create_topics_adjusted(profile)

        # Do the healthcheck on RP
        # to make sure that all topics are settle down and have their leader
        self._wait_until_cluster_healthy()

        ##
        # Create clients
        #

        # Calculate how much time ideally needed for the producers to finish
        running_time_sec = \
            profile.message_count // profile.messages_per_second_per_producer

        # Run swarm producers
        swarm_producers = self._run_producers_with_constant_rate(
            profile, pnode_topic_count, topic_prefixes)

        # Save producers for underlying test to use
        self._swarm_producers = swarm_producers
        # Run swarm consumers
        _group = "topic_swarm_group"
        swarm_consumers = self._run_consumers_with_constant_rate(
            profile, cnode_topic_count, topic_prefixes, _group)

        # Allow time for clients to start and stablize
        time.sleep(2 * 60)

        ##
        # Lifecycle test
        #

        self.logger.info("Starting lifecycle test")
        test()
        self._wait_until_cluster_healthy()
        self.logger.info("Finished lifecycle test")

        ##
        # Validate results
        #

        # account for delays from rolling restarts
        running_time_sec = 5 * running_time_sec

        # Run checks if swarm nodes finished
        self.logger.info("Make sure that swarm node producers are finished")
        for s in swarm_producers:
            s.wait(running_time_sec)
        self.logger.info("Make sure that swarm node consumers are finished")
        for s in swarm_consumers:
            s.wait(running_time_sec)

        # Clean
        self._swarm_producers = []
        self._current_profile = None

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_restart_safely(self):
        self._lifecycle_test_impl(self._restart_safely)

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_restart_unsafely(self):
        self._lifecycle_test_impl(self._restart_unsafely)

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rolling_restarts(self):
        self._lifecycle_test_impl(self._rolling_restarts)

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommission_node_safely(self):
        self._lifecycle_test_impl(self._decommission_node_safely)

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommission_node_unsafely(self):
        self._lifecycle_test_impl(self._decommission_node_unsafely)

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_block_s3_on_random_node(self):
        self._target_port = 9000
        self._lifecycle_test_impl(self._isolate_all_nodes)

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_isolate_random_node_from_cluster(self):
        self._target_port = 33145
        self._lifecycle_test_impl(self._isolate_random_node)

    @cluster(num_nodes=16, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_isolate_random_node_from_clients(self):
        self._target_port = 9092
        self._lifecycle_test_impl(self._isolate_random_node)

    @cluster(num_nodes=12, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_many_topics_throughput(self):
        """Test creates 11950 topics, and uses client-swarm to
        generate 1000 messages with 5 msg/sec rate to each topic
        and validates high watermark values

        Returns:
            None
        """

        # Max number of partitions for i3en.xlarge
        # default settings is 1000 partitions per shard/cpu
        # (9 nodes × 4 vcpus/shards × 1000) / 3 replicas
        # 12000

        # Notes on messages params and BW calculations
        # default msg size is 16KB
        # 16k * 10 = ~150KB/s per single producer
        # 10k Producers generate 1.56GB/sec load to the cluster
        # Rate beyond 40 is unreachable in most cases
        # Example: 60 msg/sec on 10k topics
        # [2024-02-26T23:26:50Z INFO  client_swarm] Producer rates: [min=390095, max=682666, avg=506402] bytes/s
        # => 506402 / 16384 = ~30

        # Get profile for 6k topics
        # This should be enough for default tests with 12 nodes cluster
        tsm = TopicScaleProfileManager()
        profile = tsm.get_custom_profile("default", {"batch_size": 1024})

        # Start kafka
        self.redpanda.start()

        # Do create topics stage
        topic_prefixes, pnode_topic_count, cnode_topic_count = \
            self._stage_create_topics_adjusted(profile)

        # Do the healthcheck on RP
        # to make sure that all topics are settle down and have their leader
        self._wait_until_cluster_healthy()

        # Calculate how much time ideally needed for the producers to finish
        # Logic is that we sleep for normal running time
        # And then running checks when swarm nodes running last messages delivery
        running_time_sec = \
            profile.message_count // profile.messages_per_second_per_producer

        # Run swarm producers
        swarm_producers = self._run_producers_with_constant_rate(
            profile, pnode_topic_count, topic_prefixes)

        # Run swarm consumers
        _group = "topic_swarm_group"
        swarm_consumers = self._run_consumers_with_constant_rate(
            profile, cnode_topic_count, topic_prefixes, _group)

        # Wait for all messages to be produced
        # Logic is that we sleep for normal running time
        # And then running checks when swarm nodes running last messages delivery
        running_time_sec = \
            profile.message_count // profile.messages_per_second_per_producer
        self.logger.info(f"Sleeping for {running_time_sec} sec (running time)")
        # Just pause for running time to eliminate unnesesary requests
        # and not put noise into already overloaded network traffic
        time.sleep(running_time_sec)

        # Run checks if swarm nodes finished
        self.logger.info("Make sure that swarm node producers are finished")
        for s in swarm_producers:
            # account for up to one-third delays
            s.wait(running_time_sec * 2)
        self.logger.info("Make sure that swarm node consumers are finished")
        for s in swarm_consumers:
            # account for up to one-third delays
            s.wait(running_time_sec * 2)

        self.logger.info("Calculating high watermarks for all topics")

        # Topic hwm getter
        def _get_hwm(topic):
            _hwm = 0
            for partition in self.rpk.describe_topic(topic):
                # Add currect high watermark for topic
                _hwm += partition.high_watermark
            return _hwm

        # Validate high watermark
        target_messages_per_node = profile.message_count * pnode_topic_count
        hwms = []
        for topic_prefix in topic_prefixes:
            # messages per node
            _topic_names = [
                f"{topic_prefix}-{idx}" for idx in range(pnode_topic_count)
            ]
            # Use Thread pool to speed things up
            with concurrent.futures.ThreadPoolExecutor(max_workers=32) as exec:
                swarmnode_hwms = sum(exec.map(_get_hwm, _topic_names))
            # save watermark for node
            hwms.append(swarmnode_hwms)

        assert all([hwm >= target_messages_per_node for hwm in hwms]), \
            f"Message counts per swarm node mismatch: " \
            f"target={target_messages_per_node}, " \
            f"swarm_nodes='''{', '.join([str(num) for num in hwms])}'''"

        return

    @cluster(num_nodes=11)
    def test_many_topics_config(self):
        """Test how Redpanda behaves when attempting to describe the configs
        of all the topics in a 40k topic cluster and how it behaves when altering
        the configs of said topics
        """
        tsm = TopicScaleProfileManager()
        profile = tsm.get_profile("topic_profile_t40k_p1")

        # Start kafka
        self.redpanda.start()

        topic_details = self._stage_create_topics(profile)
        self.logger.debug(f'topic_detals: {topic_details}')

        client = KafkaCliTools(self.redpanda)
        try:
            client.describe_topics()
        except subprocess.CalledProcessError:
            # This is expected - the kafka CLI tools will run out of
            # heap space on such a large request.
            pass

    def _request(self,
                 verb,
                 path,
                 hostname=None,
                 tls_enabled: bool = False,
                 **kwargs):
        """

        :param verb: String, as for first arg to requests.request
        :param path: URI path without leading slash
        :param timeout: Optional requests timeout in seconds
        :return:
        """

        if hostname is None:
            # Pick hostname once: we will retry the same place we got an error,
            # to avoid silently skipping hosts that are persistently broken
            nodes = [n for n in self.redpanda.nodes]
            random.shuffle(nodes)
            node = nodes[0]
            hostname = node.account.hostname

        scheme = "https" if tls_enabled else "http"
        uri = f"{scheme}://{hostname}:8081/{path}"

        if 'timeout' not in kwargs:
            kwargs['timeout'] = 60

        # Error codes that may appear during normal API operation, do not
        # indicate an issue with the service
        acceptable_errors = {409, 422, 404}

        def accept_response(resp):
            return 200 <= resp.status_code < 300 or resp.status_code in acceptable_errors

        self.logger.debug(f"{verb} hostname={hostname} {path} {kwargs}")

        # This is not a retry loop: you get *one* retry to handle issues
        # during startup, after that a failure is a failure.
        r = requests.request(verb, uri, **kwargs)
        if not accept_response(r):
            self.logger.info(
                f"Retrying for error {r.status_code} on {verb} {path} ({r.text})"
            )
            time.sleep(10)
            r = requests.request(verb, uri, **kwargs)
            if accept_response(r):
                self.logger.info(
                    f"OK after retry {r.status_code} on {verb} {path} ({r.text})"
                )
            else:
                self.logger.info(
                    f"Error after retry {r.status_code} on {verb} {path} ({r.text})"
                )

        self.logger.info(
            f"{r.status_code} {verb} hostname={hostname} {path} {kwargs}")

        return r

    def _post_subjects_subject_versions(self,
                                        subject,
                                        data,
                                        headers=HTTP_POST_HEADERS,
                                        **kwargs):
        return self._request("POST",
                             f"subjects/{subject}/versions",
                             headers=headers,
                             data=data,
                             **kwargs)

    def _get_subjects(self, deleted=False, headers=HTTP_GET_HEADERS, **kwargs):
        return self._request("GET",
                             f"subjects{'?deleted=true' if deleted else ''}",
                             headers=headers,
                             **kwargs)

    def _get_schemas_ids_id_versions(self,
                                     id,
                                     headers=HTTP_GET_HEADERS,
                                     **kwargs):
        return self._request("GET",
                             f"schemas/ids/{id}/versions",
                             headers=headers,
                             **kwargs)

    def _get_schemas_ids_id_subjects(self,
                                     id,
                                     deleted=False,
                                     headers=HTTP_GET_HEADERS,
                                     **kwargs):
        return self._request(
            "GET",
            f"schemas/ids/{id}/subjects{'?deleted=true' if deleted else ''}",
            headers=headers,
            **kwargs)

    @cluster(num_nodes=10)
    def test_many_subjects(self):
        num_subjects = 40000
        self.redpanda.start()

        schema1_def = '{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
        schema1_data = json.dumps({"schema": schema1_def})

        id = None

        for i in range(num_subjects):
            subject_name = f'subject{i}'
            result_raw = self._post_subjects_subject_versions(
                subject=subject_name, data=schema1_data)
            assert result_raw.status_code == requests.codes.ok, f'Failed to post {subject_name}: {result_raw.status_code}'
            if id is None:
                id = result_raw.json()["id"]
            else:
                assert id == result_raw.json(
                )["id"], f'Id mismatch: {id} != {result_raw.json()["id"]}'

        result_raw = self._get_subjects()
        assert result_raw.status_code == requests.codes.ok, f'Failed to get subjects: {result_raw.status_code}'
        assert len(
            result_raw.json()
        ) == num_subjects, f'Length of json ({len(result_raw.json())}) != expected {num_subjects}'

        result_raw = self._get_schemas_ids_id_subjects(id=id)
        assert result_raw.status_code == requests.codes.ok, f'Failed to get subjects by id: {result_raw.status_code}'
        assert len(
            result_raw.json()
        ) == num_subjects, f'Length of json ({len(result_raw.json())}) != expected {num_subjects}'

        result_raw = self._get_schemas_ids_id_versions(id=id)
        assert result_raw.status_code == requests.codes.ok, f'Failed to get subject versions by id: {result_raw.status_code}'
        assert len(
            result_raw.json()
        ) == num_subjects, f'Length of json ({len(result_raw.json())}) != expected {num_subjects}'

    @cluster(num_nodes=10)
    def test_many_schemas(self):
        num_schemas = 40000
        self.redpanda.start()

        schema_fmt = '{{"type":"record","name":"{subject}","fields":[{{"name":"{subject}","type":"string"}}]}}'

        prev_id = None
        for i in range(num_schemas):
            subject = f'subject{i}'
            schema_data = json.dumps(
                {"schema": schema_fmt.format(subject=subject)})
            result_raw = self._post_subjects_subject_versions(subject=subject,
                                                              data=schema_data)
            assert result_raw.status_code == requests.codes.ok, f'Failed to post {subject}: {result_raw.status_code}'
            if prev_id is None:
                prev_id = result_raw.json()["id"]
            else:
                assert prev_id != result_raw.json(
                )["id"], f'Expected different schema ID: {prev_id}'
                prev_id = result_raw.json()["id"]

        result_raw = self._get_subjects()
        assert result_raw.status_code == requests.codes.ok, f'Failed to get subjects: {result_raw.status_code}'
        assert len(
            result_raw.json()
        ) == num_schemas, f'Length of json ({len(result_raw.json())}) != expected {num_schemas}'
