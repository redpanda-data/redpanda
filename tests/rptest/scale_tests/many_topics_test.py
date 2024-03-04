# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import threading
import json
import time
import random
import sys
import concurrent.futures
import numpy

from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

from confluent_kafka import KafkaError, KafkaException

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, LoggingConfig, MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.node_operations import NodeOpsExecutor
from rptest.util import inject_remote_script
from rptest.scale_tests.topic_scale_profiles import TopicScaleProfileManager
from rptest.clients.python_librdkafka import PythonLibrdkafka


class ManyTopicsTest(RedpandaTest):

    LEADER_BALANCER_PERIOD_MS = 60 * 1_000  # 60s

    # Max time to wait for the cluster to be healthy once more.
    HEALTHY_WAIT_SECONDS = 20 * 60

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
            **kwargs)

        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.thread_local = threading.Lock()
        self.node_ops_exec = NodeOpsExecutor(
            self.redpanda,
            self.logger,
            self.thread_local,
            progress_timeout=self.HEALTHY_WAIT_SECONDS)

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

    def _wait_until_cluster_healthy(self):
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
            return unavailable_count == 0 and under_replicated_count == 0

        wait_until(
            lambda: is_healthy(),
            timeout_sec=self.HEALTHY_WAIT_SECONDS,
            backoff_sec=30,
            err_msg=f"couldn't reach under-replicated count target: {0}")

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
        self.logger.debug(
            f"Adding node {self._standby_broker.name} to the cluster")
        self.redpanda.clean_node(self._standby_broker)
        self.redpanda.start_node(self._standby_broker,
                                 first_start=True,
                                 auto_assign_node_id=True)
        wait_until(lambda: self.redpanda.registered(self._standby_broker),
                   timeout_sec=60,
                   backoff_sec=5)

        # select a node at random from the current broker set to decom.
        self.logger.debug(f"Decommissioning node {node_to_decom.name}")
        node_to_decom_idx = self.redpanda.idx(node_to_decom)
        node_to_decom_id = self.redpanda.node_id(node_to_decom)
        self.node_ops_exec.decommission(node_to_decom_idx)
        self.node_ops_exec.wait_for_removed(node_to_decom_id)
        self.node_ops_exec.stop_node(node_to_decom_idx)

        self._standby_broker = node_to_decom

    @skip_debug_mode
    @cluster(num_nodes=11, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommission_safely(self):
        self._start_initial_broker_set()

        tsm = TopicScaleProfileManager()
        profile = tsm.get_profile("topic_profile_t40k_p1")

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
