# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random

from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.cluster import cluster
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer, KgoVerifierRandomConsumer
from rptest.services.redpanda import SISettings
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.kafka_cli_tools import KafkaCliTools
from ducktape.utils.util import wait_until, TimeoutError


class HardRestartTest(PreallocNodesTest):
    small_segment_size = 1024 * 1024

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "election_timeout_ms": 15000,
        }
        si_settings = SISettings(test_context,
                                 log_segment_size=self.small_segment_size)
        super(HardRestartTest, self).__init__(test_context=test_context,
                                              num_brokers=3,
                                              node_prealloc_count=1,
                                              si_settings=si_settings,
                                              extra_rp_conf=extra_rp_conf)
        self.rpk = RpkTool(self.redpanda)

    def elections_done(self, topic_name, num_partitions):
        try:
            partitions = list(
                self.rpk.describe_topic(topic_name, tolerant=True))
        except RpkException:
            return False
        if len(partitions) < num_partitions:
            self.logger.info(
                f"describe only includes {len(partitions)} partitions for {topic_name}"
            )
            self.logger.debug(
                f"waiting for {num_partitions} partitions: {partitions}")
            return False
        return True

    @cluster(num_nodes=4)
    def test_restart_during_truncations(self):
        """
        - write to a given partition
        - kill the leader of the partition
        - restart the node briefly, allowing for some truncations
        - kill the node mid-truncation
        - restart the node and make sure all nodes can become available
        """
        topic_name = "tapioca"
        num_partitions = 5
        config = {
            "segment.bytes": self.small_segment_size,
            "retention.bytes": -1,
        }
        self.rpk.create_topic(topic_name,
                              partitions=num_partitions,
                              replicas=3,
                              config=config)
        target_node = self.redpanda.nodes[0]
        wait_until(lambda: self.elections_done(topic_name, num_partitions),
                   timeout_sec=30,
                   backoff_sec=1)

        def move_leadership_to_node(node):
            node_id = self.redpanda.node_id(node)
            for i in range(num_partitions):
                self.redpanda._admin.transfer_leadership_to(namespace="kafka",
                                                            topic=topic_name,
                                                            partition=i,
                                                            target_id=node_id)
            wait_until(lambda: self.elections_done(topic_name, num_partitions),
                       timeout_sec=30,
                       backoff_sec=1)

        move_leadership_to_node(target_node)

        msg_size = 256 * 1024
        # Write enough such that we have more than one segment per partition.
        # ~3K msgs ~= 96MiB
        msg_count = int(
            (num_partitions * self.small_segment_size * 3) / msg_size)

        def num_segments_on_target_node():
            """
            Returns a list of the number of segments per partition id at the
            list index.
            """
            node_storage = self.redpanda.node_storage(target_node)
            num_segments_per_partition = []
            for i in range(num_partitions):
                num_segments_per_partition.append(
                    len(node_storage.segments("kafka", topic_name, i)))
            return num_segments_per_partition

        kafka_cli = KafkaCliTools(self.redpanda)
        with FailureInjector(self.redpanda) as injector:
            for _ in range(msg_count):
                kafka_cli.produce(topic_name,
                                  num_records=num_partitions,
                                  record_size=msg_size,
                                  acks=1)

            # Isolate the leader so it can attempt replication to other nodes
            # and fail. This allows some records to only go into the target
            # node, teeing up for truncation.
            injector.inject_failure(
                FailureSpec(FailureSpec.FAILURE_ISOLATE, target_node))
            initial_segment_counts = num_segments_on_target_node()

            def all_partitions_add_segments():
                """
                Returns true if the number of segments of every partition in
                the target node has increased since the above call to
                num_segments_on_target_node().
                """
                kafka_cli.produce(topic_name,
                                  num_records=10 * num_partitions,
                                  record_size=msg_size,
                                  acks=1)
                new_counts = num_segments_on_target_node()
                for i in range(num_partitions):
                    if initial_segment_counts[i] >= new_counts[i]:
                        self.logger.info(
                            f"{initial_segment_counts} vs {new_counts}")
                        return False
                return True

            wait_until(all_partitions_add_segments,
                       timeout_sec=60,
                       backoff_sec=0.05)

        # Stop isolating the node and restart the target node. Elect a leader
        # on the other nodes and continue replicating in a new term.
        self.redpanda.stop_node(target_node)
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size,
            msg_count,
            custom_node=[self.preallocated_nodes[0]])
        try:
            producer.start()

            # Restart the target node. It should attempt to truncate, but send
            # a SIGKILL to exercise crash consistency.
            while True:
                try:
                    self.redpanda.start_node(target_node)
                    break
                except:
                    pass
            self.redpanda._admin.busy_loop_start(
                target_node,
                min_spins_per_scheduling_point=100000,
                max_spins_per_scheduling_point=1000000,
                num_fibers=30)
            time.sleep(random.randint(0, 10 * 1000) / 1000)
            self.redpanda.stop_node(target_node, forced=True)
        finally:
            producer.stop()
            producer.wait(timeout_sec=300)
            producer.clean()

        # Do another hard restart for good measure.
        while True:
            try:
                self.redpanda.start_node(target_node)
                break
            except:
                pass
        time.sleep(random.randint(0, 10 * 1000) / 1000)
        self.redpanda.stop_node(target_node, forced=True)
        while True:
            try:
                self.redpanda.start_node(target_node)
                break
            except:
                pass

        # Does the server get up without errors?
        wait_until(self.redpanda.healthy, timeout_sec=30, backoff_sec=1)
        assert self.redpanda.search_log_any("failed to create") is False
