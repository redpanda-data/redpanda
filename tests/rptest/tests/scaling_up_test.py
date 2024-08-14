# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
import random, math, time
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierSeqConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings
import concurrent

from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import skip_debug_mode


class ScalingUpTest(PreallocNodesTest):
    def __init__(self, test_context):
        super().__init__(
            test_context,
            num_brokers=6,
            extra_rp_conf={
                "group_topic_partitions": self.group_topic_partitions,
                "partition_autobalancing_mode": "node_add"
            },
            node_prealloc_count=1,
        )
        self.redpanda.set_skip_if_no_redpanda_log(True)

    def setup(self):
        # defer starting Redpanda
        pass

    """
    Adding nodes to the cluster should result in partition reallocations to new
    nodes
    """
    rebalance_timeout = 240
    group_topic_partitions = 16

    def _replicas_per_node(self):
        kafkacat = KafkaCat(self.redpanda)
        node_replicas = {}
        md = kafkacat.metadata()
        self.redpanda.logger.debug(f"metadata: {md}")
        for topic in md['topics']:
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    if id not in node_replicas:
                        node_replicas[id] = 0
                    node_replicas[id] += 1

        return node_replicas

    # Returns (count of replicas)[allocation_domain][node]
    def _replicas_per_domain_node(self):
        kafkacat = KafkaCat(self.redpanda)
        replicas = {}
        md = kafkacat.metadata()
        self.redpanda.logger.debug(f"metadata: {md}")
        for topic in md['topics']:
            domain = -1 if topic['topic'] == '__consumer_offsets' else 0
            in_domain = replicas.setdefault(domain, {})
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    in_domain.setdefault(id, 0)
                    in_domain[id] += 1
        return replicas

    def _topic_replicas_per_node(self):
        kafkacat = KafkaCat(self.redpanda)
        topic_replicas = defaultdict(lambda: defaultdict(int))
        md = kafkacat.metadata()
        self.redpanda.logger.debug(f"metadata: {md}")
        for topic in md['topics']:
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    topic_replicas[topic['topic']][id] += 1

        return topic_replicas

    def wait_for_partitions_rebalanced(self, total_replicas, timeout_sec):
        def partitions_rebalanced():
            per_domain_node = self._replicas_per_domain_node()
            self.redpanda.logger.info(
                f"replicas per domain per node: "
                f"{dict([(k,dict(sorted(v.items()))) for k,v in sorted(per_domain_node.items())])}"
            )
            per_node = {}

            # make sure # of replicas is level within each domain separately
            for domain, in_domain in per_domain_node.items():
                # rule out the perfect distribution first
                if max(in_domain.values()) - min(in_domain.values()) > 1:
                    # judge nonperfect ones by falling into the ±20%
                    # tolerance range
                    expected_per_node = sum(in_domain.values()) / len(
                        self.redpanda.started_nodes())
                    expected_range = [
                        math.floor(0.8 * expected_per_node),
                        math.ceil(1.2 * expected_per_node)
                    ]
                    if not all(expected_range[0] <= p[1] <= expected_range[1]
                               for p in in_domain.items()):
                        self.redpanda.logger.debug(
                            f"In domain {domain}, not all nodes' partition counts "
                            f"fall within the expected range {expected_range}. "
                            f"Nodes: {len(self.redpanda.started_nodes())}")
                        return False

                for n in in_domain:
                    per_node[n] = per_node.get(n, 0) + in_domain[n]

            self.redpanda.logger.debug(
                f"replicas per node: {dict(sorted(per_node.items()))}")
            if len(per_node) < len(self.redpanda.started_nodes()):
                return False
            if sum(per_node.values()) != total_replicas:
                return False

            # make sure that all reconfigurations are finished
            admin = Admin(self.redpanda)
            return len(admin.list_reconfigurations()) == 0

        wait_until(partitions_rebalanced,
                   timeout_sec=timeout_sec,
                   backoff_sec=1)

    def create_topics(self, rf, partition_count):
        total_replicas = 0
        topics = []
        for _ in range(1, 5):
            partitions = partition_count
            spec = TopicSpec(partition_count=partition_count,
                             replication_factor=rf)
            total_replicas += partitions * rf
            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        self._topic = random.choice(topics).name

        return total_replicas

    @property
    def producer_throughput(self):
        return 5 * (1024 * 1024) if not self.debug_mode else 1000

    @property
    def msg_count(self):
        return 20 * int(self.producer_throughput / self.msg_size)

    @property
    def msg_size(self):
        return 128

    def start_producer(self):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            self.msg_count,
            custom_node=self.preallocated_nodes,
            rate_limit_bps=self.producer_throughput)

        self.producer.start(clean=False)
        self.producer.wait_for_acks(
            5 * (self.producer_throughput / self.msg_size), 120, 1)

    def start_consumer(self):
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            readers=1,
            nodes=self.preallocated_nodes)

        self.consumer.start(clean=False)

    def verify(self):
        self.logger.info(
            f"verifying workload: topic: {self._topic}, with [rate_limit: {self.producer_throughput}, message size: {self.msg_size}, message count: {self.msg_count}]"
        )
        self.producer.wait()

        # Await the consumer that is reading only the subset of data that
        # was written before it started.
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
        del self.consumer

        # Start a new consumer to read all data written
        self.start_consumer()
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"

    @cluster(num_nodes=7)
    @matrix(partition_count=[1, 20])
    def test_adding_nodes_to_cluster(self, partition_count):
        # The test implicitly assumes that all internal topics are rf=1
        # As we add more nodes ensure that the health manager does not
        # upreplicate the topics and violate the assumption
        self.redpanda.add_extra_rp_conf(
            {"internal_topic_replication_factor": 1})
        # start single node cluster
        self.redpanda.start(nodes=[self.redpanda.nodes[0]])
        # create some topics
        total_replicas = self.create_topics(rf=1,
                                            partition_count=partition_count)
        # include __consumer_offsets topic replica
        total_replicas += self.group_topic_partitions

        self.start_producer()
        self.start_consumer()

        # add second node
        self.redpanda.start_node(self.redpanda.nodes[1])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)
        # add third node
        self.redpanda.start_node(self.redpanda.nodes[2])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        self.verify()

    @cluster(num_nodes=7)
    @matrix(partition_count=[1, 20])
    def test_adding_multiple_nodes_to_the_cluster(self, partition_count):

        # start single node cluster
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        # create some topics
        topics = []
        total_replicas = self.create_topics(rf=3,
                                            partition_count=partition_count)
        # add consumer group topic replicas
        total_replicas += self.group_topic_partitions * 3

        self.start_producer()
        self.start_consumer()

        # add three nodes at once
        for n in self.redpanda.nodes[3:]:
            self.redpanda.clean_node(n)
            self.redpanda.start_node(n)

        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

    @cluster(num_nodes=7)
    @matrix(partition_count=[1, 20])
    def test_on_demand_rebalancing(self, partition_count):
        # start single node cluster
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        self.redpanda.set_cluster_config(
            {"partition_autobalancing_mode": "off"})
        # create some topics
        total_replicas = self.create_topics(rf=3,
                                            partition_count=partition_count)
        # add consumer group topic replicas
        total_replicas += self.group_topic_partitions * 3

        self.start_producer()
        self.start_consumer()

        # add three nodes
        for n in self.redpanda.nodes[3:]:
            self.redpanda.clean_node(n)
            self.redpanda.start_node(n)

        # verify that all new nodes are empty

        per_node = self._replicas_per_node()

        assert len(per_node) == 3

        # trigger rebalance
        admin = Admin(self.redpanda, retries_amount=20)
        admin.trigger_rebalance()

        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)
        self.verify()

    @cluster(num_nodes=7)
    def test_topic_hot_spots(self):
        # start 3 nodes cluster
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        # create some topics
        total_replicas = 0
        topics = []
        for _ in range(1, 5):
            partitions = 30
            spec = TopicSpec(partition_count=partitions, replication_factor=3)
            total_replicas += partitions * 3
            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        self._topic = random.choice(topics).name

        # include __consumer_offsets topic replica
        total_replicas += self.group_topic_partitions * 3

        self.start_producer()
        self.start_consumer()

        # add second node
        self.redpanda.start_node(self.redpanda.nodes[3])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)
        # add third node
        self.redpanda.start_node(self.redpanda.nodes[4])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        self.verify()

        topic_per_node = self._topic_replicas_per_node()
        for t, nodes in topic_per_node.items():
            self.logger.info(f"{t} spans {len(nodes)} nodes")
            # assert that each topic has replicas on all of the nodes
            assert len(nodes) == len(self.redpanda.started_nodes())

    @cluster(num_nodes=7)
    def test_adding_node_with_unavailable_node(self):
        # start 3 nodes first
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        self.redpanda.set_cluster_config(
            {"partition_autobalancing_mode": "off"})
        # create some topics
        total_replicas = self.create_topics(rf=3, partition_count=20)
        # add consumer group topic replicas
        total_replicas += self.group_topic_partitions * 3

        self.start_producer()
        self.start_consumer()

        # start a node just to register it (no partitions will be moved)
        unavailable_node = self.redpanda.nodes[3]
        self.redpanda.start_node(unavailable_node)
        self.redpanda.stop_node(unavailable_node)
        time.sleep(5.0)  # let the node become unavailable

        admin = Admin(self.redpanda, default_node=self.redpanda.nodes[0])
        admin.patch_cluster_config(
            upsert={"partition_autobalancing_mode": "node_add"})

        added_node = self.redpanda.nodes[4]
        self.redpanda.start_node(added_node)
        added_node_id = self.redpanda.node_id(added_node)

        def initial_rebalance_finished():
            reconfigurations_len = len(admin.list_reconfigurations())
            replicas_on_added = self._replicas_per_node().get(added_node_id, 0)
            self.logger.info(f"waiting for initial rebalance: "
                             f"{reconfigurations_len=}, {replicas_on_added=}")
            return reconfigurations_len == 0 and replicas_on_added > 0

        wait_until(initial_rebalance_finished,
                   timeout_sec=self.rebalance_timeout,
                   backoff_sec=1,
                   err_msg="initial rebalance failed")

        self.redpanda.start_node(unavailable_node)
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)
        self.verify()

    def _kafka_usage(self, nodes):
        usage_per_node = {}
        for n in nodes:
            id = self.redpanda.node_id(n)
            disk_usage = self.redpanda.data_dir_usage("kafka", n)
            usage_per_node[id] = disk_usage

        return usage_per_node

    def _partition_sizes(self, topic, nodes):
        sizes = defaultdict(int)
        metrics = self.redpanda.metrics_sample("partition_size", nodes=nodes)
        if not metrics:
            return {}

        for s in metrics.samples:
            if s.labels['topic'] == topic:
                sizes[self.redpanda.node_id(s.node)] += s.value
        return sizes

    @skip_debug_mode
    @cluster(num_nodes=7)
    def test_fast_node_addition(self):

        log_segment_size = 2 * 1024 * 1024
        total_segments_per_partition = 20
        partition_cnt = 40
        msg_size = 16 * 1024  # 16 KiB
        data_size = log_segment_size * total_segments_per_partition * partition_cnt
        msg_cnt = data_size // msg_size

        # configure and start redpanda
        extra_rp_conf = {
            'cloud_storage_segment_max_upload_interval_sec': 10,
            'cloud_storage_manifest_max_upload_interval_sec': 10,
            # setup initial retention target to 1 segment
            'initial_retention_local_target_bytes_default': log_segment_size,
        }
        # shadow indexing is required when we want to leverage fast partition movements
        si_settings = SISettings(test_context=self.test_context,
                                 log_segment_size=log_segment_size,
                                 retention_local_strict=False)

        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start(nodes=self.redpanda.nodes[0:4])
        topic = TopicSpec(replication_factor=3,
                          partition_count=partition_cnt,
                          redpanda_remote_write=True,
                          redpanda_remote_read=True)

        total_replicas = 3 * partition_cnt

        self.client().create_topic(topic)
        self.logger.info(
            f"Producing {data_size} bytes of data in {msg_cnt} total messages")
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=msg_size,
            msg_count=msg_cnt,
            custom_node=self.preallocated_nodes)
        self.producer.start()
        self.producer.wait()

        # add fifth node
        self.redpanda.start_node(self.redpanda.nodes[4])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        def print_disk_usage(usage):
            for n, b in usage.items():
                self.logger.info(
                    f"node: {n} total partitions size: {b/(1024*1024)} Mb")

        def verify_disk_usage(usage: dict, added_ids: list, percentage: float):
            old_nodes_usage = [
                b for id, b in usage.items() if id not in added_ids
            ]
            avg_usage = sum(old_nodes_usage) / len(old_nodes_usage)

            for id in added_ids:
                added_node_usage = usage[id]
                assert added_node_usage < percentage * avg_usage, \
                f"Added node {id} disk usage {added_node_usage} is too large, "
                f"expected usage to be smaller than {percentage * avg_usage} bytes"

        usage = self._kafka_usage(nodes=self.redpanda.nodes[0:5])
        print_disk_usage(usage)

        verify_disk_usage(usage,
                          [self.redpanda.node_id(self.redpanda.nodes[4])], 0.3)

        # add sixth node
        self.redpanda.start_node(self.redpanda.nodes[5])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        usage = self._kafka_usage(self.redpanda.nodes)
        print_disk_usage(usage)
        verify_disk_usage(usage, [
            self.redpanda.node_id(self.redpanda.nodes[4]),
            self.redpanda.node_id(self.redpanda.nodes[5])
        ], 0.3)
        # verify that data can be read
        self.consumer = KgoVerifierSeqConsumer(self.test_context,
                                               self.redpanda,
                                               topic.name,
                                               msg_size,
                                               nodes=self.preallocated_nodes)

        self.consumer.start(clean=False)
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, \
        f"Invalid reads in topic: {topic.name}, invalid reads count: "
        "{self.consumer.consumer_status.validator.invalid_reads}"

    @skip_debug_mode
    @cluster(num_nodes=7)
    @matrix(use_topic_property=[True, False])
    def test_moves_with_local_retention(self, use_topic_property):

        log_segment_size = 2 * 1024 * 1024
        total_segments_per_partition = 40
        partition_cnt = 20
        msg_size = 16 * 1024  # 16 KiB
        data_size = log_segment_size * total_segments_per_partition * partition_cnt
        msg_cnt = data_size // msg_size

        # configure and start redpanda
        extra_rp_conf = {
            'cloud_storage_segment_max_upload_interval_sec': 10,
            'cloud_storage_manifest_max_upload_interval_sec': 10,
        }
        # shadow indexing is required when we want to leverage fast partition movements
        si_settings = SISettings(test_context=self.test_context,
                                 log_segment_size=log_segment_size,
                                 retention_local_strict=False)

        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start(nodes=self.redpanda.nodes[0:4])
        topic = TopicSpec(replication_factor=3,
                          partition_count=partition_cnt,
                          redpanda_remote_write=True,
                          redpanda_remote_read=True)

        total_replicas = 3 * partition_cnt

        self.client().create_topic(topic)
        requested_local_retention = log_segment_size * 15

        if use_topic_property:
            self.client().alter_topic_config(
                topic.name, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                requested_local_retention)
        else:
            self.redpanda.set_cluster_config({
                "retention_local_target_bytes_default":
                requested_local_retention
            })

        self.logger.info(
            f"Producing {data_size} bytes of data in {msg_cnt} total messages")
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=msg_size,
            msg_count=msg_cnt,
            custom_node=self.preallocated_nodes)
        self.producer.start()
        self.producer.wait()

        # add fifth node
        self.redpanda.start_node(self.redpanda.nodes[4])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        def print_disk_usage(usage):
            for n, b in usage.items():
                self.logger.info(
                    f"node: {n} total partitions size: {b/(1024*1024):02} Mb")

        def disk_usage_correct(nodes, node_id):
            size_per_node = self._partition_sizes(topic.name, nodes)
            print_disk_usage(size_per_node)
            replicas_per_node = self._topic_replicas_per_node()
            node_replicas = replicas_per_node[topic.name][node_id]

            target_size = node_replicas * requested_local_retention

            current_usage = size_per_node[node_id]
            tolerance = 0.2
            max = target_size * (1.0 + tolerance)
            min = target_size * (1.0 - tolerance)
            self.logger.info(
                f"node {node_id} target size: {target_size}, current size: {size_per_node[node_id]}, expected range ({min}, {max})"
            )
            return current_usage > min and current_usage < max

        first_new_id = self.redpanda.node_id(self.redpanda.nodes[4])

        wait_until(
            lambda: disk_usage_correct(self.redpanda.nodes[0:5], first_new_id),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Timeout waiting for correct disk usage to be reported")

        # add sixth node
        self.redpanda.start_node(self.redpanda.nodes[5])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        next_new_id = self.redpanda.node_id(self.redpanda.nodes[5])

        wait_until(
            lambda: disk_usage_correct(self.redpanda.nodes, next_new_id),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Timeout waiting for correct disk usage to be reported")
        # verify that data can be read
        self.consumer = KgoVerifierSeqConsumer(self.test_context,
                                               self.redpanda,
                                               topic.name,
                                               msg_size,
                                               nodes=self.preallocated_nodes)

        self.consumer.start(clean=False)
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, \
        f"Invalid reads in topic: {topic.name}, invalid reads count: "
        "{self.consumer.consumer_status.validator.invalid_reads}"
