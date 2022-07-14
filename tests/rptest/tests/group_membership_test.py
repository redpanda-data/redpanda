# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import requests
import random

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.rpk_producer import RpkProducer


class ListGroupsReplicationFactorTest(RedpandaTest):
    """
    We encountered an issue where listing groups would return a
    coordinator-loading error when the underlying group membership topic had a
    replication factor of 3 (we had not noticed this until we noticed that
    replication factor were defaulted to 1). it isn't clear if this is specific
    to `kcl` but that is the client that we encountered the issue with.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        extra_rp_conf = dict(default_topic_replications=3, )

        super(ListGroupsReplicationFactorTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_list_groups(self):
        kcl = KCL(self.redpanda)
        kcl.produce(self.topic, "msg\n")
        kcl.consume(self.topic, n=1, group="g0")
        kcl.list_groups()
        out = kcl.list_groups()
        assert "COORDINATOR_LOAD_IN_PROGRESS" not in out

    def _transfer_with_retry(self, namespace, topic, partition, target_id):
        """
        Leadership transfers may return 503 if done in a tight loop, as
        the current leader might still be writing their configuration after
        winning their election.

        503 is safe status to retry.
        """
        admin = Admin(redpanda=self.redpanda)
        timeout = time.time() + 10

        while time.time() < timeout:
            try:
                admin.transfer_leadership_to(namespace=namespace,
                                             topic=topic,
                                             partition=partition,
                                             target_id=target_id)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    time.sleep(1)
                else:
                    raise
            else:
                break

    @cluster(num_nodes=3)
    def test_list_groups_has_no_duplicates(self):
        """
        Reproducer for:
        https://github.com/redpanda-data/redpanda/issues/2528
        """
        kcl = KCL(self.redpanda)
        kcl.produce(self.topic, "msg\n")
        # create 4 groups
        kcl.consume(self.topic, n=1, group="g0")
        kcl.consume(self.topic, n=1, group="g1")
        kcl.consume(self.topic, n=1, group="g2")
        kcl.consume(self.topic, n=1, group="g3")
        # transfer kafka/__consumer_offsets/0 leadership across the nodes to trigger
        # group state recovery on each node
        for n in self.redpanda.nodes:
            self._transfer_with_retry(namespace="kafka",
                                      topic="__consumer_offsets",
                                      partition=0,
                                      target_id=self.redpanda.idx(n))

        # assert that there are no duplicates in
        def _list_groups():
            out = kcl.list_groups()
            groups = []
            for l in out.splitlines():
                # skip header line
                if l.startswith("BROKER"):
                    continue
                parts = l.split()
                groups.append({'node_id': parts[0], 'group': parts[1]})
            return groups

        def _check_no_duplicates():
            groups = _list_groups()
            self.redpanda.logger.debug(f"groups: {groups}")
            return len(groups) == 4

        wait_until(_check_no_duplicates,
                   10,
                   backoff_sec=2,
                   err_msg="found persistent duplicates in groups listing")


class GroupMetricsTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1), TopicSpec(partition_count=3),
              TopicSpec(partition_count=3))

    def __init__(self, ctx, *args, **kwargs):

        # Require internal_kafka topic to have an increased replication factor
        extra_rp_conf = dict(default_topic_replications=3,
                             enable_leader_balancer=False,
                             group_topic_partitions=1)
        super(GroupMetricsTest, self).__init__(test_context=ctx,
                                               num_brokers=3,
                                               extra_rp_conf=extra_rp_conf)
        self._ctx = ctx

    def _get_offset_from_metrics(self, group):
        metric = self.redpanda.metrics_sample("kafka_group_offset")
        if metric is None:
            return None
        metric = metric.label_filter(dict(group=group))
        res = {}
        for sample in metric.samples:
            res[(sample.labels['topic'],
                 sample.labels['partition'])] = sample.value
        return res

    @cluster(num_nodes=3)
    def test_check_value(self):
        rpk = RpkTool(self.redpanda)
        topic = next(filter(lambda x: x.partition_count == 1,
                            self.topics)).name

        for i in range(100):
            payload = str(random.randint(0, 1000))
            offset = rpk.produce(topic, "", payload, timeout=5)

        group_1 = "g1"
        metric_key = (topic, "0")
        for i in range(10):
            rpk.consume(topic, group=group_1, n=10)
            metrics_offsets = self._get_offset_from_metrics(group_1)
            assert metric_key in metrics_offsets
            assert metrics_offsets[metric_key] == (i + 1) * 10

        group_2 = "g2"
        rpk.consume(topic, group=group_2, n=50)
        gr_2_metrics_offsets = self._get_offset_from_metrics(group_2)
        assert metric_key in gr_2_metrics_offsets
        assert gr_2_metrics_offsets[metric_key] == 50

        rpk.group_seek_to_group(group_1, group_2)
        gr_1_metrics_offsets = self._get_offset_from_metrics(group_1)
        assert metric_key in gr_1_metrics_offsets
        assert gr_1_metrics_offsets[metric_key] == 50

        rpk.group_seek_to(group_2, "start")
        gr_2_metrics_offsets = self._get_offset_from_metrics(group_2)
        assert metric_key in gr_2_metrics_offsets
        assert gr_2_metrics_offsets[metric_key] == 0

        self.client().delete_topic(topic)

        def metrics_gone():
            metrics_offsets = self._get_offset_from_metrics(group_1)
            return metrics_offsets is None

        wait_until(metrics_gone, timeout_sec=30, backoff_sec=5)

    @cluster(num_nodes=3)
    def test_multiple_topics_and_partitions(self):
        rpk = RpkTool(self.redpanda)
        topics = self.topics
        group = "g0"

        for i in range(100):
            payload = str(random.randint(0, 1000))
            for topic_spec in topics:
                for p in range(topic_spec.partition_count):
                    rpk.produce(topic_spec.name,
                                "",
                                payload,
                                partition=p,
                                timeout=5)

        for topic_spec in topics:
            rpk.consume(topic_spec.name,
                        group=group,
                        n=100 * topic_spec.partition_count)

        metrics_offsets = self._get_offset_from_metrics(group)

        partitions_amount = sum(map(lambda x: x.partition_count, topics))
        assert len(metrics_offsets) == partitions_amount
        for topic_spec in topics:
            for i in range(topic_spec.partition_count):
                assert (topic_spec.name, str(i)) in metrics_offsets
                assert metrics_offsets[(topic_spec.name, str(i))] == 100

    @cluster(num_nodes=3)
    def test_topic_recreation(self):
        rpk = RpkTool(self.redpanda)
        topic_spec = next(filter(lambda x: x.partition_count == 1,
                                 self.topics))
        topic = topic_spec.name
        group = "g0"

        def check_metric():
            if topic_spec.name not in rpk.list_topics():
                return False
            for i in range(100):
                payload = str(random.randint(0, 1000))
                offset = rpk.produce(topic, "", payload, timeout=5)
            metric_key = (topic, "0")
            rpk.consume(topic, group=group, n=50)
            metrics_offsets = self._get_offset_from_metrics(group)
            if metric_key not in metrics_offsets:
                return False
            if metrics_offsets[metric_key] <= 0:
                return False
            return True

        wait_until(lambda: check_metric(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Initial check metric failed")

        self.client().delete_topic(topic)
        wait_until(lambda: self._get_offset_from_metrics(group) is None,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Topic metrics haven't dissapeared")

        self.client().create_topic(topic_spec)
        wait_until(lambda: check_metric(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Topic recreation check metric failed")

    @cluster(num_nodes=7)
    def test_leadership_transfer(self):
        topics = list(filter(lambda x: x.partition_count > 1, self.topics))
        group = "g0"

        producers = []
        for topic in topics:
            producer = RpkProducer(self._ctx,
                                   self.redpanda,
                                   topic.name,
                                   msg_size=5,
                                   msg_count=1000)
            producer.start()
            producers.append(producer)

        consumers = []
        for topic in topics:
            consumer = RpkConsumer(self._ctx,
                                   self.redpanda,
                                   topic.name,
                                   group=group)
            consumer.start()
            consumers.append(consumer)

        # Wait until cluster starts producing metrics
        wait_until(
            lambda: self.redpanda.metrics_sample("kafka_group_offset") != None,
            timeout_sec=30,
            backoff_sec=5)

        admin = Admin(redpanda=self.redpanda)

        def get_group_partition():
            return admin.get_partitions(namespace="kafka",
                                        topic="__consumer_offsets",
                                        partition=0)

        def get_group_leader():
            return get_group_partition()['leader_id']

        def metrics_from_single_node(node):
            """
            Check that metrics are produced only by the given node.
            """
            metrics = self.redpanda.metrics_sample("kafka_group_offset")
            if not metrics:
                self.logger.debug("No metrics found")
                return False
            metrics = metrics.label_filter(dict(group=group)).samples
            for metric in metrics:
                self.logger.debug(
                    f"Retrieved metric from node={metric.node.account.hostname}: {metric}"
                )
            return all([
                metric.node.account.hostname == node.account.hostname
                for metric in metrics
            ])

        def transfer_leadership(new_leader):
            """
            Request leadership transfer of the internal consumer group partition
            and check that it completes successfully.
            """
            self.logger.debug(
                f"Transferring leadership to {new_leader.account.hostname}")
            admin.transfer_leadership_to(
                namespace="kafka",
                topic="__consumer_offsets",
                partition=0,
                target_id=self.redpanda.idx(new_leader))
            for _ in range(3):  # re-check a few times
                leader = get_group_leader()
                self.logger.debug(f"Current leader: {leader}")
                if leader != -1 and self.redpanda.get_node(
                        leader) == new_leader:
                    return True
                time.sleep(1)
            return False

        def partition_ready():
            """
            All replicas present and known leader
            """
            partition = get_group_partition()
            self.logger.debug(f"XXXXX: {partition}")
            return len(
                partition['replicas']) == 3 and partition['leader_id'] >= 0

        def select_next_leader():
            """
            Select a leader different than the current leader
            """
            wait_until(partition_ready, timeout_sec=30, backoff_sec=5)
            partition = get_group_partition()
            replicas = partition['replicas']
            assert len(replicas) == 3
            leader = partition['leader_id']
            assert leader >= 0
            replicas = filter(lambda r: r["node_id"] != leader, replicas)
            new_leader = random.choice(list(replicas))['node_id']
            return self.redpanda.get_node(new_leader)

        # repeat the following test a few times.
        #
        #  1. transfer leadership to a new node
        #  2. check that new leader reports metrics
        #  3. check that prev leader does not report
        #
        # note that here reporting does not mean that the node does not report
        # any metrics but that it does not report metrics for consumer groups
        # for which it is not leader.
        for _ in range(4):
            new_leader = select_next_leader()

            wait_until(lambda: transfer_leadership(new_leader),
                       timeout_sec=30,
                       backoff_sec=5)

            self.logger.debug(
                f"Waiting for metrics from the single node: {new_leader.account.hostname}"
            )
            wait_until(lambda: metrics_from_single_node(new_leader),
                       timeout_sec=30,
                       backoff_sec=5)

        for host in producers + consumers:
            host.stop()
            host.free()
