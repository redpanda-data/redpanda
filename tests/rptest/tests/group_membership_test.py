# Copyright 2021 Vectorized, Inc.
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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kcl import KCL
from rptest.clients.kafka_cat import KafkaCat
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
                                             target=target_id)
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
        https://github.com/vectorizedio/redpanda/issues/2528
        """
        kcl = KCL(self.redpanda)
        kcl.produce(self.topic, "msg\n")
        # create 4 groups
        kcl.consume(self.topic, n=1, group="g0")
        kcl.consume(self.topic, n=1, group="g1")
        kcl.consume(self.topic, n=1, group="g2")
        kcl.consume(self.topic, n=1, group="g3")
        # transfer kafka_internal/group/0 leadership across the nodes to trigger
        # group state recovery on each node
        for n in self.redpanda.nodes:
            self._transfer_with_retry(namespace="kafka_internal",
                                      topic="group",
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
        extra_rp_conf = dict(default_topic_replications=3, )
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

        self.redpanda.delete_topic(topic)
        metrics_offsets = self._get_offset_from_metrics(group_1)
        assert metrics_offsets is None

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
            for i in range(100):
                payload = str(random.randint(0, 1000))
                offset = rpk.produce(topic, "", payload, timeout=5)

            metric_key = (topic, "0")
            rpk.consume(topic, group=group, n=50)
            metrics_offsets = self._get_offset_from_metrics(group)
            assert metric_key in metrics_offsets
            assert metrics_offsets[metric_key] == 50

            self.redpanda.delete_topic(topic)

        check_metric()
        metrics_offsets = self._get_offset_from_metrics(group)
        assert metrics_offsets is None
        self.redpanda.create_topic(topic_spec)
        check_metric()

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

        def get_offset_with_node_from_metric(group):
            metric = self.redpanda.metrics_sample("kafka_group_offset")
            if metric is None:
                return None
            metric = metric.label_filter(dict(group=group))
            return metric.samples

        def get_group_leader():
            return admin.get_partitions(namespace="kafka_internal",
                                        topic="group",
                                        partition=0)['leader_id']

        def check_metric_from_node(node):
            metrics_offsets = get_offset_with_node_from_metric(group)
            if metrics_offsets == None:
                return False
            return all([
                metric.node.account.hostname == node.account.hostname
                for metric in metrics_offsets
            ])

        def transfer_completed(new_leader_node):
            return self.redpanda.nodes[get_group_leader() - 1].account.hostname \
                    == new_leader_node.account.hostname

        leader_node = self.redpanda.nodes[get_group_leader() - 1]

        # Check transfer leadership to another node
        for i in range(3):
            new_leader_node = random.choice(
                list(filter(lambda x: x != leader_node, self.redpanda.nodes)))

            admin.transfer_leadership_to(
                namespace="kafka_internal",
                topic="group",
                partition=0,
                target=self.redpanda.idx(new_leader_node))

            wait_until(lambda: transfer_completed(new_leader_node) and
                       check_metric_from_node(new_leader_node),
                       timeout_sec=30,
                       backoff_sec=5)

            leader_node = new_leader_node

        # Check transfer leadership to same node
        admin.transfer_leadership_to(namespace="kafka_internal",
                                     topic="group",
                                     partition=0,
                                     target=self.redpanda.idx(leader_node))

        wait_until(lambda: transfer_completed(leader_node) and
                   check_metric_from_node(leader_node),
                   timeout_sec=30,
                   backoff_sec=5)

        for host in producers + consumers:
            host.stop()
            host.free()
