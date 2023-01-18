# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
import random
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.kcl import KCL
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.rpk_producer import RpkProducer


class OffsetForLeaderEpochTest(PreallocNodesTest):
    """
    Check offset for leader epoch handling
    """
    def _all_have_leaders(self):
        admin = Admin(self.redpanda)

        for n in self.redpanda.nodes:
            partitions = admin.get_partitions(node=n)
            if not all([p['leader'] != -1 for p in partitions]):
                return False

        return True

    def _produce(self, topic, msg_cnt):
        wait_until(lambda: self._all_have_leaders(), 20, backoff_sec=2)

        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               topic,
                               16384,
                               msg_cnt,
                               acks=-1)
        producer.start()
        producer.wait()
        producer.free()

    def __init__(self, test_context):
        super(OffsetForLeaderEpochTest,
              self).__init__(num_brokers=5,
                             test_context=test_context,
                             extra_rp_conf={
                                 'enable_leader_balancer': False,
                                 "log_compaction_interval_ms": 1000
                             },
                             node_prealloc_count=1)

    def list_offsets(self, topics, total_partitions):
        kcl = KCL(self.redpanda)
        topic_names = [t.name for t in topics]
        offsets_map = {}

        def update_offset_map():
            offsets = kcl.list_offsets(topic_names)
            self.logger.info(f"offsets_list: {offsets}")
            for p in offsets:
                offsets_map[(p.topic, p.partition)] = int(p.end_offset)
            self.logger.info(f"offsets_map: {offsets_map}")

        def all_offsets_present():
            update_offset_map()
            return all([l != -1 for _, l in offsets_map.items()
                        ]) and len(offsets_map) == total_partitions

        wait_until(all_offsets_present, 30, 1)
        return offsets_map

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_offset_for_leader_epoch(self):
        replication_factors = [1, 3, 5]
        cleanup_policies = [
            TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_DELETE
        ]
        topics = []

        for i in range(0, 10):
            topics.append(
                TopicSpec(
                    partition_count=random.randint(1, 50),
                    replication_factor=random.choice(replication_factors),
                    cleanup_policy=random.choice(cleanup_policies)))

        topic_names = [t.name for t in topics]
        total_partitions = sum([t.partition_count for t in topics])
        # create test topics
        self.client().create_topic(topics)

        initial_offsets = self.list_offsets(topics=topics,
                                            total_partitions=total_partitions)

        kcl = KCL(self.redpanda)
        leader_epoch_offsets = kcl.offset_for_leader_epoch(topics=topic_names,
                                                           leader_epoch=1)

        for o in leader_epoch_offsets:
            # check if the offset epoch matches what is expected or it is not available
            # (may be the case if leader wasn't elected in term 1 but other term in this case the offset for term 1 will not be presetn)
            assert initial_offsets[(o.topic,
                                    o.partition)] == o.epoch_end_offset or (
                                        o.epoch_end_offset == -1
                                        and o.leader_epoch > 1)

        # restart all the nodes to force leader election,
        # increase start timeout as partition count may get large
        self.redpanda.restart_nodes(self.redpanda.nodes, start_timeout=30)
        # produce more data
        for t in topics:
            self._produce(t.name, 20)

        # check epoch end offsets for term 1
        leader_epoch_offsets = kcl.offset_for_leader_epoch(topics=topic_names,
                                                           leader_epoch=1)

        for o in leader_epoch_offsets:
            assert initial_offsets[(o.topic,
                                    o.partition)] == o.epoch_end_offset or (
                                        o.epoch_end_offset == -1
                                        and o.leader_epoch > 2)

        last_offsets = self.list_offsets(topics=topics,
                                         total_partitions=total_partitions)
        rpk = RpkTool(self.redpanda)
        for t in topics:
            tp_desc = rpk.describe_topic(t.name)
            for p in tp_desc:
                for o in kcl.offset_for_leader_epoch(
                        topics=f"{t.name}:{p.id}",
                        leader_epoch=p.leader_epoch,
                        current_leader_epoch=p.leader_epoch):
                    assert last_offsets[(o.topic,
                                         o.partition)] == o.epoch_end_offset

        # test returning unknown leader epoch error, we use large leader epoch value

        leader_epoch_offsets = kcl.offset_for_leader_epoch(
            topics=topic_names, leader_epoch=1, current_leader_epoch=1000)

        for o in leader_epoch_offsets:
            assert o.error is not None and "UNKNOWN_LEADER_EPOCH" in o.error

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_offset_for_leader_epoch_transfer(self):

        topic = TopicSpec(partition_count=64, replication_factor=3)

        # create test topics
        self.client().create_topic(topic)

        kcl = KCL(self.redpanda)

        def produce_some():
            msg_size = 512
            msg_cnt = 1000000 if self.redpanda.dedicated_nodes else 10000

            producer = KgoVerifierProducer(self.test_context, self.redpanda,
                                           topic.name, msg_size, msg_cnt,
                                           self.preallocated_nodes)
            producer.start()
            producer.wait()

        admin = Admin(self.redpanda)
        offsets = {}

        def get_offsets_for_leader_epoch(epoch):
            offsets_for_leader_epoch = []

            def have_all_offsets():
                offsets_for_leader_epoch.clear()
                offsets = kcl.offset_for_leader_epoch(topics=[topic.name],
                                                      leader_epoch=epoch)
                offsets_for_leader_epoch.extend(offsets)
                return all([
                    ofle.epoch_end_offset != -1
                    for ofle in offsets_for_leader_epoch
                ])

            wait_until(have_all_offsets, 30, 1)

            return offsets_for_leader_epoch

        rpk = RpkTool(self.redpanda)

        def get_offsets_and_epochs():
            offsets = []

            def refresh():
                result = rpk.describe_topic(topic.name)
                offsets.clear()
                offsets.extend(result)

            def all_offsets_valid():
                refresh()
                return all([p.high_watermark >= 0 for p in offsets])

            wait_until(all_offsets_valid, 30, 1)

            return offsets

        # store offsets after each epoch change
        offsets_after_epochs = []

        # generate some leader epoch changes
        for _ in range(5):
            produce_some()
            # store partition epoch and offsets
            offsets = get_offsets_and_epochs()
            offsets_after_epochs.append(list(offsets))

            for p in offsets_after_epochs[-1]:
                admin.partition_transfer_leadership("kafka",
                                                    topic=topic.name,
                                                    partition=p.id)
        # generate some more leadership changes
        for _ in range(5):
            for p in offsets_after_epochs[-1]:
                admin.partition_transfer_leadership("kafka",
                                                    topic=topic.name,
                                                    partition=p.id)
        epoch_offsets = defaultdict(dict)

        # group partitions per leader epoch
        for offsets in offsets_after_epochs:
            for p in offsets:
                epoch_offsets[p.leader_epoch][p.id] = p.high_watermark

        for epoch, partitions in epoch_offsets.items():
            offsets_for_leader_epoch = get_offsets_for_leader_epoch(epoch)
            for p in offsets_for_leader_epoch:
                assert p.partition in partitions
                assert partitions[p.partition] == p.epoch_end_offset
