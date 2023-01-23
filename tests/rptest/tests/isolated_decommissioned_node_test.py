# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import firewall_blocked
from rptest.clients.rpk import RpkTool
from confluent_kafka import (admin, Producer, KafkaException, Consumer)
from ducktape.mark import parametrize

import time
import uuid
import time


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)


class IsolatedDecommissionedNodeTest(PreallocNodesTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
        }

        super(IsolatedDecommissionedNodeTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             node_prealloc_count=3,
                             extra_rp_conf=extra_rp_conf)

        self.internal_port = 33145
        self.admin = Admin(self.redpanda)
        self.isolated_node = None
        self.max_records = 40

    def is_node_isolated(self):
        return self.admin.is_node_isolated(self.isolated_node)

    def check_consume(self, isolation_handler_mode):
        topic_name = self.topics[0].name
        max_retries = 5
        retries_count = 0

        consumer = Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': f"consumer-{uuid.uuid4()}",
            'auto.offset.reset': 'earliest',
            'isolation.level': 'read_committed',
        })

        consumer.subscribe([topic_name])
        num_consumed = 0
        prev_rec = bytes("0", 'UTF-8')

        while num_consumed != self.max_records and retries_count < max_retries:
            max_consume_records = 10
            timeout = 10
            records = consumer.consume(max_consume_records, timeout)

            if len(records) == 0:
                retries_count += 1
                time.sleep(3)

            for record in records:
                retries_count = 0
                assert prev_rec == record.key(), f"{prev_rec}, {record.key()}"
                prev_rec = bytes(str(int(prev_rec) + 1), 'UTF-8')

            num_consumed += len(records)

        consumer.close()

        assert num_consumed == self.max_records, f"Can not consume all data. Consumed: {num_consumed}, expected: {self.max_records}"

    @cluster(num_nodes=3)
    def create_topic_on_isolated_node_test(self):
        # Idea of this test it to pass only isolated broker to client and expect that client will get another brokers list and will communicate with them
        topic = self.topics[0]
        self.isolated_node = self.redpanda.nodes[0]
        with firewall_blocked([self.isolated_node], self.internal_port, True):
            wait_until(self.is_node_isolated, timeout_sec=20, backoff_sec=1)

            confluent_admin = admin.AdminClient({
                "bootstrap.servers":
                self.redpanda.broker_address(self.isolated_node),
            })

            confluent_admin.create_topics([
                admin.NewTopic("123", replication_factor=1, num_partitions=1)
            ])

    @cluster(num_nodes=3)
    @parametrize(isolation_handler_mode=True)
    @parametrize(isolation_handler_mode=False)
    def discover_leader_for_topic_test(self, isolation_handler_mode):
        # Idea of this test is check that producer can descover partition leader if we pass only isolated node
        if not isolation_handler_mode:
            feature_name = "node_isolation"
            self.admin.put_feature(feature_name, {"state": "disabled"})

        topic = self.topics[0]

        self.leader_for_all = None

        def wait_leader():
            try:
                self.leader_for_all = self.admin.get_partition_leader(
                    namespace="kafka", topic=str(topic), partition=0)
                return True
            except:
                return False

        wait_until(wait_leader,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Can not get leader for first topic")

        self.admin.partition_transfer_leadership('redpanda', 'controller', 0,
                                                 self.leader_for_all)
        wait_until(lambda: self.admin.get_partition_leader(
            namespace="redpanda", topic="controller", partition=0) == self.
                   leader_for_all,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Leadership did not stabilize")

        self.not_isolated_node = None
        for node in self.redpanda.nodes:
            if self.redpanda.idx(node) == self.leader_for_all:
                self.isolated_node = node
            else:
                self.not_isolated_node = node

        with firewall_blocked([self.isolated_node], self.internal_port, True):

            wait_until(self.is_node_isolated, timeout_sec=20, backoff_sec=1)

            producer = Producer({
                "bootstrap.servers":
                self.redpanda.broker_address(self.isolated_node),
            })

            for i in range(self.max_records):
                producer.produce(str(topic),
                                 key=str(i),
                                 value=str(i),
                                 callback=on_delivery)
            try:
                producer.flush(10.0)
            except ck.cimpl.KafkaException as e:
                # We can get timeout only with switched off handler for isolation node
                assert isolation_handler_mode == False
                kafka_error = e.args[0]
                assert kafka_error.code() == ck.cimpl.KafkaError._MSG_TIMED_OUT

        if isolation_handler_mode:
            self.check_consume(isolation_handler_mode)
