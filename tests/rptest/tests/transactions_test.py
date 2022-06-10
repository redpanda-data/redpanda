# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, RedpandaService
import confluent_kafka as ck
from rptest.clients.default import DefaultClient


class UpgradeTransactionTest(RedpandaTest):
    @cluster(num_nodes=2, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def check_parsing_test(self):
        self.redpanda = RedpandaService(
            self.test_context,
            1,
            extra_rp_conf={
                "enable_idempotence": True,
                "enable_transactions": True,
                "transaction_coordinator_replication": 1,
                "id_allocator_replication": 1,
                "enable_leader_balancer": False,
                "enable_auto_rebalance_on_node_add": False,
            },
            environment={"__REDPANDA_LOGICAL_VERSION": 3})

        self.redpanda.start()

        spec = TopicSpec(partition_count=1, replication_factor=1)
        self._client = DefaultClient(self.redpanda)
        self.client().create_topic(spec)
        topic_name = spec.name

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '123',
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()

        def on_del(err, msg):
            assert err == None

        max_tx = 100
        for i in range(max_tx):
            producer.begin_transaction()
            producer.produce(topic_name, str(i), str(i), 0, on_del)
            producer.commit_transaction()

        self.redpanda.set_environment({"__REDPANDA_LOGICAL_VERSION": 4})
        for n in self.redpanda.nodes:
            self.redpanda.restart_nodes(n, stop_timeout=60)

        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
        })

        consumer.subscribe([topic_name])

        num_consumed = 0
        prev_rec = bytes("0", 'UTF-8')

        while num_consumed != max_tx:
            max_records = 10
            timeout = 10
            records = consumer.consume(max_records, timeout)

            for record in records:
                assert prev_rec == record.key()
                prev_rec = bytes(str(int(prev_rec) + 1), 'UTF-8')

            num_consumed += len(records)
