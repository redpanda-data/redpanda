# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import confluent_kafka as ck
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

from rptest.clients.rpk import AclList, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.transactions.util import (
    TransactionsMixin,
    expect_kafka_error,
    try_transaction,
)


class TransactionsAuthorizationTest(RedpandaTest, TransactionsMixin):
    topics = (
        TopicSpec(partition_count=1, replication_factor=3),
        TopicSpec(partition_count=1, replication_factor=3),
    )

    USER_1 = SaslCredentials("user-1", "password0123456789", "SCRAM-SHA-256")
    USER_2 = SaslCredentials("user-2", "password0123456789", "SCRAM-SHA-256")

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
            "group_initial_rebalance_delay": 1,
        }

        super().__init__(
            test_context=test_context, extra_rp_conf=extra_rp_conf, log_level="trace"
        )

        self.security = SecurityConfig()
        self.security.kafka_enable_authorization = True
        self.security.enable_sasl = True
        self.security.require_client_auth = True
        self.security.endpoint_authn_method = "sasl"

        self.redpanda.set_security_settings(self.security)

        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100
        self.admin = Admin(self.redpanda)

        self.rpk = RpkTool(
            self.redpanda,
            username=self.superuser.username,
            password=self.superuser.password,
            sasl_mechanism=self.superuser.algorithm,
        )

    def setUp(self):
        super().setUp()
        self.admin.create_user(
            self.USER_1.username, self.USER_1.password, self.USER_1.algorithm
        )
        self.admin.create_user(
            self.USER_2.username, self.USER_2.password, self.USER_2.algorithm
        )

    def sasl_cfg(self, user):
        return {
            "sasl.username": user.username,
            "sasl.password": user.password,
            "sasl.mechanism": user.algorithm,
            "security.protocol": "sasl_plaintext",
        }

    def sasl_txn_producer(self, user, cfg={}):
        cfg.update(self.sasl_cfg(user))
        p = ck.Producer(cfg)
        p.init_transactions()
        return p

    def sasl_consumer(self, user, cfg={}):
        cfg.update(self.sasl_cfg(user))
        return ck.Consumer(cfg)

    def allow_principal_sync(self, principal, operations, resource, resource_name):
        self.rpk.sasl_allow_principal(principal, operations, resource, resource_name)

        def acl_ready(node: ClusterNode):
            lst = AclList.parse_raw(self.rpk.acl_list(node=node))
            return [
                lst.has_permission(principal, op, resource, resource_name)
                for op in operations
            ]

        wait_until(
            lambda: all(acl_ready(node) for node in self.redpanda.nodes),
            timeout_sec=5,
            backoff_sec=1,
            err_msg="ACL not updated in time",
        )

    @cluster(num_nodes=3)
    def init_transactions_authz_test(self):
        producer_cfg = {
            "bootstrap.servers": self.redpanda.brokers(),
            "transactional.id": "0",
        }

        user = self.USER_1

        self.redpanda.logger.debug("init_transactions should fail without ACL")
        with expect_kafka_error(ck.KafkaError.TRANSACTIONAL_ID_AUTHORIZATION_FAILED):
            self.sasl_txn_producer(user, cfg=producer_cfg)

        self.allow_principal_sync(user.username, ["write"], "transactional-id", "0")

        self.sasl_txn_producer(user, cfg=producer_cfg)

    @cluster(num_nodes=3)
    def simple_authz_test(self):
        consume_user = self.USER_1
        produce_user = self.USER_2

        self.allow_principal_sync(
            produce_user.username, ["all"], "topic", self.input_t.name
        )
        self.generate_data(
            self.input_t, self.max_records, extra_cfg=self.sasl_cfg(produce_user)
        )

        producer_cfg = {
            "bootstrap.servers": self.redpanda.brokers(),
            "transactional.id": "0",
        }
        consumer_cfg = {
            "bootstrap.servers": self.redpanda.brokers(),
            "group.id": "test",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        self.allow_principal_sync(
            consume_user.username, ["read"], "topic", self.input_t.name
        )
        self.allow_principal_sync(consume_user.username, ["read"], "group", "test")
        self.allow_principal_sync(
            produce_user.username, ["write"], "transactional-id", "0"
        )
        # TODO(oren): what's this one for?
        self.allow_principal_sync(
            produce_user.username, ["read"], "topic", self.output_t.name
        )

        consumer = self.sasl_consumer(consume_user, cfg=consumer_cfg)
        consumer.subscribe([self.input_t.name])
        records = self.consume(consumer)
        assert records is not None

        producer = self.sasl_txn_producer(produce_user, cfg=producer_cfg)

        def on_delivery_purged(err, _):
            assert err is not None and err.code() == ck.KafkaError._PURGE_QUEUE

        def process_records(producer, records, on_delivery, history=[]):
            for record in records:
                assert record.error() is None, f"Consume error: {record.error()}"
                history.append(record)
                producer.produce(
                    self.output_t.name,
                    record.value(),
                    record.key(),
                    on_delivery=on_delivery,
                )

        with try_transaction(
            producer,
            consumer,
            send_offset_err=ck.KafkaError.TOPIC_AUTHORIZATION_FAILED,
            commit_err=ck.KafkaError.TOPIC_AUTHORIZATION_FAILED,
        ):
            process_records(producer, records, on_delivery_purged)

        self.allow_principal_sync(
            produce_user.username, ["write"], "topic", self.output_t.name
        )

        with try_transaction(
            producer,
            consumer,
            send_offset_err=ck.KafkaError.GROUP_AUTHORIZATION_FAILED,
            commit_err=ck.KafkaError.GROUP_AUTHORIZATION_FAILED,
        ):
            process_records(producer, records, self.on_delivery)

        self.allow_principal_sync(produce_user.username, ["read"], "group", "test")

        # Now we have all the requisite permissions set up, and we should be able to
        # make progress

        num_consumed_records = 0
        consumed_from_input_topic = []

        # Process the records we have sitting in memory

        with try_transaction(producer, consumer):
            process_records(
                producer, records, self.on_delivery, consumed_from_input_topic
            )
            num_consumed_records += len(records)

        # then consume the rest, transactionwise

        while num_consumed_records != self.max_records:
            # Imagine that consume got broken, we read the same record twice and overshoot the condition
            assert num_consumed_records < self.max_records

            records = self.consume(consumer)
            assert records is not None

            with try_transaction(producer, consumer):
                process_records(
                    producer, records, self.on_delivery, consumed_from_input_topic
                )

            num_consumed_records += len(records)

        consumer.close()
        assert len(consumed_from_input_topic) == self.max_records

        self.allow_principal_sync(
            consume_user.username, ["read"], "topic", self.output_t.name
        )
        self.allow_principal_sync(consume_user.username, ["read"], "group", "testtest")

        consumer = self.sasl_consumer(
            consume_user,
            cfg={
                "group.id": "testtest",
                "bootstrap.servers": self.redpanda.brokers(),
                "auto.offset.reset": "earliest",
            },
        )
        consumer.subscribe([self.output_t.name])

        index_from_input = 0

        while index_from_input < self.max_records:
            records = self.consume(consumer)
            for record in records:
                assert record.error() is None, f"Consume error: {record.error()}"
                assert (
                    record.key() == consumed_from_input_topic[index_from_input].key()
                ), (
                    f"Records key does not match from input {consumed_from_input_topic[index_from_input].key()}, from output {record.key()}"
                )
                assert (
                    record.value()
                    == consumed_from_input_topic[index_from_input].value()
                ), (
                    f"Records value does not match from input {consumed_from_input_topic[index_from_input].value()}, from output {record.value()}"
                )
                index_from_input += 1

        assert consumer.poll(timeout=3) is None
