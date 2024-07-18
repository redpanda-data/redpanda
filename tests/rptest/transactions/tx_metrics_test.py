# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from rptest.clients.default import DefaultClient
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.metrics_check import MetricCheck
from rptest.tests.redpanda_test import RedpandaTest
import confluent_kafka as ck
import re


class TransactionMetricsTest(RedpandaTest):
    def __init__(self, test_context):
        super(TransactionMetricsTest, self).__init__(test_context=test_context)
        self.test_topic = TopicSpec(name="test",
                                    partition_count=1,
                                    replication_factor=1)

    def random_transaction(self,
                           tx_id: str,
                           timeout_ms: int = 60000,
                           commit_or_abort: bool | None = False):
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
            'transaction.timeout.ms': timeout_ms,
        })

        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(topic=self.test_topic.name, value="val", key="key")
        producer.flush()

        if commit_or_abort == None:
            return
        if commit_or_abort:
            producer.commit_transaction()
        else:
            producer.abort_transaction()

    def validate(self, metrics: MetricCheck, open_tx, committed_tx, aborted_tx,
                 active_tx_ids):

        expectations = [
            ('vectorized_tx_coordinator_open_transactions',
             lambda _, new: int(new) == open_tx),
            ('vectorized_tx_coordinator_committed_transactions_total',
             lambda _, new: int(new) == committed_tx),
            ('vectorized_tx_coordinator_aborted_transactions_total',
             lambda _, new: int(new) == aborted_tx),
            ('vectorized_tx_coordinator_active_transaction_ids',
             lambda _, new: int(new) == active_tx_ids)
        ]

        def debug():
            return f"open txs: {open_tx}, committed txes: {committed_tx}, aborted_tx: {aborted_tx}, active_tx_ids: {active_tx_ids}"

        wait_until(lambda: metrics.evaluate(expectations),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg=f"Metrics mismatch, expected: {debug()}")

    @cluster(num_nodes=1)
    def test_metrics(self):
        DefaultClient(self.redpanda).create_topic(self.test_topic)
        # A committed transaction
        self.random_transaction(tx_id="foo", commit_or_abort=True)
        metrics = MetricCheck(self.redpanda.logger,
                              self.redpanda,
                              self.redpanda.nodes[0],
                              re.compile('vectorized_tx_coordinator*'),
                              reduce=sum)
        # Each validate passes open_tx, committed_tx, aborted_tx and active_tx_ids
        # in that order, excluding param names for brevity.
        self.validate(metrics, 0, 1, 0, 1)
        # Aborted transaction using same tx.id
        self.random_transaction(tx_id="foo", commit_or_abort=False)
        self.validate(metrics, 0, 1, 1, 1)
        # Aborted transaction with a different producer
        self.random_transaction(tx_id="bar", commit_or_abort=False)
        self.validate(metrics, 0, 1, 2, 2)
        # Open transaction with low timeout
        self.random_transaction(tx_id="baz",
                                timeout_ms=10000,
                                commit_or_abort=None)
        self.validate(metrics, 1, 1, 2, 3)
        # The transaction should auto abort in 10s
        self.validate(metrics, 0, 1, 3, 3)
