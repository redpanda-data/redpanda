# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from contextlib import contextmanager

import confluent_kafka as ck

from rptest.util import wait_until_result


@contextmanager
def expect_kafka_error(err: int | None = None):
    try:
        yield
    except ck.KafkaException as e:
        if e.args[0].code() != err:
            raise
    else:
        if err is not None:
            raise RuntimeError("Expected an exception!")


@contextmanager
def try_transaction(
    producer: ck.Producer,
    consumer: ck.Consumer,
    send_offset_err: int | None = None,
    commit_err: int | None = None,
):
    producer.begin_transaction()

    yield

    while producer.flush(0) > 0:
        producer.poll(0.1)

    with expect_kafka_error(send_offset_err):
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()), consumer.consumer_group_metadata()
        )

    with expect_kafka_error(commit_err):
        producer.commit_transaction()

    if send_offset_err is not None or commit_err is not None:
        producer.abort_transaction()


class TransactionsMixin:
    def find_coordinator(self, txid, node=None):
        if node is None:
            node = random.choice(self.redpanda.started_nodes())

        def find_tx_coordinator():
            r = self.admin.find_tx_coordinator(txid, node=node)
            return r["ec"] == 0, r

        return wait_until_result(
            find_tx_coordinator,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Can't find a coordinator for tx.id={txid}",
        )

    def on_delivery(self, err, _):
        assert err is None, err

    def generate_data(self, topic, num_records, extra_cfg={}):
        producer_cfg = {
            "bootstrap.servers": self.redpanda.brokers(),
        }
        producer_cfg.update(extra_cfg)
        producer = ck.Producer(producer_cfg)

        for i in range(num_records):
            producer.produce(topic.name, str(i), str(i), on_delivery=self.on_delivery)

        producer.flush()

    def consume(self, consumer, max_records=10, timeout_s=2):
        def consume_records():
            records = consumer.consume(max_records, timeout_s)

            if (records is not None) and (len(records) != 0):
                return True, records
            else:
                return False, records

        return wait_until_result(
            consume_records,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Can not consume data",
        )
