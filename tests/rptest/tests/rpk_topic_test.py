# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
import ducktape.errors

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.rpk_consumer import RpkConsumer
from rptest.util import expect_exception

import time
import random


class RpkToolTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkToolTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_create_topic(self):
        topic = 'rp_dt_test_create_topic'
        self._rpk.create_topic(topic)

        wait_until(lambda: topic in self._rpk.list_topics(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg=f'Topic {topic} never appeared.')

    @cluster(num_nodes=1)
    @parametrize(config_type="compression.type")
    @parametrize(config_type="compaction.strategy")
    @parametrize(config_type="message.timestamp.type")
    @parametrize(config_type="cleanup.policy")
    def test_create_topic_with_invalid_config(self, config_type):
        with expect_exception(RpkException,
                              lambda e: "INVALID_CONFIG" in str(e)):
            out = self._rpk.create_topic(
                'rp_dt_test_create_topic_with_invalid_config',
                config={config_type: "foo"})

    @cluster(num_nodes=1)
    def test_add_unfeasible_number_of_partitions(self):
        topic = 'rp_dt_test_add_unfeasible_number_of_partitions'
        with expect_exception(RpkException,
                              lambda e: "INVALID_REQUEST" in str(e)):
            self._rpk.create_topic(topic)
            out = self._rpk.add_partitions(topic, 2000000000000)

    @cluster(num_nodes=4)
    def test_produce(self):
        topic = 'rp_dt_test_produce'
        message = 'message'
        key = 'key'
        h_key = 'h_key'
        h_value = 'h_value'
        headers = [h_key + ':' + h_value]

        self._rpk.create_topic(topic)
        self._rpk.produce(topic, key, message, headers)

        c = RpkConsumer(self._ctx, self.redpanda, topic)
        c.start()

        def cond():
            return c.messages is not None \
                and len(c.messages) == 1 \
                and c.messages[0]['value'] == message \
                and c.messages[0]['key'] == key \
                and c.messages[0]['headers'] == [
                    {'key': h_key, 'value': h_value},
                ]

        wait_until(cond,
                   timeout_sec=120,
                   backoff_sec=30,
                   err_msg=f'Message in {topic} never appeared.')

    @cluster(num_nodes=4)
    def test_consume_as_group(self):
        topic = 'rp_dt_test_consume_as_group'
        message = 'message'
        key = 'key'
        h_key = 'h_key'
        h_value = 'h_value'
        headers = [h_key + ':' + h_value]

        self._rpk.create_topic(topic)

        c = RpkConsumer(self._ctx, self.redpanda, topic, group='group')
        c.start()

        def cond():
            if c.error:
                raise c.error
            self._rpk.produce(topic, key, message, headers)
            return c.messages \
                and c.messages[0]['value'] == message \
                and c.messages[0]['key'] == key \
                and c.messages[0]['headers'] == [
                    {'key': h_key, 'value': h_value},
                ]

        wait_until(cond,
                   timeout_sec=120,
                   backoff_sec=15,
                   err_msg=f'Message in {topic} never appeared.')

    @cluster(num_nodes=4)
    def test_consume_newest(self):
        topic = 'rp_dt_test_consume_newest'
        message = 'newest message'
        key = 'key'
        h_key = 'h_key'
        h_value = 'h_value'
        headers = [h_key + ':' + h_value]

        self._rpk.create_topic(topic)

        c = RpkConsumer(self._ctx, self.redpanda, topic, offset='newest')
        c.start()

        def cond():
            if c.error:
                raise c.error
            self._rpk.produce(topic, key, message, headers)
            return c.messages \
                and c.messages[0]['value'] == message \
                and c.messages[0]['key'] == key \
                and c.messages[0]['headers'] == [
                    {'key': h_key, 'value': h_value},
                ]

        wait_until(cond,
                   timeout_sec=150,
                   backoff_sec=30,
                   err_msg=f'Message in {topic} never appeared.')

    @cluster(num_nodes=4)
    def test_consume_oldest(self):
        topic = 'rp_dt_test_consume_oldest'

        n = random.randint(10, 100)
        msgs = {}
        for i in range(n):
            msgs['key-' + str(i)] = 'message-' + str(i)

        self._rpk.create_topic(topic)

        # Produce messages
        for k in msgs:
            self._rpk.produce(topic, k, msgs[k])

        c = RpkConsumer(self._ctx, self.redpanda, topic)
        c.start()

        def cond():
            # Consume from the beginning
            if len(c.messages) != len(msgs):
                return False

            for m in c.messages:
                key = m['key']
                if key is None:
                    return False

                if m['value'] != msgs[key]:
                    return False

            return True

        wait_until(cond,
                   timeout_sec=60,
                   backoff_sec=20,
                   err_msg=f'Message in {topic} never appeared.')

    @cluster(num_nodes=4)
    def test_consume_from_partition(self):
        topic = 'rp_dt_test_consume_from_partition'

        n_parts = random.randint(3, 100)
        self._rpk.create_topic(topic, partitions=n_parts)

        n = random.randint(10, 30)
        msgs = {}
        for i in range(n):
            msgs['key-' + str(i)] = 'message-' + str(i)

        part = random.randint(0, n_parts - 1)
        # Produce messages to a random partition
        for k in msgs:
            self._rpk.produce(topic, k, msgs[k], partition=part)

        # Consume from the beginning
        c = RpkConsumer(self._ctx,
                        self.redpanda,
                        topic,
                        offset='oldest',
                        partitions=[part])
        c.start()

        def cond():
            if len(c.messages) != len(msgs):
                return False

            for m in c.messages:
                key = m['key']
                if key is None:
                    return False

                if m['value'] != msgs[key]:
                    return False

            return True

        # timeout loop, but reset the timeout if we appear to be making progress
        retries = 10
        prev_msg_count = len(c.messages)
        while retries > 0:
            self.redpanda.logger.debug(
                f"Message count {len(c.messages)} retries {retries}")
            if cond():
                self._rpk.delete_topic(topic)
                return
            if len(c.messages) > prev_msg_count:
                prev_msg_count = len(c.messages)
                retries = 10
            time.sleep(1)
            retries -= 1

        raise ducktape.errors.TimeoutError(
            f'Message in {topic} never appeared.')

    @cluster(num_nodes=4)
    def test_produce_and_consume_tombstones(self):
        topic = 'rp_dt_test_produce_and_consume_tombstones'
        self._rpk.create_topic(topic)

        num_messages = 2

        tombstone_key = 'ISTOMBSTONE'
        tombstone_value = ''

        # Producing a record with an empty value and -Z flag results in a tombstone.
        self._rpk.produce(topic,
                          tombstone_key,
                          tombstone_value,
                          tombstone=True)

        not_tombstone_key = 'ISNOTTOMBSTONE'

        # Producing a record with an empty value without the -Z flag results in an empty value.
        self._rpk.produce(topic,
                          not_tombstone_key,
                          tombstone_value,
                          tombstone=False)

        c = RpkConsumer(self._ctx, self.redpanda, topic)
        c.start()

        def cond():
            return len(c.messages) == num_messages

        wait_until(cond,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg=f'Messages in {topic} never appeared.')

        # Tombstone records do not have a value in the returned JSON
        assert c.messages[0]['key'] == tombstone_key
        assert 'value' not in c.messages[0]

        # Records with an empty string have a value of `""` in the returned JSON
        assert c.messages[1]['key'] == not_tombstone_key
        assert c.messages[1]['value'] == ""
