from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.rpk_consumer import RpkConsumer

import time
import threading
import random


class RpkToolTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkToolTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    def test_create_topic(self):
        self._rpk.create_topic("topic")

        wait_until(lambda: "topic" in self._rpk.list_topics(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Topic never appeared.")

    def test_produce(self):
        topic = 'topic'
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
            return len(c.messages) == 1 \
                and c.messages[0]['message'] == message \
                and c.messages[0]['key'] == key \
                and c.messages[0]['headers'] == [
                    {'key': h_key, 'value': h_value},
                ]

        wait_until(cond,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Message didn't appear.")

    def test_consume_as_group(self):
        topic = 'topic_group'
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
                and c.messages[0]['message'] == message \
                and c.messages[0]['key'] == key \
                and c.messages[0]['headers'] == [
                    {'key': h_key, 'value': h_value},
                ]

        wait_until(cond,
                   timeout_sec=30,
                   backoff_sec=8,
                   err_msg="Message didn't appear.")

    def test_consume_newest(self):
        topic = 'topic_newest'
        message = 'message'
        key = 'key'
        h_key = 'h_key'
        h_value = 'h_value'
        headers = [h_key + ':' + h_value]

        self._rpk.create_topic(topic)
        # Gotta sleep to make sure the topic is replicated and the
        # consumer doesn't fail.
        time.sleep(5)

        c = RpkConsumer(self._ctx, self.redpanda, topic, offset='newest')
        c.start()

        def cond():
            if c.error:
                raise c.error
            self._rpk.produce(topic, key, message, headers)
            return c.messages \
                and c.messages[0]['message'] == message \
                and c.messages[0]['key'] == key \
                and c.messages[0]['headers'] == [
                    {'key': h_key, 'value': h_value},
                ]

        wait_until(cond,
                   timeout_sec=30,
                   backoff_sec=8,
                   err_msg="Message didn't appear.")

    def test_consume_oldest(self):
        topic = 'topic'

        n = random.randint(10, 100)
        msgs = {}
        for i in range(n):
            msgs['key-' + str(i)] = 'message-' + str(i)

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

                if m['message'] != msgs[key]:
                    return False

            return True

        wait_until(cond,
                   timeout_sec=30,
                   backoff_sec=8,
                   err_msg="Message didn't appear.")

    def test_consume_from_partition(self):
        topic = 'topic_partition'

        n_parts = random.randint(3, 100)
        self._rpk.create_topic(topic, partitions=n_parts)

        n = random.randint(10, 30)
        msgs = {}
        for i in range(n):
            msgs['key-' + str(i)] = 'message-' + str(i)

        part = random.randint(0, n_parts)
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

                if m['message'] != msgs[key]:
                    return False

            return True

        wait_until(cond,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Message didn't appear.")
