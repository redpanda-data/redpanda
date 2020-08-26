from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.kaf_producer import KafProducer
from rptest.services.kaf_consumer import KafConsumer


class WaitForLocalConsumerTest(RedpandaTest):
    """
    Start a kaf-based producer and consumer, then wait until the consumer has
    observed a certain number of produced records.
    """
    NUM_RECORDS = 2000

    def __init__(self, ctx):
        topics = dict(topic=dict(partitions=1, replication_factor=1))

        super(WaitForLocalConsumerTest, self).__init__(test_context=ctx,
                                                       num_brokers=1,
                                                       topics=topics)

        self._producer = KafProducer(ctx, self.redpanda, "topic")
        self._consumer = KafConsumer(ctx, self.redpanda, "topic")

    @cluster(num_nodes=4)
    def test_wait_for_local_consumer(self):
        self._consumer.start()
        self._producer.start()

        start = self._consumer.offset.copy()

        def consumed():
            assert not self._consumer.done
            if not start:
                start.update(self._consumer.offset)
                return False
            curr = self._consumer.offset
            consumed = [curr[p] - o for p, o in start.items()]
            self.logger.debug(f"Consumer progress: {consumed}")
            return all(c > WaitForLocalConsumerTest.NUM_RECORDS
                       for c in consumed)

        wait_until(consumed, timeout_sec=180, backoff_sec=3)

        # ensure that the consumer is still running. one problematic behavior we
        # observed was that the consumer was stopping.
        assert not self._consumer.done
