from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka import TopicPartition, OFFSET_BEGINNING
import random

import time


class SyncProducer:
    def __init__(self, bootstrap):
        self.bootstrap = bootstrap
        self.producer = None
        self.last_msg = None

    def init(self):
        self.producer = Producer({
            "bootstrap.servers": self.bootstrap,
            "request.required.acks": -1,
            "retries": 5,
            "enable.idempotence": True
        })

    def on_delivery(self, err, msg):
        if err is not None:
            raise KafkaException(err)
        self.last_msg = msg

    def produce(self, topic, partition, key, value, timeout_s):
        self.last_msg = None
        self.producer.produce(topic,
                              key=key.encode('utf-8'),
                              value=value.encode('utf-8'),
                              partition=partition,
                              callback=lambda e, m: self.on_delivery(e, m))
        self.producer.flush(timeout_s)
        msg = self.last_msg
        if msg == None:
            raise KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT))
        if msg.error() != None:
            raise KafkaException(msg.error())
        assert msg.offset() != None
        return msg.offset()


class LogReader:
    def __init__(self, bootstrap):
        self.bootstrap = bootstrap
        self.consumer = None
        self.stream = None

    def init(self, group, topic, partition):
        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed"
        })
        self.consumer.assign(
            [TopicPartition(topic, partition, OFFSET_BEGINNING)])
        self.stream = self.stream_gen()

    def stream_gen(self):
        while True:
            msgs = self.consumer.consume(timeout=10)
            for msg in msgs:
                yield msg

    def read_until(self, check, timeout_s):
        begin = time.time()
        while True:
            if time.time() - begin > timeout_s:
                raise KafkaException(KafkaError(KafkaError._TIMED_OUT))
            for msg in self.stream:
                offset = msg.offset()
                value = msg.value().decode('utf-8')
                key = msg.key().decode('utf-8')
                if check(offset, key, value):
                    return


def expect(offset, key, value):
    def check(ro, rk, rv):
        if ro < offset:
            return False
        if ro == offset:
            if rk != key:
                raise RuntimeError(f"expected key='{key}' got '{rk}'")
            if rv != value:
                raise RuntimeError(f"expected value='{value}' got '{rv}'")
            return True
        raise RuntimeError(f"read offset={ro} but skipped {offset}")

    return check


class PingPong:
    def __init__(self, brokers, topic, partition, logger):
        self.brokers = brokers
        random.shuffle(self.brokers)
        bootstrap = ",".join(self.brokers)
        self.consumer = LogReader(bootstrap)
        self.consumer.init(group="ping_ponger1",
                           topic=topic,
                           partition=partition)
        self.producer = SyncProducer(bootstrap)
        self.producer.init()
        self.logger = logger
        self.topic = topic
        self.partition = partition

    def ping_pong(self, timeout_s=5, retries=0):
        key = str(random.randint(0, 1000))
        value = str(random.randint(0, 1000))

        start = time.time()

        offset = None
        count = 0
        while True:
            count += 1
            try:
                offset = self.producer.produce(topic=self.topic,
                                               partition=self.partition,
                                               key=key,
                                               value=value,
                                               timeout_s=timeout_s)
                break
            except KafkaException as e:
                if count > retries:
                    raise
                if e.args[0].code() == KafkaError._MSG_TIMED_OUT:
                    pass
                elif e.args[0].code() == KafkaError._TIMED_OUT:
                    pass
                else:
                    raise
                random.shuffle(self.brokers)
                bootstrap = ",".join(self.brokers)
                # recreating a producer to overcome this issue
                # https://github.com/confluentinc/confluent-kafka-python/issues/1335
                # once it's fixed we should rely on the internal confluent_kafka's
                # ability to retry the init_producer_id request
                self.producer = SyncProducer(bootstrap)
                self.producer.init()
                self.logger.info(f"produce request {key}={value} timed out")

        self.consumer.read_until(expect(offset, key, value),
                                 timeout_s=timeout_s)
        latency = time.time() - start
        self.logger.info(
            f"ping_pong produced and consumed {key}={value}@{offset} in {(latency)*1000.0:.2f} ms"
        )
