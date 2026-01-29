import random
import time
from collections.abc import Callable, Generator
from logging import Logger

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)


class SyncProducer:
    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self.producer: Producer | None = None
        self.last_msg: Message | None = None

    def init(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": self.bootstrap,
                "request.required.acks": -1,
                "retries": 5,
                "enable.idempotence": True,
            }
        )

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            raise KafkaException(err)
        self.last_msg = msg

    def produce(
        self, topic: str, partition: int, key: str, value: str, timeout_s: float
    ) -> int:
        self.last_msg = None
        assert self.producer is not None
        self.producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            partition=partition,
            callback=lambda e, m: self.on_delivery(e, m),
        )
        self.producer.flush(timeout_s)
        msg = self.last_msg
        if msg is None:
            raise KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT))
        if msg.error() is not None:
            raise KafkaException(msg.error())
        assert msg.offset() is not None
        return msg.offset()


class LogReader:
    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self.consumer: Consumer | None = None
        self.stream: Generator[Message, None, None] | None = None

    def init(self, group: str, topic: str, partition: int) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )
        self.consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
        self.stream = self.stream_gen()

    def stream_gen(self) -> Generator[Message, None, None]:
        assert self.consumer is not None
        while True:
            msgs = self.consumer.consume(timeout=10)
            for msg in msgs:
                yield msg

    def read_until(
        self, check: Callable[[int, str, str], bool], timeout_s: float
    ) -> None:
        assert self.stream is not None
        begin = time.time()
        while True:
            if time.time() - begin > timeout_s:
                raise KafkaException(KafkaError(KafkaError._TIMED_OUT))
            for msg in self.stream:
                offset = msg.offset()
                assert offset is not None
                value_bytes = msg.value()
                assert value_bytes is not None
                value = value_bytes.decode("utf-8")
                key_bytes = msg.key()
                assert key_bytes is not None
                key = key_bytes.decode("utf-8")
                if check(offset, key, value):
                    return


def expect(offset: int, key: str, value: str) -> Callable[[int, str, str], bool]:
    def check(ro: int, rk: str, rv: str) -> bool:
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
    def __init__(
        self, brokers: list[str], topic: str, partition: int, logger: Logger
    ) -> None:
        self.brokers = brokers
        random.shuffle(self.brokers)
        bootstrap = ",".join(self.brokers)
        self.consumer = LogReader(bootstrap)
        self.consumer.init(group="ping_ponger1", topic=topic, partition=partition)
        self.producer = SyncProducer(bootstrap)
        self.producer.init()
        self.logger = logger
        self.topic = topic
        self.partition = partition

    def ping_pong(self, timeout_s: float = 5, retries: int = 0) -> None:
        key = str(random.randint(0, 1000))
        value = str(random.randint(0, 1000))

        start = time.time()

        offset: int | None = None
        count = 0
        while True:
            count += 1
            try:
                offset = self.producer.produce(
                    topic=self.topic,
                    partition=self.partition,
                    key=key,
                    value=value,
                    timeout_s=timeout_s,
                )
                break
            except KafkaException as e:
                if count > retries:
                    raise
                kafka_error = e.args[0]
                if isinstance(kafka_error, KafkaError):
                    error_code = kafka_error.code()
                    if (
                        error_code != KafkaError._MSG_TIMED_OUT
                        and error_code != KafkaError._TIMED_OUT
                    ):
                        raise
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

        assert offset is not None
        self.consumer.read_until(expect(offset, key, value), timeout_s=timeout_s)
        latency = time.time() - start
        self.logger.info(
            f"ping_pong produced and consumed {key}={value}@{offset} in {(latency) * 1000.0:.2f} ms"
        )
