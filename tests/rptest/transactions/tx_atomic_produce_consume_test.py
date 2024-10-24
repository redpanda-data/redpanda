# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading
from concurrent import futures
import time

from ducktape.mark import matrix
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
import confluent_kafka as ck
from rptest.services.failure_injector import FailureSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from ducktape.utils.util import wait_until

from rptest.utils.node_operations import FailureInjectorBackgroundThread


def deserialize_str(value: bytes, ctx) -> str:
    return value.decode('utf-8')


class ExactlyOnceVerifier():
    """
    A class populating topic with some data and then executing the transformation 
    of the data from source topic to the destination topic.
    When data are transformed we are using transactions to achieve the exactly 
    once semantics
    """
    def __init__(self,
                 redpanda,
                 src_topic: str,
                 partition_count: int,
                 dst_topic: str,
                 transform_func,
                 logger,
                 max_messages: int | None = None,
                 tx_id: str = "transformer-tx",
                 group_id: str = "transformer-group",
                 commit_every: int = 50,
                 progress_timeout_sec: int = 60):
        self._src_topic = src_topic
        self._partition_count = partition_count
        self._dst_topic = dst_topic
        self._transform_func = transform_func
        self._logger = logger
        self._redpanda = redpanda
        self._stop_ev = threading.Event()
        self._max_messages = max_messages
        self._tx_id = tx_id
        self._group_id = group_id
        self._commit_every = commit_every
        self._produced_to_src = 0
        self._lock = threading.Lock()
        self._tasks = []
        self._progress_timeout_sec = progress_timeout_sec
        self._error = None
        self._finished_partitions = set()
        self._consumer_cnt = 0

    def start_src_producer(self):
        produced_per_partition = defaultdict(int)
        self.last_report = time.time()

        def delivery_report(err, msg):
            if err is None:
                produced_per_partition[msg.partition()] += 1
                with self._lock:
                    self._produced_to_src += 1
                    now = time.time()
                    if now - self.last_report > 5:
                        self.last_report = now
                        self._logger.info(
                            f"Produced {self._produced_to_src} messages to source topic",
                        )

        producer = ck.Producer({'bootstrap.servers': self._redpanda.brokers()})

        self._logger.info(
            f"Starting source topic {self._src_topic} producer. Max messages to produce: {self._max_messages}"
        )
        i = 0
        while not self._stop_ev.is_set():
            producer.produce(self._src_topic,
                             value=str(i),
                             key=str(i),
                             on_delivery=delivery_report)
            i += 1
            if self._max_messages is not None and i >= self._max_messages:
                self._stop_ev.set()

        producer.flush()
        self._logger.info(f"Messages per partition: {produced_per_partition}")

    def produced_messages(self, to_wait: int):
        with self._lock:
            return self._produced_to_src >= to_wait

    def request_stop(self):
        self._stop_ev.set()

    def wait_for_finish(self, timeout_sec: int = 30):
        futures.wait(self._tasks,
                     timeout=timeout_sec,
                     return_when=futures.ALL_COMPLETED)

    def start_workload(self, workers: int):
        with ThreadPoolExecutor(max_workers=workers + 1) as executor:
            self._tasks.append(
                executor.submit(lambda: self.start_src_producer()))
            wait_until(lambda: self.produced_messages(10), 30, 0.5)

            def transform(id: str):
                try:
                    self.tx_transform(tx_id=id)
                except ck.KafkaError as e:
                    self._logger.error(
                        f"Client error reported: {e.error} - {e.reason}, retryable: {e.retryable}"
                    )
                except BaseException as e:
                    self._logger.error(
                        f"Error transforming {id} - {e} - {type(e)}")

            for i in range(workers):
                id = f"{self._tx_id}-{i}"
                self._tasks.append(executor.submit(transform, id))

    def get_high_watermarks(self, topic):
        rpk = RpkTool(self._redpanda)

        return {p.id: p.high_watermark for p in rpk.describe_topic(topic)}

    def tx_transform(self, tx_id):
        last_progress = time.time()
        self._logger.info(f"Starting tx-transform with tx_id: {tx_id}")
        producer = ck.Producer({
            'bootstrap.servers': self._redpanda.brokers(),
            'transactional.id': tx_id,
        })

        consumer = ck.DeserializingConsumer({
            'bootstrap.servers':
            self._redpanda.brokers(),
            'group.id':
            self._group_id,
            'auto.offset.reset':
            'earliest',
            'enable.auto.commit':
            False,
            'isolation.level':
            'read_committed',
            'value.deserializer':
            deserialize_str,
            'key.deserializer':
            deserialize_str,
        })
        try:
            with self._lock:
                self._consumer_cnt += 1
            last_positions = {}

            def made_progress(current_positions: dict):
                self._logger.info(
                    f"[{tx_id}] last positions: {last_positions}, current positions: {current_positions}"
                )
                for p_id, pos in current_positions.items():
                    if p_id in last_positions and pos > last_positions[p_id]:
                        return True
                    elif p_id not in last_positions:
                        return True
                return False

            def get_consumer_positions() -> list:
                assignments = {tp.partition for tp in consumer.assignment()}
                if len(assignments) == 0:
                    return []
                return consumer.position([
                    ck.TopicPartition(self._src_topic, p) for p in assignments
                ])

            def reached_end(positions):
                high_watermarks = self.get_high_watermarks(self._src_topic)

                assignments = {tp.partition for tp in consumer.assignment()}
                if len(assignments) == 0:
                    return False

                end_for = {
                    p.partition
                    for p in positions if p.partition in high_watermarks
                    and high_watermarks[p.partition] <= p.offset
                }
                self._logger.info(
                    f"[{tx_id}] Topic {self._src_topic} partitions high watermarks"
                    f"{high_watermarks}, assignment: {assignments} positions: {positions}, end_for: {end_for}"
                )

                consumers = 0
                with self._lock:
                    self._finished_partitions |= end_for
                    consumers = self._consumer_cnt
                    if len(self._finished_partitions) == self._partition_count:
                        return True

                return len(end_for) == len(assignments) and consumers > 1

            in_transaction = False

            def on_assign(consumer, partitions):
                self._logger.info(f"[{tx_id}] Assigned {partitions}")

            def on_revoke(consumer, partitions):
                nonlocal in_transaction
                self._logger.info(f"[{tx_id}] Revoked {partitions}")
                if in_transaction:
                    self._logger.info(f"[{tx_id}] abort transaction")
                    producer.abort_transaction()
                    in_transaction = False

            consumer.subscribe([self._src_topic],
                               on_assign=on_assign,
                               on_revoke=on_revoke)
            producer.init_transactions()

            msg_cnt = 0
            while True:
                msg = consumer.poll(timeout=1)
                if msg:
                    if not in_transaction:
                        self._logger.info(f"[{tx_id}] begin transaction")
                        producer.begin_transaction()
                        in_transaction = True
                    msg_cnt += 1
                    t_key, t_value = self._transform_func(
                        msg.key(), msg.value())

                    producer.produce(self._dst_topic,
                                     value=t_value,
                                     key=t_key,
                                     partition=msg.partition())

                    if msg_cnt % self._commit_every == 0:
                        self._logger.info(
                            f"[{tx_id}] Committing transaction after {msg_cnt} messages. "
                            f"Current consumer positions: {consumer.position(consumer.assignment())}"
                        )
                        producer.send_offsets_to_transaction(
                            consumer.position(consumer.assignment()),
                            consumer.consumer_group_metadata())
                        producer.commit_transaction()
                        in_transaction = False
                positions = get_consumer_positions()

                if reached_end(positions):
                    self._logger.info(f"{tx_id} reached end")
                    break
                positions_dict = {p.partition: p.offset for p in positions}

                if made_progress(positions_dict):
                    last_progress = time.time()

                last_positions = positions_dict

                if time.time() - last_progress > self._progress_timeout_sec:
                    self._logger.error(f"timeout waiting for {tx_id} producer")
                    self._error = TimeoutError(
                        "timeout waiting for {tx_id} producer")
                    self._stop_ev.set()
                    break

            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata())

            producer.commit_transaction()
            in_transaction = False
        except ck.KafkaError as e:
            self._logger.error(
                f"Client error reported: {e.error} - {e.reason}, retryable: {e.retryable}"
            )
            raise e
        finally:
            consumer.close()
            producer.flush()
            with self._lock:
                self._consumer_cnt -= 1

    def validate_transform(self):
        src_high_watermarks = self.get_high_watermarks(self._src_topic)
        dst_high_watermarks = self.get_high_watermarks(self._dst_topic)
        for p, hw in src_high_watermarks.items():
            assert dst_high_watermarks[
                p] >= hw, "Transformed partitions must the same or larger watermark than source"
        src_msgs = defaultdict(list)
        dst_msgs = defaultdict(list)
        consumer = ck.DeserializingConsumer({
            'bootstrap.servers':
            self._redpanda.brokers(),
            'group.id':
            f'validator-group-{self._group_id}',
            'auto.offset.reset':
            'earliest',
            'enable.auto.commit':
            True,
            'value.deserializer':
            deserialize_str,
            'key.deserializer':
            deserialize_str,
        })
        consumer.subscribe([self._src_topic, self._dst_topic])

        def is_finished():
            positions = consumer.position(consumer.assignment())
            if len(positions
                   ) != len(src_high_watermarks) + len(dst_high_watermarks):
                return False

            for tp in positions:
                if tp.topic == self._src_topic:
                    if tp.offset < src_high_watermarks[tp.partition]:
                        return False
                else:
                    if tp.offset < dst_high_watermarks[tp.partition]:
                        return False

            return True

        while True:
            if is_finished():
                break
            msg = consumer.poll(1)
            if msg is None or msg.error():
                continue
            p = msg.partition()
            if msg.topic() == self._src_topic:
                src_msgs[p].append(msg)
            else:
                dst_msgs[p].append(msg)

            if len(src_msgs[p]) > 0 and len(dst_msgs[p]) > 0:
                s_msg = src_msgs[p][0]
                d_msg = dst_msgs[p][0]
                self._logger.debug(
                    f"src: {s_msg.topic()}/{s_msg.partition()}@{s_msg.offset()} - {s_msg.key()}={s_msg.value()}"
                )
                self._logger.debug(
                    f"dst: {d_msg.topic()}/{d_msg.partition()}@{d_msg.offset()} - {d_msg.key()}={d_msg.value()}"
                )
                t_key, t_val = self._transform_func(s_msg.key(), s_msg.value())
                assert d_msg.value() == t_val
                assert d_msg.key() == t_key
                src_msgs[p].pop(0)
                dst_msgs[p].pop(0)


class TxAtomicProduceConsumeTest(RedpandaTest):
    def __init__(self, test_context):
        super(TxAtomicProduceConsumeTest,
              self).__init__(test_context=test_context, num_brokers=3)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    @matrix(with_failures=[True, False])
    def test_basic_tx_consumer_transform_produce(self, with_failures):
        partition_count = 5
        topic = TopicSpec(name='src-test-exactly-once',
                          replication_factor=3,
                          partition_count=partition_count)

        dst_topic = TopicSpec(name='dst-test-exactly-once',
                              replication_factor=3,
                              partition_count=partition_count)
        self.redpanda.set_cluster_config(
            {"group_new_member_join_timeout": "3000"})
        self.client().create_topic(topic)
        self.client().create_topic(dst_topic)
        fi = None
        if with_failures:
            fi = FailureInjectorBackgroundThread(
                self.redpanda,
                self.logger,
                max_inter_failure_time=30,
                min_inter_failure_time=10,
                max_suspend_duration_seconds=7)

        def simple_transform(k, v):
            return f"{k}-t", f"{v}-tv"

        transformer = ExactlyOnceVerifier(self.redpanda,
                                          topic.name,
                                          topic.partition_count,
                                          dst_topic.name,
                                          simple_transform,
                                          self.logger,
                                          2000,
                                          progress_timeout_sec=180)
        if fi:
            fi.start()
        transformer.start_workload(2)
        transformer.wait_for_finish()
        if fi:
            fi.stop()
        transformer.validate_transform()
