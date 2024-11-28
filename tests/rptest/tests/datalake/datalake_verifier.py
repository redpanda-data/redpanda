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
import random
import threading
import time
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import RedpandaService
from rptest.tests.datalake.query_engine_base import QueryEngineBase
from rptest.util import wait_until

from confluent_kafka import Consumer


class DatalakeVerifier():
    """
     Verifier that does the verification of the data in the redpanda Iceberg table. 
     The verifier consumes offsets from specified topic and verifies it the data 
     in the iceberg table matches.
     
     The verifier runs two threads: 
     - one of them consumes messages from the specified topic and buffers them in memory. 
       The semaphore is used to limit the number of messages buffered in memory.
       
     - second thread executes a per partition query that fetches the messages 
       from the iceberg table
    """

    #TODO: add an ability to pass lambda to verify the message content
    #TODO: add tolerance for compacted topics
    def __init__(self, redpanda: RedpandaService, topic: str,
                 query_engine: QueryEngineBase):
        self.redpanda = redpanda
        self.topic = topic
        self.logger = redpanda.logger
        self._consumed_messages = defaultdict(list)

        self._query: QueryEngineBase = query_engine
        self._cg = f"verifier-group-{random.randint(0, 1000000)}"
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # number of messages buffered in memory
        self._msg_semaphore = threading.Semaphore(5000)
        self._total_msgs_cnt = 0
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._rpk = RpkTool(self.redpanda)
        # errors found during verification
        self._errors = []
        # map of last queried offset for each partition
        self._max_queried_offsets = {}
        self._last_checkpoint = {}

    def create_consumer(self):
        c = Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': self._cg,
            'auto.offset.reset': 'earliest'
        })
        c.subscribe([self.topic])

        return c

    def _consumer_thread(self):
        self.logger.info("Starting consumer thread")
        consumer = self.create_consumer()
        while not self._stop.is_set():

            self._msg_semaphore.acquire()
            if self._stop.is_set():
                break
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                self.logger.error(f"Consumer error: {msg.error()}")
                continue

            with self._lock:
                self._total_msgs_cnt += 1
                self.logger.debug(
                    f"Consumed message partition: {msg.partition()} at offset {msg.offset()}"
                )
                self._consumed_messages[msg.partition()].append(msg)
                if len(self._errors) > 0:
                    return

        consumer.close()

    def _get_query(self, partition, last_queried_offset, max_consumed_offset):
        return f"\
        SELECT redpanda.offset FROM redpanda.{self._query.escape_identifier(self.topic)} \
        WHERE redpanda.partition={partition} \
        AND redpanda.offset>{last_queried_offset} \
        AND redpanda.offset<={max_consumed_offset} \
        ORDER BY redpanda.offset"

    def _verify_next_message(self, partition, iceberg_offset):
        if partition not in self._consumed_messages:
            self._errors.append(
                f"Partition {partition} returned from Iceberg query  not found in consumed messages"
            )

        p_messages = self._consumed_messages[partition]

        if len(p_messages) == 0:
            return

        consumer_offset = self._consumed_messages[partition][0].offset()
        if iceberg_offset > consumer_offset:
            self._errors.append(
                f"Offset from Iceberg table {iceberg_offset} is greater than next consumed offset {consumer_offset} for partition {partition}, most likely there is a gap in the table"
            )
            return

        if iceberg_offset <= self._max_queried_offsets.get(partition, -1):
            self._errors.append(
                f"Duplicate entry detected at offset {iceberg_offset} for partition {partition} "
            )
            return

        self._max_queried_offsets[partition] = iceberg_offset

        if consumer_offset != iceberg_offset:
            self._errors.append(
                f"Offset from iceberg table {iceberg_offset} for {partition} does not match the next consumed offset {consumer_offset}"
            )
            return
        self._consumed_messages[partition].pop(0)
        self._msg_semaphore.release()

    def _get_partition_offsets_list(self):
        with self._lock:
            return [(partition, messages[-1].offset())
                    for partition, messages in self._consumed_messages.items()
                    if len(messages) > 0]

    def _query_thread(self):
        self.logger.info("Starting query thread")
        while not self._stop.is_set():
            try:
                partitions = self._get_partition_offsets_list()

                for partition, max_consumed in partitions:
                    last_queried_offset = self._max_queried_offsets[
                        partition] if partition in self._max_queried_offsets else -1

                    # no new messages consumed, skip query
                    if max_consumed <= last_queried_offset:
                        continue

                    query = self._get_query(partition, last_queried_offset,
                                            max_consumed)
                    self.logger.debug(f"Executing query: {query}")

                    with self._query.run_query(query) as cursor:
                        with self._lock:
                            for row in cursor:
                                self._verify_next_message(partition, row[0])
                                if len(self._errors) > 0:
                                    self.logger.error(
                                        f"violations detected: {self._errors}, stopping verifier"
                                    )
                                    return

                    if len(self._max_queried_offsets) > 0:
                        self.logger.debug(
                            f"Max queried offsets: {self._max_queried_offsets}"
                        )

            except Exception as e:
                self.logger.error(f"Error querying iceberg table: {e}")
                time.sleep(2)

    def start(self):
        self._executor.submit(self._consumer_thread)
        self._executor.submit(self._query_thread)

    def _all_offsets_translated(self):
        partitions = self._rpk.describe_topic(self.topic)
        with self._lock:
            for p in partitions:
                if p.id not in self._max_queried_offsets:
                    self.logger.debug(
                        f"partition {p.id} not found in max offsets: {self._max_queried_offsets}"
                    )
                    return False

                if self._max_queried_offsets[p.id] < p.high_watermark - 1:
                    self.logger.debug(
                        f"partition {p.id} high watermark: {p.high_watermark}, max offset: {self._max_queried_offsets[p.id]}"
                    )
                    return False
        return True

    def _made_progress(self):
        progress = False
        with self._lock:
            for partition, offset in self._max_queried_offsets.items():
                if offset > self._last_checkpoint.get(partition, -1):
                    progress = True
                    break

            self._last_checkpoint = self._max_queried_offsets.copy()
        return progress

    def wait(self, progress_timeout_sec=30):
        try:
            while not self._all_offsets_translated():
                wait_until(
                    lambda: self._made_progress(),
                    progress_timeout_sec,
                    backoff_sec=3,
                    err_msg=
                    f"Error waiting for the query to make progress for topic {self.topic}"
                )
                assert len(
                    self._errors
                ) == 0, f"Topic {self.topic} validation errors: {self._errors}"

        finally:
            self.stop()

    def stop(self):
        self._stop.set()
        self._msg_semaphore.release()
        self._executor.shutdown(wait=False)
        assert len(
            self._errors
        ) == 0, f"Topic {self.topic} validation errors: {self._errors}"

        assert all(
            len(messages) == 0 for messages in self._consumed_messages.values(
            )), f"Partition left with consumed but not translated messages"
