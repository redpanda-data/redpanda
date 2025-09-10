# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import threading
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import confluent_kafka as ck
from confluent_kafka import TopicPartition
from ducktape.utils.util import wait_until
from kafka import KafkaAdminClient

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec


class ConsumerOffsetsVerifier:
    """
    Populates consumer offsets topic with various transactional offset commits
    over multiple groups and verifies the final offset positions.
    Assumes that there is only one consumer offset partition for simplicity since
    the intention of this test is correctness.

    The verifier does not use a real topic and consumer to generate offset commits,
    instead dummy offsets are generated randomly to mimic consumption. A real consumer
    only adds noise to the test and is not needed to validate correctness here.
    """

    def __init__(
        self,
        redpanda,
        client,
        produce_topic: str = "topic_produce",
        source_topic: str = "topic_consume",
        num_producers: int = 10,
        num_src_partitions: int = 5,
        max_commits: int = 5000,
    ):
        self._redpanda = redpanda
        self._topic = produce_topic
        self._source_topic = source_topic
        self._logger = self._redpanda.logger
        self._lock = threading.Lock()
        self._tasks = []
        self._num_producers = num_producers
        self._num_src_partitions = num_src_partitions

        produce_topic_spec = TopicSpec(
            name=produce_topic, replication_factor=3, partition_count=1
        )

        consume_topic_spec = TopicSpec(
            name=source_topic, replication_factor=3, partition_count=5
        )
        client.create_topic(produce_topic_spec)
        client.create_topic(consume_topic_spec)

        self.rpk = RpkTool(self._redpanda)

        # Each producers uses a group and each group has offset positions
        # for every source partition
        self._committed_offsets: dict[str, list[TopicPartition]] = dict()
        self._stop_ev = threading.Event()
        for producer in range(num_producers):
            self._committed_offsets[f"group-{producer}"] = [
                TopicPartition(self._source_topic, p, -1)
                for p in range(num_src_partitions)
            ]

        self._total_commits_so_far = 0
        self._max_commits = max_commits
        self._commits_done = threading.Event()
        threading.Thread(target=self._start_producers, daemon=True).start()

    def _start_producers(self):
        with ThreadPoolExecutor(max_workers=self._num_producers) as executor:
            for producer in range(self._num_producers):
                self._tasks.append(
                    executor.submit(
                        lambda: self._start_one_producer(
                            group_id=f"group-{producer}", tx_id=f"txid-{producer}"
                        )
                    )
                )

    def _stop_all(self, timeout_sec: int = 30):
        if self._stop_ev.isSet():
            return
        self._stop_ev.set()
        futures.wait(
            self._tasks, timeout=timeout_sec, return_when=futures.ALL_COMPLETED
        )

    def _current_committed_offsets(self, group_id: str, partitions: list[int]):
        with self._lock:
            return [
                tp
                for tp in self._committed_offsets[group_id]
                if tp.partition in partitions
            ]

    def _update_committed_offsets(self, group_id: str, positions: list[TopicPartition]):
        with self._lock:
            for position in positions:
                self._committed_offsets[group_id][position.partition] = position
            self._total_commits_so_far += 1
            if self._total_commits_so_far >= self._max_commits:
                self._commits_done.set()

    def _group_is_ready(self, group: str):
        gr = self.rpk.group_describe(group=group, summary=True)
        return gr.members == 1 and gr.state == "Stable"

    def _start_one_producer(self, group_id: str, tx_id: str):
        consumer = ck.Consumer(
            {
                "bootstrap.servers": self._redpanda.brokers(),
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        consumer.subscribe([self._source_topic])

        wait_until(
            lambda: self._group_is_ready(group=group_id),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Timed out waiting for group {group_id} to be stable",
        )

        producer = ck.Producer(
            {
                "bootstrap.servers": self._redpanda.brokers(),
                "transactional.id": tx_id,
                "transaction.timeout.ms": 10000,
            }
        )
        producer.init_transactions()

        def generate_dummy_positions():
            # pick a random list of partitions to update
            partitions = random.sample(range(0, self._num_src_partitions), 3)
            current_offsets = self._current_committed_offsets(
                group_id=group_id, partitions=partitions
            )
            # update positions
            for tp in current_offsets:
                tp.offset = tp.offset + random.randint(1, 5)
            return current_offsets

        i = 0
        while not self._stop_ev.isSet():
            new_positions = generate_dummy_positions()
            self._logger.debug(
                f"[{tx_id}] attempting to update positions to {new_positions}"
            )
            producer.begin_transaction()
            producer.produce(self._topic, f"{tx_id}_{i}", f"{tx_id}_id")
            producer.send_offsets_to_transaction(
                new_positions, consumer.consumer_group_metadata()
            )
            producer.flush()

            commit = random.choice([True, False])
            if commit:
                producer.commit_transaction()
                self._update_committed_offsets(group_id, new_positions)
                self._logger.debug(
                    f"[{tx_id}] attempting to update positions to {new_positions} succeeded total commits: {self._total_commits_so_far}"
                )
            else:
                producer.abort_transaction()
                self._logger.debug(
                    f"[{tx_id}] attempting to update positions to {new_positions} aborted"
                )
            sleep(0.05)

    def verify(self, timeout_sec: int = 90):
        self._logger.debug("waiting for commits done")
        self._commits_done.wait(timeout_sec)
        self._stop_all(timeout_sec)

        self._logger.debug("Verifying offsets for all groups")

        admin = KafkaAdminClient(**{"bootstrap_servers": self._redpanda.brokers()})

        def list_offsets(group_id: str):
            offsets = admin.list_consumer_group_offsets(group_id)
            result = []
            for tp, md in offsets.items():
                result.append(TopicPartition(tp.topic, tp.partition, md.offset))
            return sorted(result, key=lambda tp: tp.partition)

        def offsets_are_consistent():
            try:
                group_results = []
                for group in [f"group-{p}" for p in range(self._num_producers)]:
                    offsets = list_offsets(group)
                    expected = self._committed_offsets[group]
                    self._logger.debug(
                        f"group: {group}, offsets: {offsets}, expected: {expected}"
                    )
                    group_results.append(offsets == expected)
                return all(group_results)
            except Exception as e:
                self._logger.debug(f"exception listing offsets: {e}")
                return False

        wait_until(
            offsets_are_consistent,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timed out waiting group offsets to be consistent.",
        )
