# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import threading
import confluent_kafka as ck
from confluent_kafka import TopicPartition
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from concurrent import futures
from kafka import KafkaAdminClient
import random
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from ducktape.utils.util import wait_until
from rptest.transactions.verifiers.consumer_offsets_verifier import ConsumerOffsetsVerifier
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from time import sleep


class ConsumerOffsetsLoadGenerator():
    def __init__(self,
                 redpanda,
                 client,
                 source_topic: str = "foo",
                 num_groups: int = 10,
                 num_partitions: int = 1,
                 leadership_transfer_frequency_s: int = 2):

        self._redpanda = redpanda
        self._topic = source_topic
        self._source_topic = source_topic
        self._num_partitions = num_partitions
        self._logger = self._redpanda.logger
        self._stop_leadership_transfer = False
        self._leadership_transfer_frequency_s = leadership_transfer_frequency_s
        self._lock = threading.Lock()
        self._tasks = []
        self._num_groups = num_groups
        self._errors = []

        topic_spec = TopicSpec(name=self._topic,
                               replication_factor=3,
                               partition_count=num_partitions)
        client.create_topic(topic_spec)

        self.rpk = RpkTool(self._redpanda)

        # group -> list of TopicPartition (each TopicPartition has an offset)
        self._committed_offsets: dict[str, list[TopicPartition]] = dict()
        self._stop_ev = threading.Event()
        # Seed initial values
        for group in range(num_groups):
            self._committed_offsets[f"group-{group}"] = [
                TopicPartition(self._source_topic, p, -1)
                for p in range(num_partitions)
            ]

        threading.Thread(target=self._start_offset_committers,
                         daemon=True).start()

    def _start_offset_committers(self):
        with ThreadPoolExecutor(max_workers=self._num_groups + 2) as executor:
            for group in range(self._num_groups):
                self._tasks.append(
                    executor.submit(lambda: self._start_one_committer(
                        group_id=f"group-{group}")))
            self._tasks.append(executor.submit(self._transfer_loop))
            self._tasks.append(executor.submit(self._start_verifier))

    def _stop_all(self, timeout_sec: int = 30):
        if self._stop_ev.isSet():
            return
        self._stop_ev.set()
        _stop_leadership_transfer = True
        futures.wait(self._tasks,
                     timeout=timeout_sec,
                     return_when=futures.ALL_COMPLETED)

    def _current_committed_offsets(self, group_id: str, partitions: list[int]):
        with self._lock:
            return [
                tp for tp in self._committed_offsets[group_id]
                if tp.partition in partitions
            ]

    def _update_committed_offsets_map(self, group_id: str,
                                      positions: list[TopicPartition]):
        with self._lock:
            for position in positions:
                if position.error is None:
                    self._committed_offsets[group_id][
                        position.partition] = position

    def _group_is_ready(self, group: str):
        gr = self.rpk.group_describe(group=group, summary=True)
        return gr.members == 1 and gr.state == "Stable"

    def _start_verifier(self):
        self._logger.debug("Verifying offsets for all groups")

        admin = KafkaAdminClient(
            **{
                'bootstrap_servers': self._redpanda.brokers(),
                "isolation_level": "read_committed"
            })

        def list_offsets(group_id: str):
            offsets = admin.list_consumer_group_offsets(group_id)
            result = []
            for tp, md in offsets.items():
                result.append(TopicPartition(tp.topic, tp.partition,
                                             md.offset))
            return sorted(result, key=lambda tp: tp.partition)

        def are_group_offsets_consistent(expected: list[TopicPartition],
                                         got: list[TopicPartition],
                                         strict: bool = False):
            # Inefficient for large lists, but we don't expect many partitions
            expected_map = {
                (tp.topic, tp.partition): tp.offset
                for tp in expected
            }
            got_map = {(tp.topic, tp.partition): tp.offset for tp in got}
            for k, offset in got_map.items():
                expected_offset = expected_map.get(k, -1)
                if strict:
                    if offset != expected_offset:
                        return False
                else:
                    if offset < expected_offset:
                        # violation
                        return False
            return True

        def offsets_are_consistent():
            try:
                group_results = []
                for group in [f"group-{p}" for p in range(self._num_groups)]:
                    offsets = list_offsets(group)
                    expected = None
                    with self._lock:
                        expected = self._committed_offsets[group].copy()
                    assert expected
                    self._logger.debug(
                        f"group: {group}, offsets: {offsets}, expected: {expected}"
                    )
                    matched = are_group_offsets_consistent(expected,
                                                           offsets,
                                                           strict=False)
                    group_results.append(matched)
                    if not matched:
                        self._errors.append(
                            f"Offsets for group {group} are not consistent [{offsets} != {expected}]"
                        )
                return all(group_results)
            except Exception as e:
                self._logger.warn(f"exception listing offsets: {e}")
                return True

        while not self._stop_ev.isSet():
            self._redpanda.logger.debug("Verifying offsets iteration")
            try:
                if not offsets_are_consistent():
                    self._redpanda.logger.error(
                        "Offsets are not consistent, stopping test")
                    self._stop_ev.set()
            except Exception as e:
                self._redpanda.logger.error(e, exc_info=True)
            sleep(0.5)

    def _start_one_committer(self, group_id: str):

        consumer = ck.Consumer({
            'bootstrap.servers': self._redpanda.brokers(),
            'group.id': group_id,
            'auto.offset.reset': 'error',
            'enable.auto.commit': False,
        })

        consumer.subscribe([self._source_topic])

        wait_until(
            lambda: self._group_is_ready(group=group_id),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Timed out waiting for group {group_id} to be stable")

        def generate_dummy_positions():
            current_offsets = self._current_committed_offsets(
                group_id=group_id,
                partitions=list(range(0, self._num_partitions)))
            # update positions
            for tp in current_offsets:
                tp.offset = tp.offset + random.randint(1, 5)
            return current_offsets

        while not self._stop_ev.isSet():
            new_positions = generate_dummy_positions()
            consumer.commit(offsets=new_positions, asynchronous=False)
            self._update_committed_offsets_map(group_id, new_positions)

    def _transfer_loop(self):
        admin = self._redpanda._admin
        self._redpanda.logger.debug("Starting leadership transfer loop")
        while not self._stop_ev.isSet():
            try:
                leader_node = self._redpanda.get_node(
                    admin.await_stable_leader(topic="__consumer_offsets",
                                              partition=0,
                                              namespace="kafka",
                                              timeout_s=30))
                admin.partition_transfer_leadership(namespace="kafka",
                                                    topic="__consumer_offsets",
                                                    partition="0")
            except Exception as e:
                self._redpanda.logger.debug(e, exc_info=True)
                pass
            sleep(self._leadership_transfer_frequency_s)
        self._redpanda.logger.debug("Exiting leadership transfer loop")

    def run_for(self, timeout_sec: int = 300):
        self._stop_ev.wait(timeout_sec)
        self._stop_all(timeout_sec)
        assert len(self._errors) == 0, self._errors


class ReproConsumerOffsetsIssue(RedpandaTest):
    def __init__(self, test_context):
        super(ReproConsumerOffsetsIssue,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf={
                                 "group_topic_partitions": 1,
                                 "log_segment_size": 1024 * 10,
                                 "log_compaction_interval_ms": 10,
                                 "group_new_member_join_timeout": 3000,
                                 "group_initial_rebalance_delay": 0
                             })

    @cluster(num_nodes=3)
    def test_repro_test(self):
        ConsumerOffsetsLoadGenerator(self.redpanda,
                                     self._client).run_for(timeout_sec=20 * 60)
