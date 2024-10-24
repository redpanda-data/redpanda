# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading
import time
from rptest.clients.consumer_offsets_recovery import ConsumerOffsetsRecovery
from rptest.services.cluster import cluster

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize

import re


class ConsumerOffsetsConsistencyTest(PreallocNodesTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx

        super(ConsumerOffsetsConsistencyTest,
              self).__init__(test_ctx,
                             num_brokers=3,
                             *args,
                             node_prealloc_count=1,
                             **kwargs)
        self.rpk = RpkTool(self.redpanda)

    @property
    def timeout_sec(self):
        return 45 if not self.debug_mode else 90

    def get_offsets(self, group_name):
        offsets = {}

        def do_describe():
            gd = self.rpk.group_describe(group_name)
            return (True, gd)

        gd = wait_until_result(
            do_describe,
            timeout_sec=self.timeout_sec,
            backoff_sec=0.5,
            err_msg="RPK failed to get consumer group offsets",
            retry_on_exc=True)

        for p in gd.partitions:
            if p.current_offset is not None:
                offsets[f"{p.topic}/{p.partition}"] = p.current_offset
        return offsets

    def get_group(self):

        # The one consumer group in this test comes from KgoVerifierConsumerGroupConsumer
        # for example, kgo-verifier-1691097745-347-0
        kgo_group_re = re.compile(r'^kgo-verifier-[0-9]+-[0-9]+-0$')

        def do_list_groups():
            res = self.rpk.group_list_names()

            if res is None or len(res) == 0:
                return False

            kgo_group_m = kgo_group_re.match(res[0])

            return False if kgo_group_m is None else (True, res)

        group_list_res = wait_until_result(
            do_list_groups,
            timeout_sec=self.timeout_sec,
            backoff_sec=0.5,
            err_msg="RPK failed to list consumer groups",
            retry_on_exc=True)

        return group_list_res[0]

    @property
    def msg_size(self):
        return 16

    @property
    def msg_count(self):
        return int(20 * self.producer_throughput / self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 1024 * 1024

    @cluster(num_nodes=4)
    def test_flipping_leadership(self):
        topic = TopicSpec(partition_count=64, replication_factor=3)
        self.client().create_topic([topic])
        # set new members join timeout to 5 seconds to make the test execution faster
        self.redpanda.set_cluster_config(
            {"group_new_member_join_timeout": 5000})

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic.name,
                                       self.msg_size,
                                       self.msg_count,
                                       custom_node=self.preallocated_nodes,
                                       rate_limit_bps=self.producer_throughput)

        producer.start(clean=False)

        wait_until(lambda: producer.produce_status.acked > 10,
                   timeout_sec=self.timeout_sec,
                   backoff_sec=0.5)

        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic.name,
            self.msg_size,
            readers=3,
            nodes=self.preallocated_nodes,
            loop=True)
        consumer.start(clean=False)

        group_name = self.get_group()

        offsets = self.get_offsets(group_name)

        stop_ev = threading.Event()
        lock = threading.Lock()
        self.failure_cnt = 0
        self.successes = 0

        def fi_worker():
            with FailureInjector(self.redpanda) as f_injector:
                last_success = 0
                while not stop_ev.is_set():
                    node = random.choice(self.redpanda.started_nodes())
                    f_injector.inject_failure(
                        FailureSpec(node=node,
                                    type=random.choice([
                                        FailureSpec.FAILURE_KILL,
                                        FailureSpec.FAILURE_TERMINATE
                                    ]),
                                    length=0))
                    successes = 0
                    with lock:
                        successes = self.successes

                    while last_success >= successes:
                        time.sleep(random.randint(1, 2))
                        with lock:
                            successes = self.successes

                    with lock:
                        self.failure_cnt += 1
                        last_success = self.successes

        self.thread = threading.Thread(target=lambda: fi_worker(),
                                       args=(),
                                       daemon=True)
        self.thread.start()
        self.last_success = time.time()

        try:
            while True:
                try:
                    new_offsets = self.get_offsets(group_name)
                except Exception as e:
                    self.logger.info(
                        f"unable to retrieve group description - {e}")
                    continue

                for p, committed_offset in new_offsets.items():
                    if committed_offset is None:
                        continue
                    if p in offsets:
                        assert offsets[
                            p] <= committed_offset, "Offsets moved backward"

                    offsets[p] = committed_offset

                if len(new_offsets) > 0:
                    with lock:
                        self.last_success = time.time()
                        self.successes += 1

                with lock:
                    self.logger.info(
                        f"injected {self.failure_cnt} failures, successes: {self.successes}"
                    )
                    if self.failure_cnt >= 20:
                        break
                    timeout = 120
                    if time.time() - self.last_success > timeout:
                        assert False, f"Unable to retrieve group description for {timeout} seconds"
        finally:
            self.logger.info("stopping injector")
            stop_ev.set()

        self.logger.info("stopping producer")
        producer.stop()
        self.logger.info("waiting for consumer")
        consumer.wait()
