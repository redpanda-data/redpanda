# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, make_redpanda_service
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.util import wait_until_result


class ShutdownTest(EndToEndTest):
    def __init__(self, test_context):
        super().__init__(test_context)
        self.stopped = threading.Event()
        self.background_failures = None

    def teardown(self):
        self.stopped.set()
        if self.background_failures:
            self.background_failures.join()  # Wait for ip tables rules reset.

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_timely_shutdown_with_failures(self):
        '''
        Validates that a leader node can shutdown in a timely manner when
        a follower node is unresponsive. Specifically the test targets
        timely cleanup of the consensus class in flaky conditions.
        '''
        self.topic = TopicSpec(partition_count=1, replication_factor=3)
        rp_conf = {
            'enable_leader_balancer': False,
            'auto_create_topics_enabled': True
        }
        self.redpanda = make_redpanda_service(self.test_context,
                                              3,
                                              extra_rp_conf=rp_conf)
        admin = Admin(self.redpanda)

        def checked_get_leader():
            try:
                leader = admin.get_partition_leader(namespace="kafka",
                                                    topic=self.topic.name,
                                                    partition=0)
                return (True,
                        next(
                            filter(lambda n: self.redpanda.idx(n) == leader,
                                   self.redpanda.nodes)))
            except:
                return False

        self.redpanda.start()
        # Background load generation
        self.start_producer(2)
        # Wait until a leader is available.
        wait_until(lambda: checked_get_leader(),
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Leader not found for ntp: {self.topic}/0")

        def pause_non_leader_node():
            """Picks a non leader node for the ntp and isolates it repeatedly"""
            with FailureInjector(self.redpanda) as finjector:
                timeout_s = 5
                period_s = 1
                failure = FailureSpec.FAILURE_ISOLATE
                while not self.stopped.is_set():
                    node = random.choice(self.redpanda.nodes)
                    assert node
                    if node == checked_get_leader():
                        continue

                    # Suspend for longer than send timeouts
                    finjector.inject_failure(
                        FailureSpec(node=node, type=failure, length=timeout_s))
                    self.stopped.wait(timeout=period_s)

        # Inject failures in the background.
        self.background_failures = threading.Thread(
            target=pause_non_leader_node, args=(), daemon=True)
        self.background_failures.start()
        try:
            pending_attempts = 10
            while pending_attempts != 0:
                # Pick the current leader and restart it, repeat
                leader = wait_until_result(
                    lambda: checked_get_leader(),
                    timeout_sec=30,
                    backoff_sec=2,
                    err_msg=f"Leader not found for ntp: {self.topic}/0")

                assert leader
                self.redpanda.logger.info(
                    f"Restarting leader node {leader.account.hostname}")
                self.redpanda.restart_nodes(leader)
                pending_attempts -= 1
        finally:
            # Stop the finjector
            self.stopped.set()
            self.background_failures.join()

        self.producer.stop()
