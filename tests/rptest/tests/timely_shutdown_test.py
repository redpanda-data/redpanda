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
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, RedpandaService
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector, FailureSpec


class ShutdownTest(EndToEndTest):
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
        self.redpanda = RedpandaService(self.test_context,
                                        3,
                                        extra_rp_conf=rp_conf)
        admin = Admin(self.redpanda)

        def checked_get_leader():
            try:
                leader = admin.get_partition_leader(namespace="kafka",
                                                    topic=self.topic.name,
                                                    partition=0)
                return next(
                    filter(lambda n: self.redpanda.idx(n) == leader,
                           self.redpanda.nodes))
            except:
                return None

        self.redpanda.start()
        # Background load generation
        self.start_producer(2)
        # Wait until a leader is available.
        wait_until(lambda: checked_get_leader(),
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Leader not found for ntp: {self.topic}/0")

        stopped = False

        def pause_non_leader_node():
            """Picks a non leader node for the ntp and isolates it repeatedly"""
            with FailureInjector(self.redpanda) as finjector:
                timeout_s = 5
                failure = FailureSpec.FAILURE_ISOLATE
                while not stopped:
                    node = random.choice(self.redpanda.nodes)
                    assert node
                    if node == checked_get_leader():
                        continue
                    # Suspend for longer than send timeouts
                    finjector.inject_failure(
                        FailureSpec(node=node, type=failure, length=timeout_s))

        # Inject failures in the background.
        background_failures = threading.Thread(target=pause_non_leader_node,
                                               args=(),
                                               daemon=True)
        background_failures.start()
        pending_attempts = 5
        while pending_attempts != 0:
            # Pick the current leader and restart it, repeat
            wait_until(lambda: checked_get_leader(),
                       timeout_sec=30,
                       backoff_sec=2,
                       err_msg=f"Leader not found for ntp: {self.topic}/0")
            leader = checked_get_leader()
            assert leader
            self.redpanda.logger.info(
                f"Restarting leader node {leader.account.hostname}")
            self.redpanda.restart_nodes(leader)
            pending_attempts -= 1

        # Stop the finjector
        stopped = True
        background_failures.join()  # Wait for ip tables rules reset.
        assert not background_failures.is_alive()
        self.producer.stop()
