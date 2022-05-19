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
import time

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, RedpandaService
from rptest.clients.default import DefaultClient
from ducktape.utils.util import wait_until
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer

from ducktape.mark import matrix


class ControllerUpgradeTest(EndToEndTest):
    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_updating_cluster_when_executing_operations(self):
        '''
        Validates that cluster is operational when upgrading controller log
        '''

        # set redpanda logical version to value without __consumer_offsets support
        self.redpanda = RedpandaService(
            self.test_context,
            5,
            # set redpanda logical version to v22.1 - last version without serde encoding
            environment={"__REDPANDA_LOGICAL_VERSION": 3})

        self.redpanda.start()
        admin_fuzz = AdminOperationsFuzzer(self.redpanda)
        self._client = DefaultClient(self.redpanda)

        spec = TopicSpec(partition_count=6, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=5000)
        self.start_consumer(1)
        self.await_startup()
        admin_fuzz.start()

        def cluster_is_stable():
            admin = Admin(self.redpanda)
            brokers = admin.get_brokers()
            if len(brokers) < 3:
                return False

            for b in brokers:
                self.logger.debug(f"broker:  {b}")
                if not (b['is_alive'] and 'disk_space' in b):
                    return False

            return True

        # reset environment to use latest redpanda logical version
        self.redpanda.set_environment({})
        for n in self.redpanda.nodes:

            self.redpanda.restart_nodes(n, stop_timeout=60)

            # wait for leader balancer to start evening out leadership
            wait_until(cluster_is_stable, 90, backoff_sec=2)

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=180)
        admin_fuzz.wait(20, 240)
        admin_fuzz.stop()
