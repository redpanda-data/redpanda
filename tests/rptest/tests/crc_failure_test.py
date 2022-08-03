# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, RedpandaService


class ShutdownTest(EndToEndTest):
    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def crc_failure_repro_test(self):
        self.topic = TopicSpec(partition_count=1, replication_factor=3)
        rp_conf = {
            'enable_leader_balancer': False,
            'auto_create_topics_enabled': True
        }
        self.redpanda = RedpandaService(self.test_context,
                                        3,
                                        extra_rp_conf=rp_conf)
        self.redpanda.start()
        # Background load generation
        self.start_producer(2, 200000)
        time.sleep(3 * 60)
        self.producer.stop()
