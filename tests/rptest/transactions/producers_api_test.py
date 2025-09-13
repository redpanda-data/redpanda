# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import threading
from time import sleep

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.transactions.verifiers.consumer_offsets_verifier import (
    ConsumerOffsetsVerifier,
)


class ProducersAdminAPITest(RedpandaTest):
    def __init__(self, test_context):
        super(ProducersAdminAPITest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                "group_topic_partitions": 1,
                "group_new_member_join_timeout": 3000,
                "enable_leader_balancer": False,
            },
        )
        self._stop_scraping = threading.Event()

    def get_producer_state(self, topic: str):
        admin = self.redpanda._admin
        return admin.get_producers_state(namespace="kafka", topic=topic, partition=0)

    @cluster(num_nodes=3)
    def test_producers_state_api_during_load(self):
        verifier = ConsumerOffsetsVerifier(self.redpanda, self._client)
        self.redpanda._admin.await_stable_leader(topic="__consumer_offsets")
        self.redpanda._admin.await_stable_leader(topic=verifier._topic)

        # Run a scraper in background as the verifier is running to ensure it doesn't
        # interact with the workloads incorrectly
        def scraper():
            while not self._stop_scraping.isSet():
                self.get_producer_state(verifier._topic)
                self.get_producer_state("__consumer_offsets")
                sleep(1)

        bg_scraper = threading.Thread(target=scraper, daemon=True)
        bg_scraper.start()
        verifier.verify()
        self._stop_scraping.set()
        bg_scraper.join()

        # Basic sanity checks
        co_producers = self.get_producer_state("__consumer_offsets")
        assert len(co_producers["producers"]) == 10, (
            "Not all producers states found in consumer_offsets partition"
        )
        expected_groups = set([f"group-{i}" for i in range(10)])
        state_groups = set(
            [producer["transaction_group_id"] for producer in co_producers["producers"]]
        )
        assert expected_groups == state_groups, (
            f"Not all groups reported. expected: {expected_groups}, repoted: {state_groups}"
        )

        topic_producers = self.get_producer_state(verifier._topic)
        assert len(topic_producers["producers"]) == 10, (
            "Not all producers states found in data topic partition"
        )
