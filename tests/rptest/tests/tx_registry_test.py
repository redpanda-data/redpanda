# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.util import wait_until_result
from rptest.clients.types import TopicSpec

import random

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
import confluent_kafka as ck


class TransactionsMixin:
    def on_delivery_passes(self, err, msg):
        assert err == None, msg

    def on_delivery_fails(self, err, msg):
        assert err != None, err


class TxRegistryTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(TxRegistryTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf,
                                             log_level="trace")

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    def find_coordinator_inits_tx_registry_test(self):
        def find_tx_coordinator_passes():
            r = self.admin.find_tx_coordinator(
                "tx0", node=random.choice(self.redpanda.started_nodes()))
            return r["ec"] == 0

        def describe_tx_registry():
            state = self.admin.describe_tx_registry()
            return state["ec"] != 11, state

        wait_until(find_tx_coordinator_passes, timeout_sec=10, backoff_sec=1)

        info = wait_until_result(describe_tx_registry,
                                 timeout_sec=10,
                                 backoff_sec=1)
        assert info["ec"] == 0
        assert info["version"] == 0
        partitions = set()
        ranges = list()
        for (partition_id, hosted_txs) in info["tx_mapping"].items():
            partitions.add(partition_id)
            for hash_range in hosted_txs["hash_ranges"]:
                ranges.append(hash_range)
        assert len(partitions) > 0
        assert max(partitions) + 1 == len(partitions)
        assert min(partitions) == 0
        assert len(ranges) > 0
        ranges = sorted(ranges, key=lambda x: x["first"])
        assert ranges[0]["first"] == 0
        assert ranges[-1]["last"] == 4294967295
        for i in range(1, len(ranges)):
            assert ranges[i - 1]["last"] + 1 == ranges[i]["first"]
