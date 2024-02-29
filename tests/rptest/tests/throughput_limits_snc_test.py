# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random, time, math, json, string
from enum import Enum
from typing import Tuple

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.kcat_consumer import KcatConsumer
from rptest.clients.kafka_cat import KafkaCat

# This file is about throughput limiting that works at shard/node/cluster (SNC)
# levels, like cluster-wide and node-wide throughput limits

kiB = 1024
MiB = 1024 * kiB


class ThroughputLimitsSnc(RedpandaTest):
    """
    Tests for throughput limiting that works at shard/node/cluster (SNC)
    levels, like cluster-wide and node-wide throughput limits
    """
    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        super(ThroughputLimitsSnc, self).__init__(test_ctx,
                                                  num_brokers=3,
                                                  *args,
                                                  **kwargs)
        rnd_seed_override = test_ctx.globals.get("random_seed")
        if rnd_seed_override is None:
            # default seed value is composed from
            # - current time - the way the default generator ctor does
            # - hash of the redpanda service instance - to avoid possible
            #   identical seeds while running multiple tests in parallel
            #   with --repeat
            self.rnd_seed = int(time.time() * 1_000_000_000) + hash(
                self.redpanda.service_id)
        else:
            self.rnd_seed = rnd_seed_override
        self.logger.info(f"Random seed: {self.rnd_seed}")
        self.rnd = random.Random(self.rnd_seed)
        self.rpk = RpkTool(self.redpanda)

    class ConfigProp(Enum):
        QUOTA_NODE_MAX_IN = "kafka_throughput_limit_node_in_bps"
        QUOTA_NODE_MAX_EG = "kafka_throughput_limit_node_out_bps"
        THROTTLE_DELAY_MAX_MS = "max_kafka_throttle_delay_ms"
        CONTROLLED_API_KEYS = "kafka_throughput_controlled_api_keys"
        THROUGHPUT_CONTROL = "kafka_throughput_control"

    api_names = [
        "add_offsets_to_txn", "add_partitions_to_txn", "alter_configs",
        "alter_partition_reassignments", "api_versions", "create_acls",
        "create_partitions", "create_topics", "delete_acls", "delete_groups",
        "delete_topics", "describe_acls", "describe_configs",
        "describe_groups", "describe_log_dirs", "describe_producers",
        "describe_transactions", "end_txn", "fetch", "find_coordinator",
        "heartbeat", "incremental_alter_configs", "init_producer_id",
        "join_group", "leave_group", "list_groups", "list_offsets",
        "list_partition_reassignments", "list_transactions", "metadata",
        "offset_commit", "offset_delete", "offset_fetch",
        "offset_for_leader_epoch", "produce", "sasl_authenticate",
        "sasl_handshake", "sync_group", "txn_offset_commit"
    ]

    def binexp_random(self, min: int, max: int):
        min_exp = min.bit_length() - 1
        max_exp = max.bit_length() - 1
        return math.floor(2**(self.rnd.random() * (max_exp - min_exp) +
                              min_exp))

    def get_config_parameter_random_value(self, prop: ConfigProp):
        if prop in [
                self.ConfigProp.QUOTA_NODE_MAX_IN,
                self.ConfigProp.QUOTA_NODE_MAX_EG
        ]:
            r = self.rnd.randrange(4)
            min = 256  # practical minimum
            if r == 0:
                return None
            if r == 1:
                return min
            return self.binexp_random(min, 2**40)  # up to 1 TB/s

        if prop == self.ConfigProp.THROTTLE_DELAY_MAX_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return self.binexp_random(0, 2**25)  # up to ~1 year

        if prop == self.ConfigProp.CONTROLLED_API_KEYS:
            keys_num = len(self.api_names)
            keys = set()
            for _ in range(self.rnd.randrange(keys_num)):
                keys.add(self.api_names[self.rnd.randrange(keys_num)])
            return list(keys)

        if prop == self.ConfigProp.THROUGHPUT_CONTROL:
            throughput_control = []
            letters = string.digits + string.ascii_letters + ' '
            group_names = set()
            for _ in range(self.rnd.randrange(4)):
                tc_item = {}

                r = self.rnd.randrange(3)
                if r != 0:
                    while True:
                        new_name = ''.join(
                            self.rnd.choice(letters)
                            for _ in range(self.binexp_random(0, 512)))
                        if new_name not in group_names:
                            break
                    tc_item['name'] = new_name
                    group_names.add(new_name)

                r = self.rnd.randrange(3)
                if r == 0:
                    tc_item['client_id'] = 'client_id 1'
                elif r == 2:
                    tc_item['client_id'] = 'client_\d+'

                throughput_control.append(tc_item)
            return throughput_control

        raise Exception(f"Unsupported ConfigProp: {prop}")

    def setUp(self):
        pass

    @cluster(num_nodes=6)
    def test_consumers(self):
        """
        Non-KIP-219-compliant consumers with capability beyond the configured
        egress limit should not timeout or prevent other such consumers from
        joining a cgroup
        """
        self.redpanda.add_extra_rp_conf({
            self.ConfigProp.QUOTA_NODE_MAX_EG.value:
            400 * kiB,
            "kafka_batch_max_bytes":
            1 * MiB,
        })
        self.redpanda.set_seed_servers(self.redpanda.nodes)
        self.redpanda.start(omit_seeds_on_idx_one=False)

        partition_count = 2
        self.topics = [TopicSpec(partition_count=partition_count)]
        self._create_initial_topics()

        msg_count = 2000
        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size=10 * kiB,
                               msg_count=msg_count // partition_count,
                               printable=True)
        producer.start()

        def on_message(consumer: KcatConsumer, message: dict):
            message["payload"] = f"<{len(message['payload'])} bytes>"
            consumer._redpanda.logger.debug(f"{consumer._caption}{message}")

        consumer = KcatConsumer(self.test_context,
                                self.redpanda,
                                self.topic,
                                offset=KcatConsumer.OffsetMeta.beginning,
                                cgroup_name="cg01",
                                auto_commit_interval_ms=500)
        consumer.set_on_message(on_message)
        consumer.start()
        wait_until(
            lambda: consumer.consumed_total >= msg_count // 10,
            timeout_sec=10,
            backoff_sec=0.001,
            err_msg=
            "Timeout waiting for the first consumer to receive 10% of messages"
        )

        consumer2 = KcatConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            offset=KcatConsumer.OffsetMeta.stored,
            offset_default=KcatConsumer.OffsetDefaultMeta.beginning,
            cgroup_name="cg01",
            auto_commit_interval_ms=500)
        consumer2.set_on_message(on_message)
        consumer2.start()
        # ensure that the 2nd consumer has connected and joined the cgroup
        # by checking that it has started to consume
        wait_until(
            lambda: consumer2.consumed_total > 0,
            timeout_sec=30,
            err_msg=
            "Timeout waiting for the second consumer to join the cgroup and consume something"
        )

        producer.stop()
        consumer.stop()
        consumer2.stop()
