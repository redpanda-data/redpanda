# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random, time, math, json, string
import socket
from enum import Enum
from typing import Tuple

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint, SaslCredentials, SecurityConfig
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.kcat_consumer import KcatConsumer
from rptest.clients.kafka_cat import KafkaCat
from rptest.services import tls
from rptest.services.admin import Admin

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
        self.superuser: SaslCredentials = self.redpanda.SUPERUSER_CREDENTIALS
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
        BAL_WINDOW_MS = "kafka_quota_balancer_window_ms"
        BAL_PERIOD_MS = "kafka_quota_balancer_node_period_ms"
        QUOTA_SHARD_MIN_RATIO = "kafka_quota_balancer_min_shard_throughput_ratio"
        QUOTA_SHARD_MIN_BPS = "kafka_quota_balancer_min_shard_throughput_bps"
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

    # Returns a random value distributed base-2 exponentially between `min` and `max`
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

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_BPS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return self.binexp_random(0, 2**30)  # up to 1 GB/s

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_RATIO:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return 1
            return self.rnd.random()

        if prop == self.ConfigProp.THROTTLE_DELAY_MAX_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return self.binexp_random(0, 2**25)  # up to ~1 year

        if prop == self.ConfigProp.BAL_WINDOW_MS:
            r = self.rnd.randrange(4)
            min = 1
            max = 2147483647
            if r == 0:
                return min
            if r == 1:
                return max
            return self.binexp_random(min, max)

        if prop == self.ConfigProp.BAL_PERIOD_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return self.binexp_random(0, 2**22)  # up to ~1.5 months

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

                principals = []
                for _ in range(self.rnd.randrange(3)):
                    r = self.rnd.randrange(3)
                    if r == 0:
                        principals.append({'user': 'admin'})
                    elif r == 1:
                        principals.append({'user': '*'})
                if len(principals) != 0:
                    tc_item['principals'] = principals

                throughput_control.append(tc_item)
            return throughput_control

        raise Exception(f"Unsupported ConfigProp: {prop}")

    # Returns effective node quota reported via metrics
    def current_effective_node_quota(self) -> Tuple[int, int]:
        metrics = self.redpanda.metrics_sample(
            "quotas_quota_effective", metrics_endpoint=MetricsEndpoint.METRICS)

        assert metrics, "Effective quota metric is missing"
        self.logger.debug(f"Samples: {metrics.samples}")

        node_quota_in = sum(
            int(s.value) for s in metrics.label_filter({
                "direction": "ingress"
            }).samples)
        node_quota_eg = sum(
            int(s.value) for s in metrics.label_filter({
                "direction": "egress"
            }).samples)
        return node_quota_in, node_quota_eg

    def setUp(self):
        pass

    @cluster(num_nodes=3)
    def test_configuration(self):
        """
        Test various configuration patterns, including extreme ones,
        verify that it does not wreck havoc onto cluster
        """

        self.config = {}
        for prop in list(self.ConfigProp):
            val = self.get_config_parameter_random_value(prop)
            self.config[prop] = val
            if not val is None:
                self.redpanda.add_extra_rp_conf({prop.value: val})
        self.logger.info(
            f"Initial cluster props: {self.redpanda._extra_rp_conf}")
        super(ThroughputLimitsSnc, self).setUp()

        # TBD: parameterize to run under load or not

        errors = []
        # 12 passes approximately take 30 secs on a 3-node cluster
        for _ in range(12):

            change = {}
            for _ in range(self.rnd.randrange(4)):
                config_param = self.rnd.choice(list(self.ConfigProp))
                config_value = self.get_config_parameter_random_value(
                    config_param)
                self.config[config_param] = config_value
                change[config_param.value] = config_value
            self.logger.info(f"Changing cluster prop: {change}")
            self.redpanda.set_cluster_config(change)

            # set_cluster_config waits for the prop to be replicated
            # it takes time so no need for a sleep()

            def check_node_quota_metric(self, config_prop: self.ConfigProp,
                                        effective_node_quota: int) -> str:
                config_val = self.config[config_prop]
                if config_val is None:
                    return
                expected_node_quota = config_val * len(self.redpanda.nodes)
                if effective_node_quota == expected_node_quota:
                    return
                e = (
                    f"Expected quota value mismatch. "
                    f"Effective {effective_node_quota} != expected {expected_node_quota}. "
                    f"Direction: {config_prop.name}")
                self.logger.error(e)
                return e

            effective_node_quota = self.current_effective_node_quota()
            self.logger.debug(
                f"current_effective_node_quota: {effective_node_quota}")
            errors.append(
                check_node_quota_metric(self,
                                        self.ConfigProp.QUOTA_NODE_MAX_IN,
                                        effective_node_quota[0]))
            errors.append(
                check_node_quota_metric(self,
                                        self.ConfigProp.QUOTA_NODE_MAX_EG,
                                        effective_node_quota[1]))

        errors = set([e for e in errors if e])
        assert len(errors) == 0, (
            f"Test has failed with {len(errors)} distinct errors. "
            f"{errors}, rnd_seed: {self.rnd_seed}")

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

    @cluster(num_nodes=5)
    def test_throughput_groups_exemptions(self):
        """
        Clients with configured tput exemptions are not limited.
        This includes configuration by
        - client_id (TBD)
        - auth user
        """

        security = SecurityConfig()
        security.enable_sasl = True
        self.redpanda.set_security_settings(security)
        self.topics = [
            TopicSpec(partition_count=1),
            TopicSpec(partition_count=1)
        ]
        super(ThroughputLimitsSnc, self).setUp()

        # run a producer for the admin user, measure tput using the 1st topic
        topic = self.topics[0].name
        msg_size = 6 * kiB
        msg_count = 60
        producer_ref = RpkProducer(self.test_context,
                                   self.redpanda,
                                   topic,
                                   msg_size=msg_size,
                                   msg_count=msg_count,
                                   max_message_bytes=3 * msg_size,
                                   sasl_cred=(self.superuser.username,
                                              self.superuser.password),
                                   produce_timeout=20)
        producer_ref.start()
        producer_ref.wait()
        producer_ref.stop()
        producer_ref.free()

        # this is the reference rate with 1 producer
        rate_ref = msg_count * msg_size / producer_ref.time_elapsed
        self.logger.info(
            f"Reference producer: {producer_ref.time_elapsed:.3f} s, {rate_ref:.0f} B/s"
        )

        # (2) Set up another user, global tput limits, and tput exemptions for
        # the superuser. Then run 2 producers, one for each user, and compare
        # their effective tputs. The superuser's tput is expected to be significantly
        # over the threshold, while the restricted user's tput should be under
        # the threshold.

        # this will be the limited tput rate; together with the unlimited producer
        # they will make about the reference rate. The unlimited one should then
        # make about rate_ref * 0.9.
        tp_limit = int(round(rate_ref * 0.1))

        # adjust message count so that the entire limited traffic
        # would go through in no more than 15s
        msg_count = max(15, min(150, 15 * tp_limit // msg_size))

        # use the empty topic
        topic = self.topics[1].name

        # set tput limits and exemptions
        self.redpanda.set_cluster_config({
            self.ConfigProp.THROUGHPUT_CONTROL.value: [{
                'principals': [{
                    'user': self.superuser.username
                }]
            }],
            self.ConfigProp.QUOTA_NODE_MAX_IN.value:
            tp_limit,
            self.ConfigProp.QUOTA_NODE_MAX_EG.value:
            tp_limit,
        })

        # create restricted used
        admin = Admin(self.redpanda)
        user = SaslCredentials("user2", "password2", "SCRAM-SHA-256")
        admin.create_user(user.username, user.password, user.algorithm)

        # enable cluster and topic access for the new user
        rpk = RpkTool(self.redpanda,
                      username=self.superuser.username,
                      password=self.superuser.password,
                      sasl_mechanism=self.superuser.algorithm)
        rpk.acl_create_allow_cluster(user.username, 'ALL')
        rpk.acl_create_allow_topic(user.username, topic, 'ALL')

        # allow thrice the expected timeout to the limited producer
        producer1_timeout = 3 * msg_count * msg_size // tp_limit

        # run the producers
        self.logger.info(
            f"Running the producers. tp_limit={tp_limit}, msg_size={msg_size}, "
            f"msg_count={msg_count}, producer1_timeout={producer1_timeout}")
        producer0 = RpkProducer(self.test_context,
                                self.redpanda,
                                topic,
                                msg_size=msg_size,
                                msg_count=msg_count,
                                max_message_bytes=3 * msg_size,
                                sasl_cred=(self.superuser.username,
                                           self.superuser.password))
        producer1 = RpkProducer(self.test_context,
                                self.redpanda,
                                topic,
                                msg_size=msg_size,
                                msg_count=msg_count,
                                max_message_bytes=3 * msg_size,
                                sasl_cred=(user.username, user.password),
                                produce_timeout=producer1_timeout)
        producer0.start()
        producer1.start()
        producer0.wait()
        producer1.wait()
        producer0.stop()
        producer1.stop()
        producer0.free()
        producer1.free()

        # measure and compare
        rate0 = msg_count * msg_size / producer0.time_elapsed
        rate1 = msg_count * msg_size / producer1.time_elapsed
        self.logger.info(
            f"Non-throttled producer: {producer0.time_elapsed:.3f} s, {rate0:.0f} B/s"
        )
        self.logger.info(
            f"Producer throttled at {tp_limit} B/s: {producer1.time_elapsed:.3f} s, {rate1:.0f} B/s"
        )
        self.logger.info(
            f"Total rate: {rate0+rate1:.0f}, reference rate: {rate_ref:.0f}")
        assert rate0 / rate1 > 5, \
            f"Exempt clients's rate ({rate0:.0f} B/s) must be much larger than "\
            f"the limited ({rate1:.0f} B/s), only {rate0/rate1:.1f} times yet (expected: 5)"
        # because the test is short, peaks in tput can render limited tput
        # slightly bigger than the limit
        assert rate1 / tp_limit < 1.2, \
            f"Non-exempt clients's rate ({rate1:.0f} B/s) must be lower than "\
            f"the limit ({tp_limit:.0f} B/s)"
