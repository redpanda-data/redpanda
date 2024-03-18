# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from enum import Enum


# no StrEnum support in test python version
class WriteCachingMode(str, Enum):
    ON = "on"
    OFF = "off"
    DISABLED = "disabled"

    def __bool__(self):
        return self.value == self.ON

    def __str__(self):
        return self.value


class WriteCachingPropertiesTest(RedpandaTest):
    WRITE_CACHING_PROP = "write.caching"
    FLUSH_MS_PROP = "flush.ms"
    FLUSH_BYTES_PROP = "flush.bytes"

    WRITE_CACHING_DEBUG_KEY = "write_caching_enabled"
    FLUSH_MS_DEBUG_KEY = "flush_ms"
    FLUSH_BYTES_DEBUG_KEY = "flush_bytes"

    def __init__(self, test_context):
        super(WriteCachingPropertiesTest,
              self).__init__(test_context=test_context, num_brokers=3)
        self.topic_name = "test"
        self.topics = [TopicSpec(name=self.topic_name)]

    def configs_converged(self, write_caching: WriteCachingMode, flush_ms: int,
                          flush_bytes: int):
        """Checks that the desired write caching configuration is set in topic & partition (raft) states"""
        assert self.rpk
        assert self.admin

        configs = self.rpk.describe_topic_configs(self.topic_name)
        assert self.WRITE_CACHING_PROP in configs.keys(), configs
        assert self.FLUSH_MS_PROP in configs.keys(), configs
        assert self.FLUSH_BYTES_PROP in configs.keys(), configs

        properties_check = configs[self.WRITE_CACHING_PROP][0] == str(
            write_caching) and configs[self.FLUSH_MS_PROP][0] == str(
                flush_ms) and configs[self.FLUSH_BYTES_PROP][0] == str(
                    flush_bytes)

        if not properties_check:
            return False

        partition_state = self.admin.get_partition_state(
            "kafka", self.topic_name, 0)
        replicas = partition_state["replicas"]
        assert len(replicas) == 3

        def validate_flush_ms(val: int) -> bool:
            # account for jitter
            return flush_ms - 5 <= val <= flush_ms + 5

        for replica in replicas:
            raft_state = replica["raft_state"]
            assert self.WRITE_CACHING_DEBUG_KEY in raft_state.keys(
            ), raft_state
            assert self.FLUSH_MS_DEBUG_KEY in raft_state.keys(), raft_state
            assert self.FLUSH_BYTES_DEBUG_KEY in raft_state.keys(), raft_state

            replica_check = raft_state[self.WRITE_CACHING_DEBUG_KEY] == bool(
                write_caching) and validate_flush_ms(
                    raft_state[self.FLUSH_MS_DEBUG_KEY]) and raft_state[
                        self.FLUSH_BYTES_DEBUG_KEY] == flush_bytes

            if not replica_check:
                return False

        return True

    def validate_topic_configs(self, write_caching: WriteCachingMode,
                               flush_ms: int, flush_bytes: int):
        self.redpanda.wait_until(
            lambda: self.configs_converged(write_caching, flush_ms, flush_bytes
                                           ),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Write caching configuration failed to converge")

    def set_cluster_config(self, key: str, value):
        self.rpk.cluster_config_set(key, value)

    def set_topic_properties(self, key: str, value):
        self.rpk.alter_topic_config(self.topic_name, key, value)

    @cluster(num_nodes=3)
    def test_properties(self):
        self.admin = self.redpanda._admin
        self.rpk = RpkTool(self.redpanda)

        write_caching_conf = "write_caching"
        flush_ms_conf = "raft_replica_max_flush_delay_ms"
        flush_bytes_conf = "raft_replica_max_pending_flush_bytes"

        # Validate cluster defaults
        self.validate_topic_configs(WriteCachingMode.OFF, 100, 262144)

        # Changing cluster level configs
        self.set_cluster_config(write_caching_conf, "on")
        self.validate_topic_configs(WriteCachingMode.ON, 100, 262144)
        self.set_cluster_config(flush_ms_conf, 200)
        self.validate_topic_configs(WriteCachingMode.ON, 200, 262144)
        self.set_cluster_config(flush_bytes_conf, 32768)
        self.validate_topic_configs(WriteCachingMode.ON, 200, 32768)

        # Turn off write caching at topic level
        self.set_topic_properties(self.WRITE_CACHING_PROP, "off")
        self.validate_topic_configs(WriteCachingMode.OFF, 200, 32768)

        # Turn off write caching at cluster level but enable at topic level
        # topic properties take precedence
        self.set_cluster_config(write_caching_conf, "off")
        self.set_topic_properties(self.WRITE_CACHING_PROP, "on")
        self.validate_topic_configs(WriteCachingMode.ON, 200, 32768)

        # Kill switch test, disable write caching feature globally,
        # should override topic level property
        self.set_cluster_config(write_caching_conf, "disabled")
        self.validate_topic_configs(WriteCachingMode.DISABLED, 200, 32768)

        # Try to update the topic property now, should throw an error
        try:
            self.set_topic_properties(self.WRITE_CACHING_PROP, "on")
            assert False, "No exception thrown when updating topic propertes in disabled mode."
        except RpkException as e:
            assert "INVALID_CONFIG" in str(e)

        # Enable again
        self.set_cluster_config(write_caching_conf, "on")
        self.set_topic_properties(self.WRITE_CACHING_PROP, "on")
        self.validate_topic_configs(WriteCachingMode.ON, 200, 32768)
