# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import time
from functools import partial
from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from ducktape.mark import parametrize
from rptest.services.admin import Admin
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions, ver_string


class OffsetRetentionDisabledAfterUpgrade(RedpandaTest):
    """
    When upgrading to Redpanda v23 or later offset retention should be disabled
    by default. Offset retention did not exist pre-v23, so existing clusters
    should have to opt-in after upgrade in order to avoid suprises.

    When a cluster upgrades to v23 then only newly committed offsets are
    reclaimed automatically. Offsets written prior to the upgrade must be
    manually removed using the kafka delete offset api.

    Offsets committed prior to v23 that are normally non-reclaimable become
    reclaimable if they are updated following the upgrade.
    """
    topics = (TopicSpec(), TopicSpec(), TopicSpec(), TopicSpec(), TopicSpec())

    # pre-defined named topics used to simulate use of topics that are used only
    # pre-v23, both pre and post upgrade, and only after upgrade to v23. the
    # idle variants stop receiving updates prior to enabling legacy support.
    pre_v23_topic = topics[0].name
    prepost_v23_topic = topics[1].name
    prepost_v23_topic_idle = topics[2].name
    post_v23_topic = topics[3].name
    post_v23_topic_idle = topics[4].name

    feature_config_timing = {
        "group_offset_retention_sec",
        "group_offset_retention_check_ms",
    }
    feature_config_legacy = "legacy_group_offset_retention_enabled"
    feature_config_names = feature_config_timing | {feature_config_legacy}

    def __init__(self, test_context):
        super(OffsetRetentionDisabledAfterUpgrade,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.redpanda._installer

    def setUp(self):
        # handled by test case to support parameterization
        pass

    def _validate_pre_upgrade(self, version):
        """
        1. verify starting version of cluster nodes
        2. verify expected configs of initial cluster
        """
        self.installer.install(self.redpanda.nodes, version)
        super(OffsetRetentionDisabledAfterUpgrade, self).setUp()

        # wait until all nodes are running the same version
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert ver_string(version) in unique_versions

        # sanity check none of feature configs should exist
        admin = Admin(self.redpanda)
        config = admin.get_cluster_config()
        assert config.keys().isdisjoint(self.feature_config_names)

    def _perform_upgrade(self, initial_version, version):
        """
        1. verify upgrade of all cluster nodes
        2. verify expected configs after upgrade
        """
        # upgrade all nodes to target version
        self.installer.install(self.redpanda.nodes, version)
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # wait until all nodes are running the same version
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert ver_string(initial_version) not in unique_versions

        # sanity check that all new feature configs exist
        admin = Admin(self.redpanda)
        config = admin.get_cluster_config()
        assert config.keys() > self.feature_config_names

        # configs should all have positive values
        for name in self.feature_config_timing:
            assert config[name] > 0

        # after upgrade legacy support should be disabled
        assert config[self.feature_config_legacy] == False

    def _offset_removal_occurred(self, period, pre_upgrade,
                                 pre_legacy_support):
        rpk = RpkTool(self.redpanda)
        group = "hey_group"

        def offsets_exist(include_idle):
            """
            Note to future ci-failure investigators. If you were to ever observe
            a failure in which it appeared that prepost/post topic offsets were
            not removed it could be because the following occurred:

              1) the version fence was not written for some reason
              2) so some of the offsets were incorrectly marked as legacy
              3) something else happened like updates to commits also failing

            it's unlikely that this would happen, but if it does, the proper fix
            would be to update the test to check for this condition (may be
            non-trivial to automate this check) and then retry the test.

            in real-world scenarios if this were to happen then it is expected
            that offset delete api is used to clean up any offsets that lose
            this race condition that exists during an upgrade.
            """
            desc = rpk.group_describe(group)
            if len(desc.partitions) == 0:
                return False

            pre = any(p.topic == self.pre_v23_topic for p in desc.partitions)
            prepost = any(p.topic == self.prepost_v23_topic
                          for p in desc.partitions)
            prepost_idle = any(p.topic == self.prepost_v23_topic_idle
                               for p in desc.partitions)
            post = any(p.topic == self.post_v23_topic for p in desc.partitions)
            post_idle = any(p.topic == self.post_v23_topic_idle
                            for p in desc.partitions)

            assert pre, "pre-topic offsets should never be reclaimed"

            # prior to upgrade, the only offsets that should exist are for the
            # pre and prepost topics.
            if pre_upgrade:
                assert not post, "post-topic has not yet been written"
                return prepost and prepost_idle

            if include_idle:
                return prepost and post and prepost_idle and post_idle
            return prepost and post

        # consume from the group for twice as long as the retention period
        # setting and verify that group offsets exist for the entire time.
        start = time.time()
        while time.time() - start < (period * 2):
            if pre_upgrade:
                # pre-v23
                rpk.produce(self.pre_v23_topic, "k", "v")
                rpk.consume(self.pre_v23_topic, n=1, group=group)
            else:
                # post-v23
                rpk.produce(self.post_v23_topic, "k", "v")
                rpk.consume(self.post_v23_topic, n=1, group=group)
                if pre_legacy_support:
                    rpk.produce(self.post_v23_topic_idle, "k", "v")
                    rpk.consume(self.post_v23_topic_idle, n=1, group=group)

            # pre/post-v23
            rpk.produce(self.prepost_v23_topic, "k", "v")
            rpk.consume(self.prepost_v23_topic, n=1, group=group)
            if pre_legacy_support:
                rpk.produce(self.prepost_v23_topic_idle, "k", "v")
                rpk.consume(self.prepost_v23_topic_idle, n=1, group=group)

            wait_until(lambda: offsets_exist(pre_legacy_support),
                       timeout_sec=1,
                       backoff_sec=1)
            time.sleep(1)

        # after one half life the offset should still exist. prior to legacy
        # support being enabled include idle / be strict. after it is enabled
        # then it's possible the idle topics get reclaimed so relax this check.
        time.sleep(period / 2)
        assert offsets_exist(pre_legacy_support)

        # after waiting for twice the retention period, it should be gone.
        # always be strict on this check and include idle topics.
        time.sleep(period * 2 - period / 2)
        return not offsets_exist(True)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(initial_version=(22, 2, 9))
    @parametrize(initial_version=(22, 3, 11))
    def test_upgrade_from_pre_v23(self, initial_version):
        """
        1. test that retention feature doesn't work on legacy version
        2. upgrade cluster and test that feature still doesn't work
        3. enable legacy support on the cluster
        4. test that retention works properly
        """
        period = 30

        # in old cluster offset retention should not be active
        self._validate_pre_upgrade(initial_version)
        assert not self._offset_removal_occurred(period, True, True)

        # in cluster upgraded from pre-v23 retention should not be active
        self._perform_upgrade(initial_version, RedpandaInstaller.HEAD)
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("group_offset_retention_sec", str(period))
        rpk.cluster_config_set("group_offset_retention_check_ms", str(1000))
        assert not self._offset_removal_occurred(period, False, True)

        # enable legacy. enablng legacy support takes affect at the next
        # retention check. since that is configured above to happen every second
        # then the response time should be adequate.
        rpk.cluster_config_set("legacy_group_offset_retention_enabled",
                               str(True))
        assert self._offset_removal_occurred(period, False, False)


class OffsetRetentionTest(RedpandaTest):
    topics = (TopicSpec(), )
    period = 30

    def __init__(self, test_context):
        # retention time is set to 30 seconds and expired offset queries happen
        # every second. these are likely unrealistic in practice, but allow us
        # to build tests that are quick and responsive.
        super(OffsetRetentionTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=dict(
                                 group_offset_retention_sec=self.period,
                                 group_offset_retention_check_ms=1000,
                             ))

        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_offset_expiration(self):
        group = "hey_group"

        def offsets_exist():
            desc = self.rpk.group_describe(group)
            return len(desc.partitions) > 0

        # consume from the group for twice as long as the retention period
        # setting and verify that group offsets exist for the entire time.
        start = time.time()
        while time.time() - start < (self.period * 2):
            self.rpk.produce(self.topic, "k", "v")
            self.rpk.consume(self.topic, n=1, group=group)
            wait_until(offsets_exist, timeout_sec=1, backoff_sec=1)
            time.sleep(1)

        # after one half life the offset should still exist
        time.sleep(self.period / 2)
        assert offsets_exist()

        # after waiting for twice the retention period, it should be gone
        time.sleep(self.period * 2 - self.period / 2)
        assert not offsets_exist()


class OffsetDeletionTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3), )
    group = "hey_hey_group"

    def __init__(self, test_context):
        super(OffsetDeletionTest, self).__init__(test_context=test_context)

        self.rpk = RpkTool(self.redpanda)

    def make_consumer(self, topic, session_timeout_ms=60000):
        return KafkaCliConsumer(
            self.test_context,
            self.redpanda,
            topic=topic,
            group=self.group,
            from_beginning=True,
            consumer_properties={'session.timeout.ms': session_timeout_ms},
            instance_name=f'cli-consumer-offset-delete-test-{topic}')

    @cluster(num_nodes=5)
    def test_offset_deletion(self):
        def wait_for_partitions_in_group(n):
            desc = self.rpk.group_describe(self.group)
            return len(desc.partitions) == n

        def assert_status(output, expected_status, expected_topic):
            for response in output:
                assert response.topic == expected_topic, f"Expected: {expected_topic} Observed: {response.topic}"
                assert response.status == expected_status, response.status

        # Produce some data to a topic and consume assigning new consumer group
        for _ in range(0, 10):
            self.rpk.produce(self.topic, "k", "v", partition=0)
            self.rpk.produce(self.topic, "k", "v", partition=1)
            self.rpk.produce(self.topic, "k", "v", partition=2)
        self.rpk.consume(self.topic, n=3, group=self.group)
        wait_until(partial(wait_for_partitions_in_group, 3),
                   timeout_sec=30,
                   backoff_sec=1)

        # Assert offset-delete errors when request contains missing topic-partitions
        missing_topic = f"{self.topic}-foo"
        missing_topic_partitions = {missing_topic: [0, 1, 2]}
        output = self.rpk.offset_delete(self.group, missing_topic_partitions)
        assert len(output) == 3
        assert_status(output, 'UNKNOWN_TOPIC_OR_PARTITION', missing_topic)

        # Assert offset-delete errors when non-existent group is passed in
        topic_partitions = {self.topic: [0, 1, 2]}
        output = self.rpk.offset_delete("missing", topic_partitions)
        assert output.status == 'GROUP_ID_NOT_FOUND', output.status

        # Assert offset-delete errors when attempting to delete offsets of topic
        # partitions still assigned to an active group
        consumer = self.make_consumer(self.topic)
        consumer.start()
        consumer.wait_for_messages(1)
        wait_until(partial(wait_for_partitions_in_group, 3),
                   timeout_sec=30,
                   backoff_sec=1)

        output = self.rpk.offset_delete(self.group, topic_partitions)
        assert len(output) == 3
        assert_status(output, 'GROUP_SUBSCRIBED_TO_TOPIC', self.topic)

        consumer.stop()
        consumer.wait()
        consumer.free()

        # Assert offset-delete removes offsets of dead groups
        output = self.rpk.offset_delete(self.group, topic_partitions)
        assert len(output) == 3
        assert_status(output, 'OK', self.topic)

        desc = self.rpk.group_describe(self.group)
        assert len(desc.partitions) == 0

        # Assert offset-delete removes offsets of unsubscribed topic/partitions
        # within an active group
        new_topic = "foo"
        self.rpk.create_topic(new_topic, partitions=3)
        for i in range(0, 3):
            self.rpk.produce(new_topic, "k", "v", partition=i)

        # Add two new consumers to the group
        consumer_a = self.make_consumer(self.topic)
        min_default_group_session_timeout_ms = 6000
        consumer_b = self.make_consumer(new_topic,
                                        min_default_group_session_timeout_ms)
        consumer_a.start()
        consumer_b.start()

        # Wait until both have joined the group
        wait_until(partial(wait_for_partitions_in_group, 6),
                   timeout_sec=30,
                   backoff_sec=1)

        # Have them both consume some data
        consumer_a.wait_for_messages(1)
        consumer_b.wait_for_messages(1)

        # Shutdown one consumer
        consumer_b.stop()
        consumer_b.wait()

        # After consumer_b shuts down wait for the group rebalance to occur
        # and unsubscription of topic/partitions it was assigned to
        wait_until(partial(wait_for_partitions_in_group, 3),
                   timeout_sec=30,
                   backoff_sec=1)

        # Remove the offsets that it had been keeping in redpanda
        output = self.rpk.offset_delete(self.group, {new_topic: [0, 1, 2, 3]})
        assert len(output) == 4
        good_responses = [x for x in output if x.partition != 3]
        bad_responses = [x for x in output if x.partition == 3]
        assert len(good_responses) == 3
        assert len(bad_responses) == 1
        assert_status(good_responses, "OK", new_topic)
        assert_status(bad_responses, "UNKNOWN_TOPIC_OR_PARTITION", new_topic)

        # Shutdown other consumer
        consumer_a.stop()
        consumer_a.wait()
