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
from rptest.clients.rpk import RpkTool
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
    """
    topics = (TopicSpec(), )

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

    def _offset_removal_occurred(self, period):
        rpk = RpkTool(self.redpanda)
        group = "hey_group"

        def offsets_exist():
            desc = rpk.group_describe(group)
            return len(desc.partitions) > 0

        # consume from the group for twice as long as the retention period
        # setting and verify that group offsets exist for the entire time.
        start = time.time()
        while time.time() - start < (period * 2):
            rpk.produce(self.topic, "k", "v")
            rpk.consume(self.topic, n=1, group=group)
            wait_until(offsets_exist, timeout_sec=1, backoff_sec=1)
            time.sleep(1)

        # after one half life the offset should still exist
        time.sleep(period / 2)
        assert offsets_exist()

        # after waiting for twice the retention period, it should be gone
        time.sleep(period * 2 - period / 2)
        return not offsets_exist()

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
        assert not self._offset_removal_occurred(period)

        # in cluster upgraded from pre-v23 retention should not be active
        self._perform_upgrade(initial_version, RedpandaInstaller.HEAD)
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("group_offset_retention_sec", str(period))
        rpk.cluster_config_set("group_offset_retention_check_ms", str(1000))
        assert not self._offset_removal_occurred(period)

        # enable legacy. enablng legacy support takes affect at the next
        # retention check. since that is configured above to happen every second
        # then the response time should be adequate.
        rpk.cluster_config_set("legacy_group_offset_retention_enabled",
                               str(True))
        assert self._offset_removal_occurred(period)


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
