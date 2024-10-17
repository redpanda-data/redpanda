# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time
from collections import defaultdict
from packaging.version import Version

from ducktape.mark import parametrize, matrix
from ducktape.utils.util import wait_until
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import (
    segments_count,
    wait_for_local_storage_truncate,
    produce_until_segments,
    wait_until_segments,
)
from rptest.utils.mode_checks import skip_fips_mode
from rptest.utils.si_utils import BucketView
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, CloudStorageType, get_cloud_storage_type
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
    KgoVerifierRandomConsumer,
    KgoVerifierConsumerGroupConsumer,
)
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import InstallOptions, RedpandaInstaller, wait_for_num_versions


class UpgradeFromSpecificVersion(RedpandaTest):
    """
    Basic test that upgrading software works as expected, upgrading from the last
    feature version to the HEAD version.
    """
    def __init__(self, test_context):
        super(UpgradeFromSpecificVersion,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.redpanda._installer

    def setUp(self):
        self.old_version = self.redpanda._installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        self.old_version_str = f"v{self.old_version[0]}.{self.old_version[1]}.{self.old_version[2]}"
        self.installer.install(self.redpanda.nodes, self.old_version)
        super(UpgradeFromSpecificVersion, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_basic_upgrade(self):
        first_node = self.redpanda.nodes[0]

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        # Upgrade one node to the head version.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.old_version_str in unique_versions, unique_versions

        # Rollback the partial upgrade and ensure we go back to the original
        # state.
        self.installer.install([first_node], self.old_version)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        # Only once we upgrade the rest of the nodes do we converge on the new
        # version.
        self.installer.install([first_node], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str not in unique_versions, unique_versions


class UpgradeFromPriorFeatureVersionTest(RedpandaTest):
    """
    Basic test that installs the previous feature version and performs an
    upgrade.
    """
    def __init__(self, test_context):
        super(UpgradeFromPriorFeatureVersionTest,
              self).__init__(test_context=test_context, num_brokers=1)
        self.installer = self.redpanda._installer

    def setUp(self):
        self.prev_version = \
            self.installer.highest_from_prior_feature_version(RedpandaInstaller.HEAD)
        self.installer.install(self.redpanda.nodes, self.prev_version)
        super(UpgradeFromPriorFeatureVersionTest, self).setUp()

    @cluster(num_nodes=1,
             log_allow_list=RESTART_LOG_ALLOW_LIST +
             [re.compile("cluster - .*Error while reconciling topic.*")])
    def test_basic_upgrade(self):
        node = self.redpanda.nodes[0]
        initial_version = Version(self.redpanda.get_version(node))
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        self.redpanda.restart_nodes([node])
        head_version_str = self.redpanda.get_version(node)
        head_version = Version(head_version_str)
        assert initial_version < head_version, f"{initial_version} vs {head_version}"

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert head_version_str in unique_versions, unique_versions


PREV_VERSION_LOG_ALLOW_LIST = [
    # e.g. cluster - controller_backend.cc:400 - Error while reconciling topics - seastar::abort_requested_exception (abort requested)
    "cluster - .*Error while reconciling topic.*",
    # Typo fixed in recent versions.
    # e.g.  raft - [follower: {id: {1}, revision: {10}}] [group_id:3, {kafka/topic/2}] - recovery_stm.cc:422 - recovery append entries error: raft group does not exists on target broker
    "raft - .*raft group does not exists on target broker",
    # e.g. rpc - Service handler thrown an exception - seastar::gate_closed_exception (gate closed)
    "(kafka|rpc) - .*gate_closed_exception.*",
    # Tests on mixed versions will start out with an unclean restart before
    # starting a workload.
    "(raft|kafka|rpc) - .*(disconnected_endpoint|Broken pipe|Connection reset by peer)",
]


class UpgradeBackToBackTest(PreallocNodesTest):
    """
    Test that runs through two rolling upgrades while running through workloads.
    """
    MSG_SIZE = 100
    PRODUCE_COUNT = 100000
    RANDOM_READ_COUNT = 100
    RANDOM_READ_PARALLEL = 4
    CONSUMER_GROUP_READERS = 4
    topics = (TopicSpec(partition_count=3, replication_factor=3), )

    oldest_version = (21, 11)

    def __init__(self, test_context):
        if self.debug_mode:
            self.MSG_SIZE = 10
            self.RANDOM_READ_COUNT = 10
            self.RANDOM_READ_PARALLEL = 1
            self.CONSUMER_GROUP_READERS = 1
            self.topics = (TopicSpec(partition_count=1,
                                     replication_factor=3), )
        super(UpgradeBackToBackTest, self).__init__(test_context,
                                                    num_brokers=3,
                                                    node_prealloc_count=1)
        self.installer = self.redpanda._installer
        self.versions = []
        self.current_version = None

        self._producer = KgoVerifierProducer(test_context, self.redpanda,
                                             self.topic, self.MSG_SIZE,
                                             self.PRODUCE_COUNT,
                                             self.preallocated_nodes)
        self._seq_consumer = KgoVerifierSeqConsumer(
            test_context,
            self.redpanda,
            self.topic,
            self.MSG_SIZE,
            nodes=self.preallocated_nodes)
        self._rand_consumer = KgoVerifierRandomConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.RANDOM_READ_COUNT, self.RANDOM_READ_PARALLEL,
            self.preallocated_nodes)
        self._cg_consumer = KgoVerifierConsumerGroupConsumer(
            test_context,
            self.redpanda,
            self.topic,
            self.MSG_SIZE,
            self.CONSUMER_GROUP_READERS,
            nodes=self.preallocated_nodes)

        self._consumers = [
            self._seq_consumer, self._rand_consumer, self._cg_consumer
        ]

    def load_versions(self, single_upgrade: bool):
        # Special case: just one upgrade
        if single_upgrade:
            self.versions = [
                self.installer.highest_from_prior_feature_version(
                    RedpandaInstaller.HEAD), RedpandaInstaller.HEAD
            ]
            return

        else:
            self.versions = self.load_version_range(self.oldest_version)

    def install_next(self):
        v = self.versions.pop(0)
        self.logger.info(f"Installing version {v}...")
        self.installer.install(self.redpanda.nodes, v)
        self.current_version = v

    def setUp(self):
        pass

    @cluster(num_nodes=4, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    @parametrize(single_upgrade=True)
    @parametrize(single_upgrade=False)
    def test_upgrade_with_all_workloads(self, single_upgrade):
        self.load_versions(single_upgrade)

        # Have we already started our producer/consumer?
        started = False

        # Is our producer/consumer currently stopped (having already been started)?
        paused = False

        wrote_at_least = None

        for current_version in self.upgrade_through_versions(self.versions):
            if not started:
                # First version, start up the workload
                self._producer.start(clean=False)
                self._producer.wait_for_offset_map()
                self._producer.wait_for_acks(100,
                                             timeout_sec=10,
                                             backoff_sec=2)
                wrote_at_least = self._producer.produce_status.acked
                for consumer in self._consumers:
                    consumer.start(clean=False)
                started = True

            if current_version[0:2] == (21, 11):
                # Stop the producer before the next upgrade hop: 21.11 -> 22.1 upgrades
                # are not graceful
                self._producer.wait()
                assert self._producer.produce_status.acked == self.PRODUCE_COUNT
                paused = True
            elif current_version[0:2] == (22, 1) and paused:
                self._producer.start(clean=False)

        for consumer in self._consumers:
            consumer.wait()

        assert self._seq_consumer.consumer_status.validator.valid_reads >= wrote_at_least
        assert self._rand_consumer.consumer_status.validator.total_reads >= self.RANDOM_READ_COUNT * self.RANDOM_READ_PARALLEL
        assert self._cg_consumer.consumer_status.validator.valid_reads >= wrote_at_least

        # Validate that the data structures written by a mixture of historical
        # versions remain readable by our current debug tools
        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.nodes:
            controller_records = log_viewer.read_controller(node=node)
            self.logger.info(
                f"Read {len(controller_records)} controller records from node {node.name} successfully"
            )
            if log_viewer.has_controller_snapshot(node):
                controller_snapshot = log_viewer.read_controller_snapshot(
                    node=node)
                self.logger.info(
                    f"Read controller snapshot: {controller_snapshot}")


class UpgradeWithWorkloadTest(EndToEndTest):
    """
    Test class that performs upgrades while verifying a concurrently running
    workload is making progress.
    """
    def setUp(self):
        super(UpgradeWithWorkloadTest, self).setUp()

        # Use a relatively low throughput to give the restarted node a chance
        # to catch up. If the node is particularly slow compared to the others
        # (e.g. a locally-built debug binary), catching up can take a while.
        self.producer_msgs_per_sec = 10

        # Start the prior feature version, we will upgrade to latest.
        self.start_redpanda(
            num_nodes=3,
            install_opts=InstallOptions(install_previous_version=True))
        self.installer = self.redpanda._installer
        self.initial_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)

        # Start running a workload.
        spec = TopicSpec(name="topic", partition_count=2, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(num_nodes=1, throughput=self.producer_msgs_per_sec)
        self.start_consumer(num_nodes=1)
        self.await_startup(min_records=self.producer_msgs_per_sec)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rolling_upgrade(self):
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        # Give ample time to restart, given the running workload.
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

        post_upgrade_num_msgs = self.producer.num_acked
        self.run_validation(min_records=post_upgrade_num_msgs +
                            (self.producer_msgs_per_sec * 3))

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(upgrade_after_rollback=True)
    @parametrize(upgrade_after_rollback=False)
    def test_rolling_upgrade_with_rollback(self, upgrade_after_rollback):
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        # Upgrade one node.
        first_node = self.redpanda.nodes[0]
        # Give ample time to restart, given the running workload.
        self.redpanda.rolling_restart_nodes([first_node],
                                            start_timeout=90,
                                            stop_timeout=90)

        def await_progress():
            num_msgs = self.producer.num_acked
            self.await_num_produced(num_msgs +
                                    (self.producer_msgs_per_sec * 3))
            self.await_num_consumed(num_msgs +
                                    (self.producer_msgs_per_sec * 3))

        # Ensure that after we upgrade a node, we're still able to make
        # progress.
        await_progress()

        # Then roll it back; we should still be able to make progress.
        self.installer.install([first_node], self.initial_version)
        self.redpanda.rolling_restart_nodes([first_node],
                                            start_timeout=90,
                                            stop_timeout=90)
        await_progress()

        if upgrade_after_rollback:
            self.installer.install([first_node], RedpandaInstaller.HEAD)
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                                start_timeout=90,
                                                stop_timeout=90)

        post_rollback_num_msgs = self.producer.num_acked
        self.run_validation(min_records=post_rollback_num_msgs +
                            (self.producer_msgs_per_sec * 3),
                            enable_idempotence=True)


class UpgradeFromPriorFeatureVersionCloudStorageTest(RedpandaTest):
    """
    Check that a mixed-version cluster does not run into issues with
    an older node trying to read cloud storage data from a newer node.
    """
    def __init__(self, test_context):

        super().__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(
                test_context, cloud_storage_housekeeping_interval_ms=1000),
            extra_rp_conf={
                # We will exercise storage/cloud_storage read paths, get the
                # batch cache out of the way to ensure reads hit storage layer.
                "disable_batch_cache": True,
                # We will manually manipulate leaderships, do not want to fight
                # with the leader balancer
                "enable_leader_balancer": False,
            })
        self.installer = self.redpanda._installer
        self.rpk = RpkTool(self.redpanda)

    # This test starts the Redpanda service inline (see 'install_and_start') at the beginning
    # of the test body. By default, in the Azure CDT env, the service startup
    # logic attempts to set the azure specific cluster configs.
    # However, these did not exist prior to v23.1 and the test would fail
    # before it can be skipped.
    def setUp(self):
        pass

    def install_and_start(self):
        self.prev_version = \
            self.installer.highest_from_prior_feature_version(RedpandaInstaller.HEAD)
        self.installer.install(self.redpanda.nodes, self.prev_version)
        super().setUp()

    # before v24.2, dns query to s3 endpoint do not include the bucketname, which is required for AWS S3 fips endpoints
    @skip_fips_mode
    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_rolling_upgrade(self, cloud_storage_type):
        """
        Verify that when tiered storage writes happen during a rolling upgrade,
        we continue to write remote content that old versions can read, until
        the upgrade is complete.

        This ensures that rollbacks remain possible.
        """
        self.install_and_start()
        self.redpanda.install_license()

        initial_version = Version(
            self.redpanda.get_version(self.redpanda.nodes[0]))

        admin = Admin(self.redpanda)

        segment_bytes = 512 * 1024
        local_retention_bytes = 2 * 512 * 1024
        topic_config = {
            # Tiny segments
            'segment.bytes': segment_bytes,
            'retention.local.target.bytes': local_retention_bytes
        }

        if initial_version < Version("22.3.0"):
            # We are starting with Redpanda <=22.2, so much use old style declaration of local retention
            topic_config['retention.bytes'] = topic_config[
                'retention.local.target.bytes']
            del topic_config['retention.local.target.bytes']

        # Create a topic with small local retention
        topic = "cipot"
        n_partitions = 1
        self.rpk.create_topic(topic,
                              partitions=n_partitions,
                              replicas=3,
                              config=topic_config)

        # For convenience, write records about the size of a segment
        record_size = segment_bytes

        # Track how many records we produced, so that we can validate consume afterward
        expect_records = defaultdict(int)

        def produce(partition, n_records):
            producer = KgoVerifierProducer(self.test_context,
                                           self.redpanda,
                                           topic,
                                           record_size,
                                           n_records,
                                           batch_max_bytes=int(record_size *
                                                               2))
            producer.start()
            producer.wait()
            producer.free()
            expect_records[partition] += n_records

        def verify():
            for p in range(0, n_partitions):
                self.rpk.consume(topic,
                                 n=expect_records[p],
                                 partition=p,
                                 quiet=True)

        # Ensure some manifests + segments are written from the original version (old feature release)
        for p in range(0, n_partitions):
            n_records = 10
            produce(p, n_records)

        # Wait for archiver to upload to S3
        wait_for_local_storage_truncate(self.redpanda,
                                        topic,
                                        target_bytes=local_retention_bytes +
                                        segment_bytes,
                                        timeout_sec=60)

        # Restart 2/3 nodes, leave last node on old version
        new_version_nodes = self.redpanda.nodes[:-1]
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(new_version_nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

        new_version_node = self.redpanda.nodes[0]
        old_node = self.redpanda.nodes[-1]

        # Verify all data readable
        verify()

        # Pick some arbitrary partition to write data to via a new-version node
        newdata_p = 0

        # There might not be any partitions with leadership on new version
        # node yet, so just transfer one there.

        admin.transfer_leadership_to(
            namespace="kafka",
            topic=topic,
            partition=newdata_p,
            target_id=self.redpanda.idx(new_version_node))

        # Create some new segments in S3 from a new-version node: later we will
        # cause the old node to try and read them to check that compatibility.
        n_records = 10
        produce(newdata_p, n_records)

        # Certain version jumps block tiered storage uploads during the upgrade:
        #  22.2.x -> 22.3.x (when compaction/retention etc was added)
        #  23.1.x -> 23.2.x (when infinite retention was added + other improvements)
        block_uploads_during_upgrade = (
            initial_version < Version("22.3.0")
            or initial_version > Version("23.1.0")
            and initial_version < Version("23.2.0"))

        #  23.1.x -> 23.2.x: the `cloud_storage_manifest_max_upload_interval_sec` is new,
        #                    and needs to be set to avoid the test timing out waiting for
        #                    local log trim, as in 23.2.x we are lazy about uploading manifests by default.
        if initial_version > Version("23.1.0") and initial_version < Version(
                "23.2.0"):
            admin.patch_cluster_config(upsert={
                'cloud_storage_manifest_max_upload_interval_sec':
                1,
                'cloud_storage_spillover_manifest_max_segments':
                2,
                'cloud_storage_spillover_manifest_size':
                None,
            },
                                       node=new_version_node)

        if block_uploads_during_upgrade:
            # If uploads are blocked during upgrade, we expect the new
            # nodes not to be able to trim their local logs.
            time.sleep(10)

            storage = self.redpanda.storage()
            topic_partitions = storage.partitions("kafka", topic)
            for p in topic_partitions:
                if p.num != newdata_p:
                    # We are only checking our test NTP
                    continue

                if p.node in new_version_nodes:
                    # Only new nodes should have paused uploads, and
                    # therefore accumulated local segments
                    assert len(p.segments) > 2
        else:
            # In the general case, S3 PUTs are permitted during upgrade, so we should
            # see local storage getting truncated
            wait_for_local_storage_truncate(
                self.redpanda,
                topic,
                partition_idx=newdata_p,
                target_bytes=local_retention_bytes + segment_bytes,
                timeout_sec=60)

        # capture the cloud storage state to run a progress check later
        bucket_view = BucketView(self.redpanda)
        manifest_mid_upgrade = bucket_view.manifest_for_ntp(
            topic=topic, partition=newdata_p)

        # Move leadership to the old version node and check the partition is readable
        # from there.
        admin.transfer_leadership_to(namespace="kafka",
                                     topic=topic,
                                     partition=newdata_p,
                                     target_id=self.redpanda.idx(old_node))

        produce(newdata_p, 10)

        # Verify all data readable
        verify()

        # Finish the upgrade
        self.redpanda.rolling_restart_nodes([self.redpanda.nodes[-1]],
                                            start_timeout=90,
                                            stop_timeout=90)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        head_version_str = self.redpanda.get_version(self.redpanda.nodes[0])
        head_version = Version(head_version_str)
        assert initial_version < head_version, f"{initial_version} vs {head_version}"
        assert head_version_str in unique_versions, unique_versions

        # Verify all data readable
        verify()

        wait_for_local_storage_truncate(self.redpanda,
                                        topic,
                                        partition_idx=newdata_p,
                                        target_bytes=local_retention_bytes +
                                        segment_bytes,
                                        timeout_sec=60)

        def insync_offset_advanced():
            bucket_view.reset()
            current_manifest = bucket_view.manifest_for_ntp(
                topic=topic, partition=newdata_p)

            return current_manifest["insync_offset"] > manifest_mid_upgrade[
                "insync_offset"]

        # Wait for a manifest re-upload such that rp-storage-tool
        # does not flag expected anomalies due to segment merger reuploads.
        wait_until(insync_offset_advanced,
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg="New manifest was not uploaded post upgrade")

        # Check that spillover commands applied cleanly. If they did not, it's an
        # indication that the upgrade has led to inconsistent state accross
        # archival STMs on different nodes.
        assert self.redpanda.search_log_any(
            "Can't apply spillover_cmd") is False


class RedpandaInstallerTest(RedpandaTest):
    def setUp(self):
        super().setUp()
        # ensure that _installer._head_version is set
        self.redpanda._installer.start()

    @cluster(num_nodes=1)
    def test_install_by_line(self):
        """
        Smoke test that checks RedpandaInstaller.install
        this isn't actually doing upgrades in the traditional sense,
        instead is intentionally wiping and restarting the cluster with new binaries
        """

        # base step, exercise edge case of asking to install a release that is actually HEAD
        head_version, head_version_str = self.redpanda._installer.install(
            self.redpanda.nodes,
            version=self.redpanda._installer._head_version[0:2])
        self.logger.info(f"base step: install(HEAD) -> {head_version_str}")
        self.redpanda.start(clean_nodes=True)
        reported_version = self.redpanda.get_version(self.redpanda.nodes[0])
        assert reported_version == head_version_str, \
            f"installed a different version {reported_version} than advertised {head_version_str}"

        # loop: run down the versions and check them
        version = self.redpanda._installer.highest_from_prior_feature_version(
            head_version)
        limit = 3
        while limit > 0 and version:
            # ask to install a line and check that latest is installed
            line = version[0:2]
            installed_version, installed_version_str = self.redpanda._installer.install(
                self.redpanda.nodes, line)
            self.logger.info(f"install {installed_version_str} from {line=}")
            assert version == installed_version, \
                f"highest feature version {version} and latest in line version {installed_version} do not match"

            self.redpanda.start(clean_nodes=True)
            reported_version = self.redpanda.get_version(
                self.redpanda.nodes[0])
            assert reported_version == installed_version_str, \
                f"installed a different version {reported_version} than advertised {installed_version_str}"

            version = self.redpanda._installer.highest_from_prior_feature_version(
                version)
            limit = limit - 1
