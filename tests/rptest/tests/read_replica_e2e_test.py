# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from re import T
from typing import NamedTuple, Optional, List
from rptest.services.cluster import cluster

from rptest.clients.default import DefaultClient
from rptest.services.redpanda import SISettings
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.util import expect_exception

from ducktape.mark import matrix
from ducktape.tests.test import TestContext

from rptest.services.redpanda import CloudStorageType, CloudStorageTypeAndUrlStyle, MetricsEndpoint, RedpandaService, get_cloud_storage_type, get_cloud_storage_url_style, get_cloud_storage_type_and_url_style, make_redpanda_service
from rptest.services.redpanda_installer import InstallOptions, RedpandaInstaller
from rptest.tests.end_to_end import EndToEndTest
from rptest.utils.expect_rate import ExpectRate, RateTarget
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.util import (wait_until, wait_until_result)
from rptest.utils.mode_checks import skip_fips_mode


class BucketUsage(NamedTuple):
    num_objects: int
    total_bytes: int
    keys: set[str]


# These tests do not wait for data to be written by the origin cluster
# before creating read replicas, so it is expected to see log errors
# when the destination cluster can't find manifests.
# See https://github.com/redpanda-data/redpanda/issues/8965
READ_REPLICA_LOG_ALLOW_LIST = [
    "Failed to download partition manifest",
    "Failed to download manifest",
]


def get_lwm_per_partition(cluster: RedpandaService, topic_name,
                          partition_count):
    id_to_lwm = dict()
    rpk = RpkTool(cluster)
    for prt in rpk.describe_topic(topic_name):
        id_to_lwm[prt.id] = prt.start_offset
    if len(id_to_lwm) != partition_count:
        return False, None
    return True, id_to_lwm


def lwms_are_identical(logger, src_cluster, dst_cluster, topic_name,
                       partition_count):
    # Collect the HWMs for each partition before stopping.
    src_lwms = wait_until_result(lambda: get_lwm_per_partition(
        src_cluster, topic_name, partition_count),
                                 timeout_sec=30,
                                 backoff_sec=1)

    # Ensure that our HWMs on the destination are the same.
    rr_lwms = wait_until_result(lambda: get_lwm_per_partition(
        dst_cluster, topic_name, partition_count),
                                timeout_sec=30,
                                backoff_sec=1)
    logger.info(f"{src_lwms} vs {rr_lwms}")
    return src_lwms == rr_lwms


def get_hwm_per_partition(cluster: RedpandaService, topic_name,
                          partition_count):
    id_to_hwm = dict()
    rpk = RpkTool(cluster)
    for prt in rpk.describe_topic(topic_name):
        id_to_hwm[prt.id] = prt.high_watermark
    if len(id_to_hwm) != partition_count:
        return False, None
    return True, id_to_hwm


def hwms_are_identical(logger, src_cluster, dst_cluster, topic_name,
                       partition_count):
    # Collect the HWMs for each partition before stopping.
    src_hwms = wait_until_result(lambda: get_hwm_per_partition(
        src_cluster, topic_name, partition_count),
                                 timeout_sec=30,
                                 backoff_sec=1)

    # Ensure that our HWMs on the destination are the same.
    rr_hwms = wait_until_result(lambda: get_hwm_per_partition(
        dst_cluster, topic_name, partition_count),
                                timeout_sec=30,
                                backoff_sec=1)
    logger.info(f"{src_hwms} vs {rr_hwms}")
    return src_hwms == rr_hwms


def create_read_replica_topic(dst_cluster, topic_name, bucket_name) -> None:
    rpk_dst_cluster = RpkTool(dst_cluster)
    # NOTE: we set 'redpanda.remote.readreplica' to ORIGIN
    # cluster's bucket
    conf = {
        'redpanda.remote.readreplica': bucket_name,
    }
    rpk_dst_cluster.create_topic(topic_name, config=conf)

    def has_leader():
        partitions = list(
            rpk_dst_cluster.describe_topic(topic_name, tolerant=True))
        for part in partitions:
            if part.leader == -1:
                return False
        return True

    wait_until(has_leader,
               timeout_sec=60,
               backoff_sec=10,
               err_msg="No leader in read-replica")


class TestReadReplicaService(EndToEndTest):
    log_segment_size = 1048576  # 5MB
    topic_name = "panda-topic"

    def __init__(self, test_context: TestContext):
        extra_rp_conf = dict(cloud_storage_spillover_manifest_size=None)
        super(TestReadReplicaService, self).__init__(
            test_context=test_context,
            si_settings=SISettings(
                test_context,
                cloud_storage_max_connections=5,
                log_segment_size=TestReadReplicaService.log_segment_size,
                cloud_storage_readreplica_manifest_sync_timeout_ms=500,
                cloud_storage_segment_max_upload_interval_sec=5,
                # Ensure that the replica can read from spilled metadata
                cloud_storage_spillover_manifest_max_segments=4,
                # Ensure metadata spilling happens promptly
                cloud_storage_housekeeping_interval_ms=100,
                fast_uploads=True),
            extra_rp_conf=extra_rp_conf)

        # Read reaplica shouldn't have it's own bucket.
        # We're adding 'none' as a bucket name without creating
        # an actual bucket with such name.
        self.rr_settings = SISettings(
            test_context,
            bypass_bucket_creation=True,
            cloud_storage_enable_remote_write=False,
            cloud_storage_max_connections=5,
            log_segment_size=TestReadReplicaService.log_segment_size,
            cloud_storage_readreplica_manifest_sync_timeout_ms=500,
            cloud_storage_segment_max_upload_interval_sec=5,
            cloud_storage_housekeeping_interval_ms=10)
        self.second_cluster = None

    def start_second_cluster(self) -> None:
        # NOTE: the RRR cluster won't have a bucket, so don't upload.
        extra_rp_conf = dict(enable_cluster_metadata_upload_loop=False)
        self.second_cluster = make_redpanda_service(
            self.test_context,
            num_brokers=3,
            si_settings=self.rr_settings,
            extra_rp_conf=extra_rp_conf)
        self.second_cluster.start(start_si=False)

    def create_read_replica_topic(self) -> None:
        create_read_replica_topic(self.second_cluster, self.topic_name,
                                  self.si_settings.cloud_storage_bucket)

    def start_consumer(self) -> None:
        # important side effect for superclass; we will use the replica
        # consumer from here on.
        self.consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.second_cluster,
            topic=self.topic_name,
            group_id='consumer_test_group',
            on_record_consumed=self.on_record_consumed)
        self.consumer.start()

    def start_producer(self) -> None:
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_name,
            throughput=1000,
            message_validator=is_int_with_prefix)
        self.producer.start()

    def create_read_replica_topic_success(self) -> bool:
        try:
            self.create_read_replica_topic()
        except RpkException as e:
            if "The server experienced an unexpected error when processing the request" in str(
                    e):
                return False
            else:
                raise
        else:
            return True

    def _setup_read_replica(self,
                            num_messages=0,
                            partition_count=3,
                            producer_timeout=None) -> None:
        if producer_timeout is None:
            producer_timeout = 30

        self.logger.info(f"Setup read replica \"{self.topic_name}\", : "
                         f"{num_messages} msg, {partition_count} "
                         "partitions.")
        # Create original topic
        self.start_redpanda(3, si_settings=self.si_settings)
        spec = TopicSpec(name=self.topic_name,
                         partition_count=partition_count,
                         replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)

        if num_messages > 0:
            self.start_producer()
            wait_until(
                lambda: self.producer.num_acked > num_messages,
                timeout_sec=producer_timeout,
                err_msg=
                f"Producer only produced {self.producer.num_acked}/{num_messages} messages in {producer_timeout}"
            )
            self.logger.info("Stopping producer after writing up to offsets %s" %\
                            str(self.producer.last_acked_offsets))
            self.producer.stop()

        self.start_second_cluster()

        # wait until the read replica topic creation succeeds
        wait_until(
            self.create_read_replica_topic_success,
            timeout_sec=300,
            backoff_sec=5,
            err_msg="Could not create read replica topic. Most likely " +
            "because topic manifest is not in S3.")
        if num_messages > 0:
            wait_until(lambda: self.second_cluster.metric_sum(
                "redpanda_kafka_max_offset",
                topic=self.topic_name,
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS) > 0,
                       timeout_sec=10,
                       backoff_sec=1)

    def _bucket_usage(self) -> BucketUsage:
        assert self.redpanda and self.redpanda.cloud_storage_client
        keys: set[str] = set()
        s3 = self.redpanda.cloud_storage_client
        num_objects = total_bytes = 0
        bucket = self.si_settings.cloud_storage_bucket
        for o in s3.list_objects(bucket):
            num_objects += 1
            total_bytes += o.content_length
            keys.add(o.key)
        self.redpanda.logger.info(f"bucket usage {num_objects} objects, " +
                                  f"{total_bytes} bytes for {bucket}")
        return BucketUsage(num_objects, total_bytes, keys)

    def _bucket_delta(self, bu1: BucketUsage,
                      bu2: BucketUsage) -> Optional[BucketUsage]:
        """ Return None if both bucket usage reports are equal.
            Otherwise, return deltas. """
        obj_delta = bu2.num_objects - bu1.num_objects
        bytes_delta = bu2.total_bytes - bu1.total_bytes
        keys_delta = bu2.keys.difference(bu1.keys)
        if obj_delta or bytes_delta or len(keys_delta) > 0:
            return BucketUsage(obj_delta, bytes_delta, keys_delta)
        else:
            return None

    @cluster(num_nodes=7, log_allow_list=READ_REPLICA_LOG_ALLOW_LIST)
    @matrix(
        partition_count=[5],
        cloud_storage_type=get_cloud_storage_type(docker_use_arbitrary=True))
    def test_identical_lwms_after_delete_records(
            self, partition_count: int,
            cloud_storage_type: CloudStorageType) -> None:
        self._setup_read_replica(partition_count=partition_count,
                                 num_messages=1000)
        rpk = RpkTool(self.redpanda)

        def set_lwm(new_lwm):
            response = rpk.trim_prefix(self.topic_name,
                                       new_lwm,
                                       partitions=[0])
            assert response[0].error_msg == '', response[0].error_msg
            assert response[0].new_start_offset == new_lwm

        def check_lwm(new_lwm):
            topics_info = list(rpk.describe_topic(self.topic_name))
            self.logger.info(f"{self.topic_name} topics_info: {topics_info}")
            if len(topics_info) == 0:
                return False
            topic_info = topics_info[0]
            for t in topics_info:
                if t.id == 0:
                    topic_info = t
                    break
            return topic_info.start_offset == new_lwm, topic_info

        wait_until(lambda: check_lwm(0), timeout_sec=10, backoff_sec=1)
        set_lwm(5)
        wait_until(lambda: check_lwm(5), timeout_sec=10, backoff_sec=1)

        def clusters_report_identical_lwms():
            return lwms_are_identical(self.logger, self.redpanda,
                                      self.second_cluster, self.topic_name,
                                      partition_count)

        wait_until(clusters_report_identical_lwms,
                   timeout_sec=30,
                   backoff_sec=1)

        # As a sanity check, ensure the same is true after a restart.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        wait_until(clusters_report_identical_lwms,
                   timeout_sec=30,
                   backoff_sec=1)

        check_lwm(5)
        set_lwm(6)
        check_lwm(6)

        self.second_cluster.restart_nodes(self.second_cluster.nodes)
        wait_until(clusters_report_identical_lwms,
                   timeout_sec=30,
                   backoff_sec=1)
        check_lwm(6)
        set_lwm(7)
        check_lwm(7)

    @cluster(num_nodes=8, log_allow_list=READ_REPLICA_LOG_ALLOW_LIST)
    @matrix(
        partition_count=[5],
        cloud_storage_type=get_cloud_storage_type(docker_use_arbitrary=True))
    def test_identical_hwms(self, partition_count: int,
                            cloud_storage_type: CloudStorageType) -> None:
        self._setup_read_replica(partition_count=partition_count,
                                 num_messages=1000)
        self.start_consumer()
        self.run_validation(min_records=1000)  # calls self.consumer.stop()

        def clusters_report_identical_hwms():
            return hwms_are_identical(self.logger, self.redpanda,
                                      self.second_cluster, self.topic_name,
                                      partition_count)

        wait_until(clusters_report_identical_hwms,
                   timeout_sec=30,
                   backoff_sec=1)

        # As a sanity check, ensure the same is true after a restart.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        wait_until(clusters_report_identical_hwms,
                   timeout_sec=30,
                   backoff_sec=1)

        self.second_cluster.restart_nodes(self.second_cluster.nodes)
        wait_until(clusters_report_identical_hwms,
                   timeout_sec=30,
                   backoff_sec=1)

    @cluster(num_nodes=7, log_allow_list=READ_REPLICA_LOG_ALLOW_LIST)
    @matrix(partition_count=[10], cloud_storage_type=get_cloud_storage_type())
    def test_writes_forbidden(self, partition_count: int,
                              cloud_storage_type: CloudStorageType) -> None:
        """
        Verify that the read replica cluster does not permit writes,
        and does not perform other data-modifying actions such as
        removing objects from S3 on topic deletion.
        """

        self._setup_read_replica(partition_count=partition_count,
                                 num_messages=1000)
        second_rpk = RpkTool(self.second_cluster)
        with expect_exception(
                RpkException, lambda e:
                "unable to produce record: INVALID_TOPIC_EXCEPTION: The request attempted to perform an operation on an invalid topic."
                in str(e)):
            second_rpk.produce(self.topic_name, "", "test payload")

        objects_before = set(
            self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket))
        assert len(objects_before) > 0
        second_rpk.delete_topic(self.topic_name)
        objects_after = set(
            self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket))
        if len(objects_after) < len(objects_before):
            deleted = objects_before - objects_after
            self.logger.error(f"Objects unexpectedly deleted: {deleted}")

            # This is not an exact equality check because the source
            # cluster might still be uploading segments: the object
            # count is permitted to increase.
            assert len(objects_after) >= len(objects_before)

    # fips on S3 is not compatible with path-style urls. TODO remove this once get_cloud_storage_type_and_url_style is fips aware
    @skip_fips_mode
    @cluster(num_nodes=9, log_allow_list=READ_REPLICA_LOG_ALLOW_LIST)
    @matrix(
        partition_count=[10],
        cloud_storage_type_and_url_style=get_cloud_storage_type_and_url_style(
        ))
    def test_simple_end_to_end(
        self, partition_count: int,
        cloud_storage_type_and_url_style: List[CloudStorageTypeAndUrlStyle]
    ) -> None:
        data_timeout = 300
        self._setup_read_replica(num_messages=100000,
                                 partition_count=partition_count,
                                 producer_timeout=300)

        # Consume from read replica topic and validate
        self.start_consumer()
        self.run_validation(
            consumer_timeout_sec=data_timeout)  # calls self.consumer.stop()

        # Run consumer again, this time with source cluster stopped.
        # Now we can test that replicas do not write to s3.
        self.start_consumer()

        # Assert zero bytes written for at least 20 seconds.
        total_bytes = lambda: self._bucket_usage().total_bytes
        assert self.redpanda and self.redpanda.logger
        er = ExpectRate(total_bytes, self.redpanda.logger)
        zero_growth = RateTarget(max_total_sec=60,
                                 target_sec=20,
                                 target_min_rate=0,
                                 target_max_rate=0)
        er.expect_rate(zero_growth)

        # Get current bucket usage
        pre_usage = self._bucket_usage()
        self.logger.info(f"pre_usage {pre_usage}")

        # Let replica consumer run to completion, assert no s3 writes
        self.run_consumer_validation(consumer_timeout_sec=data_timeout)

        post_usage = self._bucket_usage()
        self.logger.info(f"post_usage {post_usage}")
        delta = self._bucket_delta(pre_usage, post_usage)
        if delta:
            m = f"S3 Bucket usage changed during read replica test: {delta}"
            assert False, m


class ReadReplicasUpgradeTest(EndToEndTest):
    log_segment_size = 1024 * 1024
    topic_name = "panda-topic"

    def __init__(self, test_context: TestContext):
        super(ReadReplicasUpgradeTest, self).__init__(
            test_context=test_context,
            si_settings=SISettings(
                test_context,
                cloud_storage_max_connections=5,
                log_segment_size=self.log_segment_size,
                cloud_storage_readreplica_manifest_sync_timeout_ms=500,
                cloud_storage_segment_max_upload_interval_sec=3,
                fast_uploads=True))

        # Unlike other read replica tests, create a bucket so we can upgrade to
        # a version that uses cluster metadata uploads by default (that expects
        # a bucket).
        self.rr_settings = SISettings(
            test_context,
            bypass_bucket_creation=False,
            cloud_storage_max_connections=5,
            log_segment_size=self.log_segment_size,
            cloud_storage_readreplica_manifest_sync_timeout_ms=500,
            cloud_storage_segment_max_upload_interval_sec=1,
            cloud_storage_housekeeping_interval_ms=1)
        self.second_cluster = None

    # before v24.2, dns query to s3 endpoint do not include the bucketname, which is required for AWS S3 fips endpoints
    @skip_fips_mode
    @cluster(num_nodes=8)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_upgrades(self, cloud_storage_type):
        partition_count = 1
        install_opts = InstallOptions(install_previous_version=True)
        self.start_redpanda(3,
                            si_settings=self.si_settings,
                            install_opts=install_opts,
                            license_required=True)
        spec = TopicSpec(name=self.topic_name,
                         partition_count=partition_count,
                         replication_factor=3)
        DefaultClient(self.redpanda).create_topic(spec)
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_name,
            throughput=100,
            message_validator=is_int_with_prefix)
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 1000,
                       timeout_sec=30,
                       err_msg="Producer failed to produce messages for %ds." %\
                       30)
        self.producer.stop()

        self.second_cluster = make_redpanda_service(
            self.test_context, num_brokers=3, si_settings=self.rr_settings)
        previous_version = self.second_cluster._installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        self.second_cluster._installer.install(self.second_cluster.nodes,
                                               previous_version)
        self.second_cluster.start(start_si=True)
        self.second_cluster.install_license()
        create_read_replica_topic(self.second_cluster, self.topic_name,
                                  self.si_settings.cloud_storage_bucket)

        def cluster_has_offsets(cluster):
            hwms = wait_until_result(lambda: get_hwm_per_partition(
                cluster, self.topic_name, partition_count),
                                     timeout_sec=30,
                                     backoff_sec=1)
            if len(hwms) != partition_count:
                return False, None
            if any([hwms[p] == 0 for p in range(partition_count)]):
                return False, None
            return True, hwms

        rr_hwms = wait_until_result(
            lambda: cluster_has_offsets(self.second_cluster),
            timeout_sec=30,
            backoff_sec=1)
        self.logger.info(f"pre-upgrade read replica HWMs: {rr_hwms}")
        # Upgrade the read replica cluster first.
        self.second_cluster._installer.install(self.second_cluster.nodes,
                                               RedpandaInstaller.HEAD)
        self.second_cluster.restart_nodes(self.second_cluster.nodes)

        new_rr_hwms = wait_until_result(
            lambda: cluster_has_offsets(self.second_cluster),
            timeout_sec=30,
            backoff_sec=1)
        self.logger.info(f"post-upgrade read replica HWMs: {new_rr_hwms}")

        # Then upgrade the source cluster and make sure the source and RRR
        # cluster match.
        self.redpanda._installer.install(self.redpanda.nodes,
                                         RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Speed up uploads of the manifest.
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set(
            "cloud_storage_manifest_max_upload_interval_sec", "5")

        def clusters_report_identical_hwms():
            return hwms_are_identical(self.logger, self.redpanda,
                                      self.second_cluster, self.topic_name,
                                      partition_count)

        # NOTE: immediately after restarting, on older versions the HWMs may
        # not be identical until after we upload some data.

        # Writes some more to update the manifest with the new version.
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_name,
            throughput=100,
            message_validator=is_int_with_prefix)
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 1000,
                       timeout_sec=30,
                       err_msg="Producer failed to produce messages for %ds." %\
                       30)
        self.producer.stop()
        wait_until(clusters_report_identical_hwms,
                   timeout_sec=30,
                   backoff_sec=1)


class TestMisconfiguredReadReplicaService(EndToEndTest):
    @cluster(num_nodes=1)
    def test_create_replica_without_cloud_enabled(self):
        """
        Regression test for an issue where creating a read replica when cloud
        isn't configured crashes the node.
        """
        self.start_redpanda(1)
        rpk = RpkTool(self.redpanda)
        conf = {
            'redpanda.remote.readreplica': "dummy_bucket",
        }
        topic_name = "no_cloud_no_problems"
        try:
            rpk.create_topic(topic_name, replicas=1, config=conf)
            assert False, "Expected failure"
        except Exception as e:
            assert "Configuration is invalid" in str(e), str(e)
        assert self.redpanda.all_up()
        topics = rpk.list_topics()
        assert len(list(topics)) == 0, f"Unexpected partitions: {topics}"
