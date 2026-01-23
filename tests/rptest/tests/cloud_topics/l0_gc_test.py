# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.clients.admin.v2 import Admin, l0_gc_pb, ntp_pb
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.services.kgo_verifier_services import (
    KgoVerifierParams,
    KgoVerifierMultiProducer,
    KgoVerifierProducer,
)
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from ducktape.errors import TimeoutError
from ducktape.tests.test import TestContext
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SISettings,
    get_cloud_storage_type,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception, wait_until_result


class CloudTopicsL0GCTest(RedpandaTest):
    def __init__(self, test_context: TestContext):
        self.test_context = test_context
        si_settings = SISettings(
            test_context=test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        extra_rp_conf = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "cloud_topics_reconciliation_min_interval": 2000,
            "cloud_topics_reconciliation_max_interval": 2000,
            "cloud_topics_epoch_service_epoch_increment_interval": 5000,
            "cloud_topics_epoch_service_local_epoch_cache_duration": 5000,
            "cloud_topics_short_term_gc_minimum_object_age": 10000,
            "cloud_topics_short_term_gc_interval": 2000,
            "cloud_topics_short_term_gc_backoff_interval": 10000,
        }
        super(CloudTopicsL0GCTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=si_settings,
        )

    @property
    def l0_gc_client(self):
        return Admin(self.redpanda).l0_gc()

    def __create_topics(self, topics: list[TopicSpec]):
        rpk = RpkTool(self.redpanda)
        for spec in topics:
            rpk.create_topic(
                spec.name,
                spec.partition_count,
                spec.replication_factor,
                config={"redpanda.cloud_topic.enabled": "true"},
            )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_l0_gc(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=2)]
        self.__create_topics(self.topics)

        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=[spec.name for spec in self.topics],
            msg_size=1024,
            rate_limit_bps=2 * 1024 * 1024,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(300, timeout_sec=90)

        # TODO: we are only checking that deletes are happening here (and should
        # also be happening in parallel with the repeater's fetch/produce
        # workload), but we do want to add tests that check constraints
        def get_num_objects_deleted():
            samples = self.redpanda.metrics_sample(
                "vectorized_cloud_topics_l0_gc_objects_deleted_total"
            )
            self.logger.info(samples)
            if samples is not None and samples.samples:
                return int(sum(s.value for s in samples.samples))
            return 0

        wait_until(
            lambda: get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type()[0:1])
    def test_l0_gc_pause(self, cloud_storage_type: CloudStorageType):
        self.topics = [
            TopicSpec(partition_count=2),
        ]
        self.__create_topics(self.topics)

        def get_num_objects_deleted():
            samples = self.redpanda.metrics_sample(
                "vectorized_cloud_topics_l0_gc_objects_deleted_total"
            )
            self.logger.info(samples)
            if samples is not None and samples.samples:
                n = int(sum(s.value for s in samples.samples))
                print(n)
                return n
            return 0

        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=[spec.name for spec in self.topics],
            msg_size=1024,
            rate_limit_bps=2 * 1024 * 1024,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(300, timeout_sec=90)

        print("DID IT STOP?")

        wait_until(
            lambda: get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )
        pause_response = self.l0_gc_client.pause(l0_gc_pb.PauseRequest())
        assert pause_response is not None, "PauseResponse should not be None"

        n_deleted = get_num_objects_deleted()
        with expect_exception(TimeoutError, lambda _: True):
            wait_until(
                lambda: get_num_objects_deleted() > n_deleted,
                timeout_sec=30,
                backoff_sec=5,
                retry_on_exc=True,
            )
            result = get_num_objects_deleted()
            print(f"{result=} > {n_deleted=}")


class L0GcAdminTest(RedpandaTest):
    """
    Test class for the L0 GC admin API endpoints.

    This test verifies that the new L0 GC admin API endpoints are available
    and return the expected stub responses (failed status).
    """

    def __init__(self, test_context):
        # Configure cloud topics settings

        super(L0GcAdminTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                CLOUD_TOPICS_CONFIG_STR: True,
                "enable_cluster_metadata_upload_loop": False,
            },
            si_settings=SISettings(
                test_context,
                cloud_storage_max_connections=10,
                cloud_storage_enable_remote_read=False,
                cloud_storage_enable_remote_write=False,
                fast_uploads=True,
            ),
        )
        self.rpk = RpkTool(self.redpanda)
        self.test_context = test_context

    @cluster(num_nodes=3)
    def test_advance_epoch_endpoint_availability(self):
        """
        Test that the advance_epoch endpoint is available and returns expected responses.

        This test verifies:
        1. The endpoint is accessible via the admin API
        2. The endpoint accepts requests with topic partitions
        3. The endpoint returns failed status for each partition (stub implementation)
        """
        admin = Admin(self.redpanda)

        # Create a test topic
        topic_name = "test_l0_gc_topic"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        # Prepare the advance epoch request
        l0_gc_client = admin.l0_gc()

        # Build request with multiple partitions
        request = l0_gc_pb.AdvanceEpochRequest()

        for partition_id in range(partition_count):
            # Create TopicPartitionEpoch for each partition
            tp_epoch = l0_gc_pb.TopicPartitionEpoch()

            # Set the partition
            tp = ntp_pb.TopicPartition()
            tp.topic = topic_name
            tp.partition = partition_id
            tp_epoch.partition.CopyFrom(tp)

            # Set epoch to 0 (initial epoch)
            tp_epoch.epoch = 0

            request.partitions.append(tp_epoch)

        self.logger.info(
            f"Sending advance_epoch request for {partition_count} partitions"
        )

        # Call the advance_epoch endpoint
        response = l0_gc_client.advance_epoch(request)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert len(response.partitions) == partition_count, (
            f"Expected {partition_count} results, got {len(response.partitions)}"
        )

        # Verify each partition result has failed status (stub implementation)
        for i, result in enumerate(response.partitions):
            self.logger.info(
                f"Partition {i} result: "
                f"topic={result.partition.topic if result.HasField('partition') else 'N/A'}, "
                f"partition={result.partition.partition if result.HasField('partition') else 'N/A'}, "
                f"result={result.error}"
            )

            assert result.error == l0_gc_pb.L0_GC_ERROR_FAILED, (
                f"Expected FAILED status for partition {i}, got {result.error}"
            )

        self.logger.info(
            "Successfully verified advance_epoch endpoint returns failed status as expected"
        )

    @cluster(num_nodes=3)
    def test_advance_epoch_empty_request(self):
        """
        Test that the advance_epoch endpoint handles empty requests correctly.
        """
        admin = Admin(self.redpanda)
        l0_gc_client = admin.l0_gc()

        # Send empty request
        request = l0_gc_pb.AdvanceEpochRequest()

        self.logger.info("Sending empty advance_epoch request")
        response = l0_gc_client.advance_epoch(request)

        # Verify empty response
        assert response is not None, "Response should not be None"
        assert len(response.partitions) == 0, (
            f"Expected 0 results for empty request, got {len(response.partitions)}"
        )

        self.logger.info("Successfully verified advance_epoch handles empty requests")

    @cluster(num_nodes=3)
    def test_advance_epoch_single_partition(self):
        """
        Test the advance_epoch endpoint with a single partition.
        """
        admin = Admin(self.redpanda)

        # Create a test topic
        topic_name = "test_l0_gc_single_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=1,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with single partition
        request = l0_gc_pb.AdvanceEpochRequest()
        tp_epoch = l0_gc_pb.TopicPartitionEpoch()

        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = 0
        tp_epoch.partition.CopyFrom(tp)
        tp_epoch.epoch = 5  # Test with non-zero epoch

        request.partitions.append(tp_epoch)

        self.logger.info(
            f"Sending advance_epoch request for single partition with epoch=5"
        )

        # Call the endpoint
        response = l0_gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_FAILED

        self.logger.info("Successfully verified single partition request")

    @cluster(num_nodes=3)
    def test_advance_epoch_topic_not_found(self):
        """
        Test that the advance_epoch endpoint returns TOPIC_NOT_FOUND error
        when the requested topic does not exist.
        """
        admin = Admin(self.redpanda)
        l0_gc_client = admin.l0_gc()

        # Use a topic name that doesn't exist
        non_existent_topic = "this_topic_does_not_exist"

        # Build request for non-existent topic
        request = l0_gc_pb.AdvanceEpochRequest()
        tp_epoch = l0_gc_pb.TopicPartitionEpoch()

        tp = ntp_pb.TopicPartition()
        tp.topic = non_existent_topic
        tp.partition = 0
        tp_epoch.partition.CopyFrom(tp)
        tp_epoch.epoch = 0

        request.partitions.append(tp_epoch)

        self.logger.info(
            f"Sending advance_epoch request for non-existent topic: {non_existent_topic}"
        )

        # Call the endpoint
        response = l0_gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_TOPIC_NOT_FOUND, (
            f"Expected TOPIC_NOT_FOUND error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified TOPIC_NOT_FOUND error for non-existent topic"
        )

    @cluster(num_nodes=3)
    def test_advance_epoch_invalid_partition_too_high(self):
        """
        Test that the advance_epoch endpoint returns INVALID_PARTITION error
        when the requested partition number is too high (>= partition_count).
        """
        admin = Admin(self.redpanda)

        # Create a test topic with 3 partitions
        topic_name = "test_l0_gc_invalid_partition"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with partition number that's too high
        # Valid partitions are 0, 1, 2, so partition 3 should be invalid
        request = l0_gc_pb.AdvanceEpochRequest()
        tp_epoch = l0_gc_pb.TopicPartitionEpoch()

        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = partition_count  # This should be invalid
        tp_epoch.partition.CopyFrom(tp)
        tp_epoch.epoch = 0

        request.partitions.append(tp_epoch)

        self.logger.info(
            f"Sending advance_epoch request for partition {partition_count} "
            f"(topic has only {partition_count} partitions: 0-{partition_count - 1})"
        )

        # Call the endpoint
        response = l0_gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified INVALID_PARTITION error for partition number too high"
        )

    @cluster(num_nodes=3)
    def test_advance_epoch_invalid_partition_negative(self):
        """
        Test that the advance_epoch endpoint returns INVALID_PARTITION error
        when the requested partition number is negative.
        """
        admin = Admin(self.redpanda)

        # Create a test topic
        topic_name = "test_l0_gc_negative_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with negative partition number
        request = l0_gc_pb.AdvanceEpochRequest()
        tp_epoch = l0_gc_pb.TopicPartitionEpoch()

        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = -1  # Invalid negative partition
        tp_epoch.partition.CopyFrom(tp)
        tp_epoch.epoch = 0

        request.partitions.append(tp_epoch)

        self.logger.info("Sending advance_epoch request for negative partition number")

        # Call the endpoint
        response = l0_gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified INVALID_PARTITION error for negative partition"
        )

    @cluster(num_nodes=3)
    def test_advance_epoch_not_cloud_topic(self):
        """
        Test that the advance_epoch endpoint returns NOT_CLOUD_TOPIC error
        when the requested topic is not a cloud topic.
        """
        admin = Admin(self.redpanda)

        # Create a regular (non-cloud) topic
        topic_name = "test_l0_gc_not_cloud_topic"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request for the non-cloud topic
        request = l0_gc_pb.AdvanceEpochRequest()
        tp_epoch = l0_gc_pb.TopicPartitionEpoch()

        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = 0
        tp_epoch.partition.CopyFrom(tp)
        tp_epoch.epoch = 0

        request.partitions.append(tp_epoch)

        self.logger.info(
            f"Sending advance_epoch request for non-cloud topic: {topic_name}"
        )

        # Call the endpoint
        response = l0_gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_NOT_CLOUD_TOPIC, (
            f"Expected NOT_CLOUD_TOPIC error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified NOT_CLOUD_TOPIC error for non-cloud topic"
        )

    @cluster(num_nodes=3)
    def test_advance_epoch_mixed_errors(self):
        """
        Test that the advance_epoch endpoint correctly handles requests with
        multiple partitions that have different error conditions.
        """
        admin = Admin(self.redpanda)

        # Create a cloud topic
        topic_name = "test_l0_gc_mixed_errors"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        # Create a non-cloud topic
        non_cloud_topic = "test_l0_gc_mixed_errors_non_cloud"
        self.rpk.create_topic(
            topic=non_cloud_topic,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with multiple partitions having different error conditions
        request = l0_gc_pb.AdvanceEpochRequest()

        # 1. Valid cloud topic, valid partition (should return FAILED since stub)
        tp_epoch1 = l0_gc_pb.TopicPartitionEpoch()
        tp1 = ntp_pb.TopicPartition()
        tp1.topic = topic_name
        tp1.partition = 0
        tp_epoch1.partition.CopyFrom(tp1)
        tp_epoch1.epoch = 0
        request.partitions.append(tp_epoch1)

        # 2. Non-existent topic
        tp_epoch2 = l0_gc_pb.TopicPartitionEpoch()
        tp2 = ntp_pb.TopicPartition()
        tp2.topic = "non_existent_topic"
        tp2.partition = 0
        tp_epoch2.partition.CopyFrom(tp2)
        tp_epoch2.epoch = 0
        request.partitions.append(tp_epoch2)

        # 3. Valid topic, invalid partition (too high)
        tp_epoch3 = l0_gc_pb.TopicPartitionEpoch()
        tp3 = ntp_pb.TopicPartition()
        tp3.topic = topic_name
        tp3.partition = partition_count + 1
        tp_epoch3.partition.CopyFrom(tp3)
        tp_epoch3.epoch = 0
        request.partitions.append(tp_epoch3)

        # 4. Valid topic, invalid partition (negative)
        tp_epoch4 = l0_gc_pb.TopicPartitionEpoch()
        tp4 = ntp_pb.TopicPartition()
        tp4.topic = topic_name
        tp4.partition = -5
        tp_epoch4.partition.CopyFrom(tp4)
        tp_epoch4.epoch = 0
        request.partitions.append(tp_epoch4)

        # 5. Non-cloud topic
        tp_epoch5 = l0_gc_pb.TopicPartitionEpoch()
        tp5 = ntp_pb.TopicPartition()
        tp5.topic = non_cloud_topic
        tp5.partition = 0
        tp_epoch5.partition.CopyFrom(tp5)
        tp_epoch5.epoch = 0
        request.partitions.append(tp_epoch5)

        self.logger.info("Sending advance_epoch request with mixed error conditions")

        # Call the endpoint
        response = l0_gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 5, (
            f"Expected 5 results, got {len(response.partitions)}"
        )

        # Verify each result
        # Result 1: Valid cloud topic and partition (should return FAILED in stub)
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_FAILED, (
            f"Expected FAILED for valid topic/partition, got {response.partitions[0].error}"
        )

        # Result 2: Non-existent topic
        assert response.partitions[1].error == l0_gc_pb.L0_GC_ERROR_TOPIC_NOT_FOUND, (
            f"Expected TOPIC_NOT_FOUND for non-existent topic, got {response.partitions[1].error}"
        )

        # Result 3: Invalid partition (too high)
        assert response.partitions[2].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for high partition, got {response.partitions[2].error}"
        )

        # Result 4: Invalid partition (negative)
        assert response.partitions[3].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for negative partition, got {response.partitions[3].error}"
        )

        # Result 5: Non-cloud topic
        assert response.partitions[4].error == l0_gc_pb.L0_GC_ERROR_NOT_CLOUD_TOPIC, (
            f"Expected NOT_CLOUD_TOPIC for non-cloud topic, got {response.partitions[4].error}"
        )

        self.logger.info(
            "Successfully verified mixed error conditions are handled correctly"
        )

    @cluster(num_nodes=4)
    def test_get_epoch_endpoint_availability(self):
        """
        Test that the get_epoch endpoint is available and returns expected responses.

        This test verifies:
        1. The endpoint is accessible via the admin API
        2. The endpoint accepts requests with topic partitions
        3. The endpoint returns a nonzero epoch for each partition
        """
        admin = Admin(self.redpanda)

        # Create a test topic
        topic_name = "test_l0_gc_get_epoch_topic"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size=1024,
            msg_count=1024,
            timeout_sec=30,
        )

        # Prepare the get epoch request
        l0_gc_client = admin.l0_gc()

        # Build request with multiple partitions
        request = l0_gc_pb.GetEpochRequest()

        for partition_id in range(partition_count):
            # Create TopicPartition for each partition
            tp = ntp_pb.TopicPartition()
            tp.topic = topic_name
            tp.partition = partition_id

            request.partitions.append(tp)

        self.logger.info(f"Sending get_epoch request for {partition_count} partitions")

        # Call the get_epoch endpoint
        response = l0_gc_client.get_epoch(request)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert len(response.partitions) == partition_count, (
            f"Expected {partition_count} results, got {len(response.partitions)}"
        )

        # Verify each partition result has failed status (stub implementation)
        for i, result in enumerate(response.partitions):
            self.logger.info(
                f"Partition {i} result: "
                f"topic={result.partition.topic if result.HasField('partition') else 'N/A'}, "
                f"partition={result.partition.partition if result.HasField('partition') else 'N/A'}, "
                f"result={result.epoch}"
            )

            assert result.epoch > 0, (
                f"Expected nonzero epoch for partition {i}, got {result.epoch}"
            )

        self.logger.info(
            "Successfully verified get_epoch endpoint returns valid epochs as expected"
        )

    @cluster(num_nodes=3)
    def test_get_epoch_empty_request(self):
        """
        Test that the get_epoch endpoint handles empty requests correctly.
        """
        admin = Admin(self.redpanda)
        l0_gc_client = admin.l0_gc()

        # Send empty request
        request = l0_gc_pb.GetEpochRequest()

        self.logger.info("Sending empty get_epoch request")
        response = l0_gc_client.get_epoch(request)

        # Verify empty response
        assert response is not None, "Response should not be None"
        assert len(response.partitions) == 0, (
            f"Expected 0 results for empty request, got {len(response.partitions)}"
        )

        self.logger.info("Successfully verified get_epoch handles empty requests")

    @cluster(num_nodes=3)
    def test_get_epoch_single_partition(self):
        """
        Test the get_epoch endpoint with a single partition.
        """
        admin = Admin(self.redpanda)

        # Create a test topic
        topic_name = "test_l0_gc_get_epoch_single_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=1,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with single partition
        request = l0_gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = 0

        request.partitions.append(tp)

        self.logger.info("Sending get_epoch request for single partition")

        # Call the endpoint
        response = l0_gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_FAILED

        self.logger.info("Successfully verified single partition request")

    @cluster(num_nodes=3)
    def test_get_epoch_topic_not_found(self):
        """
        Test that the get_epoch endpoint returns TOPIC_NOT_FOUND error
        when the requested topic does not exist.
        """
        admin = Admin(self.redpanda)
        l0_gc_client = admin.l0_gc()

        # Use a topic name that doesn't exist
        non_existent_topic = "this_topic_does_not_exist_get_epoch"

        # Build request for non-existent topic
        request = l0_gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = non_existent_topic
        tp.partition = 0

        request.partitions.append(tp)

        self.logger.info(
            f"Sending get_epoch request for non-existent topic: {non_existent_topic}"
        )

        # Call the endpoint
        response = l0_gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_TOPIC_NOT_FOUND, (
            f"Expected TOPIC_NOT_FOUND error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified TOPIC_NOT_FOUND error for non-existent topic"
        )

    @cluster(num_nodes=3)
    def test_get_epoch_invalid_partition_too_high(self):
        """
        Test that the get_epoch endpoint returns INVALID_PARTITION error
        when the requested partition number is too high (>= partition_count).
        """
        admin = Admin(self.redpanda)

        # Create a test topic with 3 partitions
        topic_name = "test_l0_gc_get_epoch_invalid_partition"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with partition number that's too high
        # Valid partitions are 0, 1, 2, so partition 3 should be invalid
        request = l0_gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = partition_count  # This should be invalid

        request.partitions.append(tp)

        self.logger.info(
            f"Sending get_epoch request for partition {partition_count} "
            f"(topic has only {partition_count} partitions: 0-{partition_count - 1})"
        )

        # Call the endpoint
        response = l0_gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified INVALID_PARTITION error for partition number too high"
        )

    @cluster(num_nodes=3)
    def test_get_epoch_invalid_partition_negative(self):
        """
        Test that the get_epoch endpoint returns INVALID_PARTITION error
        when the requested partition number is negative.
        """
        admin = Admin(self.redpanda)

        # Create a test topic
        topic_name = "test_l0_gc_get_epoch_negative_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with negative partition number
        request = l0_gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = -1  # Invalid negative partition

        request.partitions.append(tp)

        self.logger.info("Sending get_epoch request for negative partition number")

        # Call the endpoint
        response = l0_gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified INVALID_PARTITION error for negative partition"
        )

    @cluster(num_nodes=3)
    def test_get_epoch_not_cloud_topic(self):
        """
        Test that the get_epoch endpoint returns NOT_CLOUD_TOPIC error
        when the requested topic is not a cloud topic.
        """
        admin = Admin(self.redpanda)

        # Create a regular (non-cloud) topic
        topic_name = "test_l0_gc_get_epoch_not_cloud_topic"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request for the non-cloud topic
        request = l0_gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = 0

        request.partitions.append(tp)

        self.logger.info(f"Sending get_epoch request for non-cloud topic: {topic_name}")

        # Call the endpoint
        response = l0_gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == l0_gc_pb.L0_GC_ERROR_NOT_CLOUD_TOPIC, (
            f"Expected NOT_CLOUD_TOPIC error, got {response.partitions[0].error}"
        )

        self.logger.info(
            "Successfully verified NOT_CLOUD_TOPIC error for non-cloud topic"
        )

    @cluster(num_nodes=3)
    def test_get_epoch_mixed_errors(self):
        """
        Test that the get_epoch endpoint correctly handles requests with
        multiple partitions that have different error conditions.
        """
        admin = Admin(self.redpanda)

        # Create a cloud topic
        topic_name = "test_l0_gc_get_epoch_mixed_errors"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        # Create a non-cloud topic
        non_cloud_topic = "test_l0_gc_get_epoch_mixed_errors_non_cloud"
        self.rpk.create_topic(
            topic=non_cloud_topic,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        l0_gc_client = admin.l0_gc()

        # Build request with multiple partitions having different error conditions
        request = l0_gc_pb.GetEpochRequest()

        # 1. Valid cloud topic, valid partition (should return epoch)
        tp1 = ntp_pb.TopicPartition()
        tp1.topic = topic_name
        tp1.partition = 0
        request.partitions.append(tp1)

        # 2. Non-existent topic
        tp2 = ntp_pb.TopicPartition()
        tp2.topic = "non_existent_topic_get_epoch"
        tp2.partition = 0
        request.partitions.append(tp2)

        # 3. Valid topic, invalid partition (too high)
        tp3 = ntp_pb.TopicPartition()
        tp3.topic = topic_name
        tp3.partition = partition_count + 1
        request.partitions.append(tp3)

        # 4. Valid topic, invalid partition (negative)
        tp4 = ntp_pb.TopicPartition()
        tp4.topic = topic_name
        tp4.partition = -5
        request.partitions.append(tp4)

        # 5. Non-cloud topic
        tp5 = ntp_pb.TopicPartition()
        tp5.topic = non_cloud_topic
        tp5.partition = 0
        request.partitions.append(tp5)

        self.logger.info("Sending get_epoch request with mixed error conditions")

        # Call the endpoint
        response = l0_gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 5, (
            f"Expected 5 results, got {len(response.partitions)}"
        )

        # Verify each result
        # Result 1: Valid cloud topic and partition (should return epoch)
        assert response.partitions[0].epoch == 0, (
            f"Expected epoch for valid topic/partition, got {response.partitions[0].epoch}"
        )

        # Result 2: Non-existent topic
        assert response.partitions[1].error == l0_gc_pb.L0_GC_ERROR_TOPIC_NOT_FOUND, (
            f"Expected TOPIC_NOT_FOUND for non-existent topic, got {response.partitions[1].error}"
        )

        # Result 3: Invalid partition (too high)
        assert response.partitions[2].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for high partition, got {response.partitions[2].error}"
        )

        # Result 4: Invalid partition (negative)
        assert response.partitions[3].error == l0_gc_pb.L0_GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for negative partition, got {response.partitions[3].error}"
        )

        # Result 5: Non-cloud topic
        assert response.partitions[4].error == l0_gc_pb.L0_GC_ERROR_NOT_CLOUD_TOPIC, (
            f"Expected NOT_CLOUD_TOPIC for non-cloud topic, got {response.partitions[4].error}"
        )

        self.logger.info(
            "Successfully verified mixed error conditions are handled correctly for get_epoch"
        )
