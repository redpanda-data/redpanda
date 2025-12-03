# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.admin.v2 import Admin, gc_pb, ntp_pb
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (
    SISettings,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.tests.redpanda_test import RedpandaTest


class GcAdminTest(RedpandaTest):
    """
    Test class for the GC admin API endpoints.

    This test verifies that the new GC admin API endpoints are available
    and return the expected stub responses (failed status).
    """

    def __init__(self, test_context):
        # Configure cloud topics settings

        super(GcAdminTest, self).__init__(
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
        topic_name = "test_gc_topic"
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
        gc_client = admin.gc()

        # Build request with multiple partitions
        request = gc_pb.AdvanceEpochRequest()

        for partition_id in range(partition_count):
            # Create TopicPartitionEpoch for each partition
            tp_epoch = gc_pb.TopicPartitionEpoch()

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
        response = gc_client.advance_epoch(request)

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

            assert result.error == gc_pb.GC_ERROR_FAILED, (
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
        gc_client = admin.gc()

        # Send empty request
        request = gc_pb.AdvanceEpochRequest()

        self.logger.info("Sending empty advance_epoch request")
        response = gc_client.advance_epoch(request)

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
        topic_name = "test_gc_single_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=1,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        gc_client = admin.gc()

        # Build request with single partition
        request = gc_pb.AdvanceEpochRequest()
        tp_epoch = gc_pb.TopicPartitionEpoch()

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
        response = gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_FAILED

        self.logger.info("Successfully verified single partition request")

    @cluster(num_nodes=3)
    def test_advance_epoch_topic_not_found(self):
        """
        Test that the advance_epoch endpoint returns TOPIC_NOT_FOUND error
        when the requested topic does not exist.
        """
        admin = Admin(self.redpanda)
        gc_client = admin.gc()

        # Use a topic name that doesn't exist
        non_existent_topic = "this_topic_does_not_exist"

        # Build request for non-existent topic
        request = gc_pb.AdvanceEpochRequest()
        tp_epoch = gc_pb.TopicPartitionEpoch()

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
        response = gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_TOPIC_NOT_FOUND, (
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
        topic_name = "test_gc_invalid_partition"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        gc_client = admin.gc()

        # Build request with partition number that's too high
        # Valid partitions are 0, 1, 2, so partition 3 should be invalid
        request = gc_pb.AdvanceEpochRequest()
        tp_epoch = gc_pb.TopicPartitionEpoch()

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
        response = gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
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
        topic_name = "test_gc_negative_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        gc_client = admin.gc()

        # Build request with negative partition number
        request = gc_pb.AdvanceEpochRequest()
        tp_epoch = gc_pb.TopicPartitionEpoch()

        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = -1  # Invalid negative partition
        tp_epoch.partition.CopyFrom(tp)
        tp_epoch.epoch = 0

        request.partitions.append(tp_epoch)

        self.logger.info("Sending advance_epoch request for negative partition number")

        # Call the endpoint
        response = gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
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
        topic_name = "test_gc_not_cloud_topic"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        gc_client = admin.gc()

        # Build request for the non-cloud topic
        request = gc_pb.AdvanceEpochRequest()
        tp_epoch = gc_pb.TopicPartitionEpoch()

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
        response = gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_NOT_CLOUD_TOPIC, (
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
        topic_name = "test_gc_mixed_errors"
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
        non_cloud_topic = "test_gc_mixed_errors_non_cloud"
        self.rpk.create_topic(
            topic=non_cloud_topic,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        gc_client = admin.gc()

        # Build request with multiple partitions having different error conditions
        request = gc_pb.AdvanceEpochRequest()

        # 1. Valid cloud topic, valid partition (should return FAILED since stub)
        tp_epoch1 = gc_pb.TopicPartitionEpoch()
        tp1 = ntp_pb.TopicPartition()
        tp1.topic = topic_name
        tp1.partition = 0
        tp_epoch1.partition.CopyFrom(tp1)
        tp_epoch1.epoch = 0
        request.partitions.append(tp_epoch1)

        # 2. Non-existent topic
        tp_epoch2 = gc_pb.TopicPartitionEpoch()
        tp2 = ntp_pb.TopicPartition()
        tp2.topic = "non_existent_topic"
        tp2.partition = 0
        tp_epoch2.partition.CopyFrom(tp2)
        tp_epoch2.epoch = 0
        request.partitions.append(tp_epoch2)

        # 3. Valid topic, invalid partition (too high)
        tp_epoch3 = gc_pb.TopicPartitionEpoch()
        tp3 = ntp_pb.TopicPartition()
        tp3.topic = topic_name
        tp3.partition = partition_count + 1
        tp_epoch3.partition.CopyFrom(tp3)
        tp_epoch3.epoch = 0
        request.partitions.append(tp_epoch3)

        # 4. Valid topic, invalid partition (negative)
        tp_epoch4 = gc_pb.TopicPartitionEpoch()
        tp4 = ntp_pb.TopicPartition()
        tp4.topic = topic_name
        tp4.partition = -5
        tp_epoch4.partition.CopyFrom(tp4)
        tp_epoch4.epoch = 0
        request.partitions.append(tp_epoch4)

        # 5. Non-cloud topic
        tp_epoch5 = gc_pb.TopicPartitionEpoch()
        tp5 = ntp_pb.TopicPartition()
        tp5.topic = non_cloud_topic
        tp5.partition = 0
        tp_epoch5.partition.CopyFrom(tp5)
        tp_epoch5.epoch = 0
        request.partitions.append(tp_epoch5)

        self.logger.info("Sending advance_epoch request with mixed error conditions")

        # Call the endpoint
        response = gc_client.advance_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 5, (
            f"Expected 5 results, got {len(response.partitions)}"
        )

        # Verify each result
        # Result 1: Valid cloud topic and partition (should return FAILED in stub)
        assert response.partitions[0].error == gc_pb.GC_ERROR_FAILED, (
            f"Expected FAILED for valid topic/partition, got {response.partitions[0].error}"
        )

        # Result 2: Non-existent topic
        assert response.partitions[1].error == gc_pb.GC_ERROR_TOPIC_NOT_FOUND, (
            f"Expected TOPIC_NOT_FOUND for non-existent topic, got {response.partitions[1].error}"
        )

        # Result 3: Invalid partition (too high)
        assert response.partitions[2].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for high partition, got {response.partitions[2].error}"
        )

        # Result 4: Invalid partition (negative)
        assert response.partitions[3].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for negative partition, got {response.partitions[3].error}"
        )

        # Result 5: Non-cloud topic
        assert response.partitions[4].error == gc_pb.GC_ERROR_NOT_CLOUD_TOPIC, (
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
        topic_name = "test_gc_get_epoch_topic"
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
        gc_client = admin.gc()

        # Build request with multiple partitions
        request = gc_pb.GetEpochRequest()

        for partition_id in range(partition_count):
            # Create TopicPartition for each partition
            tp = ntp_pb.TopicPartition()
            tp.topic = topic_name
            tp.partition = partition_id

            request.partitions.append(tp)

        self.logger.info(f"Sending get_epoch request for {partition_count} partitions")

        # Call the get_epoch endpoint
        response = gc_client.get_epoch(request)

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
        gc_client = admin.gc()

        # Send empty request
        request = gc_pb.GetEpochRequest()

        self.logger.info("Sending empty get_epoch request")
        response = gc_client.get_epoch(request)

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
        topic_name = "test_gc_get_epoch_single_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=1,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        gc_client = admin.gc()

        # Build request with single partition
        request = gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = 0

        request.partitions.append(tp)

        self.logger.info("Sending get_epoch request for single partition")

        # Call the endpoint
        response = gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_FAILED

        self.logger.info("Successfully verified single partition request")

    @cluster(num_nodes=3)
    def test_get_epoch_topic_not_found(self):
        """
        Test that the get_epoch endpoint returns TOPIC_NOT_FOUND error
        when the requested topic does not exist.
        """
        admin = Admin(self.redpanda)
        gc_client = admin.gc()

        # Use a topic name that doesn't exist
        non_existent_topic = "this_topic_does_not_exist_get_epoch"

        # Build request for non-existent topic
        request = gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = non_existent_topic
        tp.partition = 0

        request.partitions.append(tp)

        self.logger.info(
            f"Sending get_epoch request for non-existent topic: {non_existent_topic}"
        )

        # Call the endpoint
        response = gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_TOPIC_NOT_FOUND, (
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
        topic_name = "test_gc_get_epoch_invalid_partition"
        partition_count = 3
        self.rpk.create_topic(
            topic=topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        gc_client = admin.gc()

        # Build request with partition number that's too high
        # Valid partitions are 0, 1, 2, so partition 3 should be invalid
        request = gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = partition_count  # This should be invalid

        request.partitions.append(tp)

        self.logger.info(
            f"Sending get_epoch request for partition {partition_count} "
            f"(topic has only {partition_count} partitions: 0-{partition_count - 1})"
        )

        # Call the endpoint
        response = gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
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
        topic_name = "test_gc_get_epoch_negative_partition"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
            },
        )

        gc_client = admin.gc()

        # Build request with negative partition number
        request = gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = -1  # Invalid negative partition

        request.partitions.append(tp)

        self.logger.info("Sending get_epoch request for negative partition number")

        # Call the endpoint
        response = gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
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
        topic_name = "test_gc_get_epoch_not_cloud_topic"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        gc_client = admin.gc()

        # Build request for the non-cloud topic
        request = gc_pb.GetEpochRequest()
        tp = ntp_pb.TopicPartition()
        tp.topic = topic_name
        tp.partition = 0

        request.partitions.append(tp)

        self.logger.info(f"Sending get_epoch request for non-cloud topic: {topic_name}")

        # Call the endpoint
        response = gc_client.get_epoch(request)

        # Verify response
        assert response is not None
        assert len(response.partitions) == 1
        assert response.partitions[0].error == gc_pb.GC_ERROR_NOT_CLOUD_TOPIC, (
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
        topic_name = "test_gc_get_epoch_mixed_errors"
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
        non_cloud_topic = "test_gc_get_epoch_mixed_errors_non_cloud"
        self.rpk.create_topic(
            topic=non_cloud_topic,
            partitions=3,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "false",
            },
        )

        gc_client = admin.gc()

        # Build request with multiple partitions having different error conditions
        request = gc_pb.GetEpochRequest()

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
        response = gc_client.get_epoch(request)

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
        assert response.partitions[1].error == gc_pb.GC_ERROR_TOPIC_NOT_FOUND, (
            f"Expected TOPIC_NOT_FOUND for non-existent topic, got {response.partitions[1].error}"
        )

        # Result 3: Invalid partition (too high)
        assert response.partitions[2].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for high partition, got {response.partitions[2].error}"
        )

        # Result 4: Invalid partition (negative)
        assert response.partitions[3].error == gc_pb.GC_ERROR_INVALID_PARTITION, (
            f"Expected INVALID_PARTITION for negative partition, got {response.partitions[3].error}"
        )

        # Result 5: Non-cloud topic
        assert response.partitions[4].error == gc_pb.GC_ERROR_NOT_CLOUD_TOPIC, (
            f"Expected NOT_CLOUD_TOPIC for non-cloud topic, got {response.partitions[4].error}"
        )

        self.logger.info(
            "Successfully verified mixed error conditions are handled correctly for get_epoch"
        )
