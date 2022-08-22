# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import time

from ducktape.utils.util import wait_until

from rptest.clients.kcl import RawKCL, KclCreateTopicsRequestTopic, \
    KclCreatePartitionsRequestTopic
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster


class ClusterQuotaTest(RedpandaTest):
    """
    Ducktape tests for all things quota related
    """
    def __init__(self, *args, **kwargs):
        additional_options = {'kafka_admin_topic_api_rate': 1}
        super().__init__(*args,
                         num_brokers=3,
                         extra_rp_conf=additional_options,
                         **kwargs)
        # Use kcl so the throttle_time_ms value in the response can be examined
        self.kcl = RawKCL(self.redpanda)

    @cluster(num_nodes=3)
    def test_partition_throttle_mechanism(self):
        """
        Ensure the partition throttling mechanism (KIP-599) works
        """

        # The kafka_admin_topic_api_rate is 1, quota will be 10. This test will
        # make 1 request within containing three topics to create, each containing
        # different number of partitions.
        #
        # The first topic should succeed, the second will exceed the quota but
        # succeed since the throttling algorithms burst settings will allow it
        # to. The third however should fail since the quota has already exceeded.
        exceed_quota_req = [
            KclCreateTopicsRequestTopic('baz', 1, 1),
            KclCreateTopicsRequestTopic('foo', 10, 1),
            KclCreateTopicsRequestTopic('bar', 2, 1)
        ]

        # Use KCL so that the details about the response can be examined, namely
        # this test must observe that the newly introduce 'throttle_quota_exceeded'
        # response code is used and that 'ThrottleMillis' was approprately set
        response = self.kcl.raw_create_topics(6, exceed_quota_req)
        response = json.loads(response)
        assert response['Version'] == 6
        foo_response = [t for t in response['Topics'] if t['Topic'] == 'foo']
        bar_response = [t for t in response['Topics'] if t['Topic'] == 'bar']
        baz_response = [t for t in response['Topics'] if t['Topic'] == 'baz']
        assert foo_response[0]['ErrorCode'] == 0  # success
        assert baz_response[0]['ErrorCode'] == 0  # success
        assert bar_response[0]['ErrorCode'] == 89  # throttling_quota_exceeded

        # Respect throttle millis response - exhaust the timeout so quota resets
        throttle_ms = response['ThrottleMillis']
        self.redpanda.logger.info(f"First throttle_ms: {throttle_ms}")
        assert throttle_ms > 0
        time.sleep((throttle_ms * 0.001) + 1)

        # Test that throttling works via the CreatePartitions API
        # The first request should exceed the quota, preventing the second from
        # occurring
        exceed_quota_req = [
            KclCreatePartitionsRequestTopic('foo', 21, 1),
            KclCreatePartitionsRequestTopic('baz', 2, 1)
        ]
        response = self.kcl.raw_create_partitions(3, exceed_quota_req)
        response = json.loads(response)
        foo_response = [t for t in response['Topics'] if t['Topic'] == 'foo']
        baz_response = [t for t in response['Topics'] if t['Topic'] == 'baz']
        assert foo_response[0]['ErrorCode'] == 0  # success
        assert baz_response[0]['ErrorCode'] == 89  # throttling_quota_exceeded

        # Respect throttle millis response
        throttle_ms = response['ThrottleMillis']
        self.redpanda.logger.info(f"Second throttle_ms: {throttle_ms}")
        assert throttle_ms > 0
        time.sleep((throttle_ms * 0.001) + 1)

        # Test that throttling works via the DeleteTopics API, 'foo' should at
        # this point contain 21 partitions, deleting this will exceed the quota,
        # any subsequent requests should fail
        exceed_quota_req = ['foo', 'baz']
        response = self.kcl.raw_delete_topics(5, exceed_quota_req)
        response = json.loads(response)
        foo_response = [t for t in response['Topics'] if t['Topic'] == 'foo']
        baz_response = [t for t in response['Topics'] if t['Topic'] == 'baz']
        assert foo_response[0]['ErrorCode'] == 0  # success
        assert baz_response[0]['ErrorCode'] == 89  # throttling_quota_exceeded
        assert response['ThrottleMillis'] > 0
