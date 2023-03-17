# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.admin import Admin
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.util import wait_until_result
from rptest.util import wait_until
import confluent_kafka as ck
import requests
import time

# Errors we should tolerate right after start Redpanda
NOT_LEADER_LOG_ERRORS = [
    # e.g.  admin_api_server - admin_server.cc:409 - [_anonymous] exception intercepted - url: [http://docker-rp-6:9644/v1/debug/raft/kafka/topic-sppnwqzxzz/0] http_return_status[500] reason - seastar::httpd::server_error_exception (admin_server.cc:3347 - Can not find leader for ntp {kafka/topic-sppnwqzxzz/0})">
    "admin_api_server - .*http_return_status[500]"
]


class AdminApiRaftStateTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        super(AdminApiRaftStateTest, self).__init__(test_context=test_context,
                                                    num_brokers=3)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3, log_allow_list=NOT_LEADER_LOG_ERRORS)
    def simple_test(self):
        self.topic_name = self.topics[0].name

        def leader_is_elected():
            try:
                info = self.admin.get_raft_state("kafka", self.topic_name, 0)
                return True, info
            except requests.exceptions.HTTPError as e:
                # Leader is not elected yet. We should retry
                assert e.response.status_code == 500
                return False

        raft_info = wait_until_result(
            leader_is_elected,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Can not elect leader for topic: {self.topic_name}")

        leader_id = self.admin.get_partition_leader(namespace="kafka",
                                                    topic=self.topic_name,
                                                    partition=0)

        assert raft_info["leader_info"]["id"] == leader_id
        assert raft_info["leader_info"]["commit_index"] == 0
        assert raft_info["leader_info"]["majority_replicated_index"] == 0

        assert len(raft_info["followers"]) == 2

        for follower in raft_info["followers"]:
            assert follower["id"] != leader_id

    @cluster(num_nodes=3, log_allow_list=NOT_LEADER_LOG_ERRORS)
    def controller_test(self):
        self.topic_name = "controller"

        def leader_is_elected():
            try:
                info = self.admin.get_raft_state("redpanda", self.topic_name,
                                                 0)
                return True, info
            except requests.exceptions.HTTPError as e:
                # Leader is not elected yet. We should retry
                assert e.response.status_code == 500
                return False

        raft_info = wait_until_result(
            leader_is_elected,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Can not elect leader for topic: {self.topic_name}")

        leader_id = self.admin.get_partition_leader(namespace="redpanda",
                                                    topic=self.topic_name,
                                                    partition=0)

        assert raft_info["leader_info"]["id"] == leader_id

        for follower in raft_info["followers"]:
            assert follower["id"] != leader_id

        def wait_all_followers_sync():
            info = self.admin.get_raft_state("redpanda", self.topic_name, 0)
            leader_ci = info["leader_info"]["commit_index"]
            for follower in info["followers"]:
                if leader_ci != follower["last_flushed_log_index"]:
                    return False
            return True

        wait_until(
            wait_all_followers_sync,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Followers can not sync with leader for controller")

    @cluster(num_nodes=3)
    def local_request_test(self):
        self.topic_name = self.topics[0].name

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
        })

        for i in range(10):
            producer.produce(self.topic_name, str(i), str(i), 0)
        producer.flush()

        def wait_all_followers_sync():
            committed_offset = None
            for node in self.redpanda.nodes:
                info = self.admin.get_local_partition_state("kafka", self.topic_name, 0, node)
                if not committed_offset:
                    committed_offset = info["committed_offset"]
                
                if committed_offset != info["committed_offset"] or committed_offset == 0:
                    return False
            return True

        wait_until(
            wait_all_followers_sync,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Followers can not sync with leader for topic: {self.topic_name}")

        info = None
        for node in self.redpanda.nodes:
            if not info:
                info = self.admin.get_local_partition_state("kafka", self.topic_name, 0, node)
            assert info == self.admin.get_local_partition_state("kafka", self.topic_name, 0, node)
        
        assert info["is_read_replica_mode_enabled"] == False
        assert info["is_remote_fetch_enabled"] == False
        assert info["is_cloud_data_available"] == False