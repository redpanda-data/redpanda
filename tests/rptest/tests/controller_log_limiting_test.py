# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import json
from subprocess import CalledProcessError

from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.mirror_maker_test import MirrorMakerService
from rptest.tests.partition_balancer_test import PartitionBalancerService, CONSUMER_TIMEOUT

from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.kcl import RawKCL, KclCreateTopicsRequestTopic
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.admin import Admin
from rptest.services.redpanda import MetricsEndpoint, CHAOS_LOG_ALLOW_LIST
from rptest.services.mirror_maker2 import MirrorMaker2
from rptest.services.cluster import cluster
from requests.exceptions import HTTPError
from ducktape.utils.util import wait_until

from rptest.utils.mode_checks import skip_debug_mode

OPERATIONS_LIMIT = 3
TOO_MANY_REQUESTS_ERROR_CODE = 89
TOO_MANY_REQUESTS_HTTP_ERROR_CODE = 429


def get_metric(redpanda, metric_type, label):
    admin = redpanda._admin
    controller_node = redpanda.get_node(
        admin.await_stable_leader(
            topic="controller",
            partition=0,
            namespace="redpanda",
        ))
    metrics = redpanda.metrics_sample(
        metric_type,
        nodes=[controller_node],
        metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS).label_filter(
            {"redpanda_cmd_group": label})
    assert len(metrics.samples) == 1
    return metrics.samples[0].value


def check_metric(redpanda, metric_type, label, value):
    metric_value = get_metric(redpanda, metric_type, label)
    assert metric_value == value


class TopicOperationsLimitingTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        additional_options = {
            "enable_controller_log_rate_limiting": True,
            "rps_limit_topic_operations": OPERATIONS_LIMIT,
        }
        super().__init__(*args,
                         num_brokers=3,
                         extra_rp_conf=additional_options,
                         **kwargs)
        self.kcl = RawKCL(self.redpanda)

    def check_available_metric(self, value):
        check_metric(self.redpanda, "requests_available_rps",
                     "topic_operations", value)

    def check_dropped_metric(self, value):
        check_metric(self.redpanda, "requests_dropped", "topic_operations",
                     value)

    def chek_capacity_is_full(self, value):
        return get_metric(self.redpanda, "requests_available_rps",
                          "topic_operations") == value

    @cluster(num_nodes=3)
    def test_create_partition_limit(self):
        requsts_amount_1 = OPERATIONS_LIMIT * 2
        self.check_available_metric(OPERATIONS_LIMIT)
        exceed_quota_req = []
        for i in range(requsts_amount_1):
            exceed_quota_req.append(KclCreateTopicsRequestTopic(str(i), 1, 1))
        wait_until(lambda: self.chek_capacity_is_full(OPERATIONS_LIMIT),
                   timeout_sec=10,
                   backoff_sec=1)
        response = self.kcl.raw_create_topics(6, exceed_quota_req)
        response = json.loads(response)
        assert response['Version'] == 6
        success_amount = 0
        errors_amount = 0
        for topic_response in response['Topics']:
            if topic_response['ErrorCode'] == 0:
                success_amount += 1
            if topic_response['ErrorCode'] == TOO_MANY_REQUESTS_ERROR_CODE:
                errors_amount += 1
        assert success_amount + errors_amount == requsts_amount_1
        assert success_amount == OPERATIONS_LIMIT
        assert errors_amount == requsts_amount_1 - OPERATIONS_LIMIT

        self.check_dropped_metric(OPERATIONS_LIMIT)

        wait_until(lambda: self.chek_capacity_is_full(OPERATIONS_LIMIT),
                   timeout_sec=10,
                   backoff_sec=1)

        self.check_available_metric(OPERATIONS_LIMIT)

        exceed_quota_req = []
        requests_amount_2 = OPERATIONS_LIMIT * 3
        for i in range(OPERATIONS_LIMIT * 2, OPERATIONS_LIMIT * 5):
            exceed_quota_req.append(KclCreateTopicsRequestTopic(str(i), 1, 1))

        response = self.kcl.raw_create_topics(6, exceed_quota_req)
        response = json.loads(response)
        assert response['Version'] == 6
        success_amount = 0
        errors_amount = 0
        for topic_response in response['Topics']:
            if topic_response['ErrorCode'] == 0:
                success_amount += 1
            if topic_response['ErrorCode'] == TOO_MANY_REQUESTS_ERROR_CODE:
                errors_amount += 1
        assert success_amount + errors_amount == requests_amount_2
        assert success_amount == OPERATIONS_LIMIT
        assert errors_amount == requests_amount_2 - OPERATIONS_LIMIT

        self.check_dropped_metric(OPERATIONS_LIMIT * 3)

    @cluster(num_nodes=3)
    def test_create_partition_limit_accumulation(self):
        self.client().alter_broker_config(
            {
                "controller_log_accummulation_rps_capacity_topic_operations":
                OPERATIONS_LIMIT * 2
            },
            incremental=True)
        wait_until(lambda: self.chek_capacity_is_full(OPERATIONS_LIMIT * 2),
                   timeout_sec=10,
                   backoff_sec=1)

        exceed_quota_req = []
        for i in range(OPERATIONS_LIMIT * 2):
            exceed_quota_req.append(KclCreateTopicsRequestTopic(str(i), 1, 1))

        response = self.kcl.raw_create_topics(6, exceed_quota_req)
        response = json.loads(response)
        assert response['Version'] == 6
        for topic_response in response['Topics']:
            assert topic_response['ErrorCode'] == 0


class ControllerConfigLimitTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         extra_rp_conf={
                             "rps_limit_configuration_operations":
                             OPERATIONS_LIMIT,
                             "enable_controller_log_rate_limiting": True
                         },
                         **kwargs)

    @cluster(num_nodes=3)
    def test_alter_configs_limit(self):
        requests_amount = OPERATIONS_LIMIT * 2
        success_amount = 0
        quota_error_amount = 0
        wait_until(lambda: self.check_capcity_is_full(OPERATIONS_LIMIT),
                   timeout_sec=10,
                   backoff_sec=1)
        for i in range(requests_amount):
            try:
                self.client().alter_broker_config(
                    {
                        "controller_log_accummulation_rps_capacity_topic_operations":
                        i
                    },
                    incremental=True)
            except RuntimeError as e:
                if "THROTTLING_QUOTA_EXCEEDED" in str(e):
                    quota_error_amount += 1
                else:
                    # unexpected error type
                    raise
            else:
                success_amount += 1
            time.sleep(0.1)
        assert quota_error_amount > 0
        assert success_amount > 0

    def check_capcity_is_full(self, capacity):
        return get_metric(self.redpanda, "requests_available_rps",
                          "configuration_operations") == capacity

    @cluster(num_nodes=3)
    def test_alter_configs_limit_accumulate(self):
        requests_amount = OPERATIONS_LIMIT * 2
        self.client().alter_broker_config(
            {
                "controller_log_accummulation_rps_capacity_configuration_operations":
                requests_amount
            },
            incremental=True)
        success_amount = 0
        quota_error_amount = 0

        wait_until(lambda: self.check_capcity_is_full(requests_amount),
                   timeout_sec=10,
                   backoff_sec=1)
        for i in range(requests_amount):
            out = self.client().alter_broker_config(
                {
                    "controller_log_accummulation_rps_capacity_topic_operations":
                    i + 25
                },
                incremental=True)
            if "THROTTLING_QUOTA_EXCEEDED" in out:
                quota_error_amount += 1
            if "OK" in out:
                success_amount += 1
        assert quota_error_amount == 0
        assert success_amount > 0


class ControllerPartitionMovementLimitTest(PartitionMovementMixin,
                                           EndToEndTest):
    def __init__(self, ctx, *args, **kwargs):
        super(ControllerPartitionMovementLimitTest,
              self).__init__(ctx,
                             *args,
                             extra_rp_conf={
                                 "rps_limit_move_operations": 0,
                                 "enable_controller_log_rate_limiting": True,
                             },
                             **kwargs)
        self._ctx = ctx

    def perform_move(self, topic, partition):
        old_assignments, new_assignment = self._dispatch_random_partition_move(
            topic=topic.name, partition=partition)
        return self._equal_assignments(old_assignments, new_assignment)

    @cluster(num_nodes=3)
    def test_move_partition_limit(self):
        self.start_redpanda(num_nodes=3)

        topic = TopicSpec(partition_count=3, replication_factor=1)
        self.client().create_topic(topic)
        try:
            while (self.perform_move(topic, 0)):
                pass
        except HTTPError as err:
            assert err.response.status_code == TOO_MANY_REQUESTS_HTTP_ERROR_CODE
        else:
            assert 0, "Too Many Requests error must be raised"


class ControllerAclsAndUsersLimitTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         extra_rp_conf={
                             "rps_limit_acls_and_users_operations":
                             OPERATIONS_LIMIT,
                             "enable_controller_log_rate_limiting": True
                         },
                         **kwargs)

    def check_capacity_is_full(self, capacity):
        return get_metric(self.redpanda, "requests_available_rps",
                          "acls_and_users_operations") == capacity

    @cluster(num_nodes=3)
    def test_create_user_limit(self):
        rpk = RpkTool(self.redpanda)
        wait_until(lambda: self.check_capacity_is_full(OPERATIONS_LIMIT),
                   timeout_sec=10,
                   backoff_sec=1)
        for i in range(OPERATIONS_LIMIT * 2):
            try:
                rpk.sasl_create_user(
                    f"testuser_{i}", "password",
                    self.redpanda.SUPERUSER_CREDENTIALS.algorithm)
            except RpkException as err:
                if i >= OPERATIONS_LIMIT:
                    assert 'Too many requests' in err.stderr
                else:
                    raise err
            else:
                if i >= OPERATIONS_LIMIT:
                    assert 0, "Too Many Requests error must be raised"

    @cluster(num_nodes=3)
    def test_create_acl_limit(self):
        client = KafkaCliTools(self.redpanda)
        self.client().alter_broker_config(
            {"rps_limit_acls_and_users_operations": 0}, incremental=True)
        try:
            client.create_cluster_acls("User", "describe")
        except CalledProcessError as err:
            assert "ThrottlingQuotaExceededException" in err.output
        else:
            assert 0, "Too Many Requests error must be raised"


class ControllerNodeManagementLimitTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         extra_rp_conf={
                             "rps_limit_node_management_operations": 1,
                             "enable_controller_log_rate_limiting": True
                         },
                         **kwargs)

    @cluster(num_nodes=3)
    def test_maintance_mode_limit(self):
        self.admin = Admin(self.redpanda)
        admin = self.redpanda._admin
        controller_node = self.redpanda.get_node(
            admin.await_stable_leader(
                topic="controller",
                partition=0,
                namespace="redpanda",
            ))
        node = next(
            filter(lambda node: node != controller_node, self.redpanda.nodes))
        self.admin.maintenance_start(node)
        try:
            self.admin.maintenance_stop(node)
        except HTTPError as err:
            assert err.response.status_code == TOO_MANY_REQUESTS_HTTP_ERROR_CODE
        else:
            assert 0, "Too Many Requests error must be raised"


class ControllerLogLimitMirrorMakerTests(MirrorMakerService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @skip_debug_mode
    @cluster(num_nodes=10)
    def test_mirror_maker_with_limits(self):
        # start brokers
        self.start_brokers(source_type=MirrorMakerService.redpanda_source)
        target_client = DefaultClient(self.redpanda)
        target_client.alter_broker_config(
            {
                "enable_controller_log_rate_limiting": True,
                "rps_limit_topic_operations": 25,
                "rps_limit_acls_and_users_operations": 25,
                "rps_limit_node_management_operations": 25,
                "rps_limit_move_operations": 25,
                "rps_limit_configuration_operations": 25
            },
            incremental=True)

        # start mirror maker
        self.mirror_maker = MirrorMaker2(self.test_context,
                                         num_nodes=1,
                                         source_cluster=self.source_broker,
                                         target_cluster=self.redpanda)
        topics = []
        for i in range(0, 50):
            topics.append(TopicSpec(partition_count=3))
        self.source_client.create_topic(topics)
        self.mirror_maker.start()
        # start source producer & target consumer
        self.start_workload()

        self.run_validation(consumer_timeout_sec=120)
        self.mirror_maker.stop()
        for t in topics:
            desc = target_client.describe_topic(t.name)
            self.logger.debug(f'source topic: {t}, target topic: {desc}')
            assert len(desc.partitions) == t.partition_count


class ControllerLogLimitPartitionBalancerTests(PartitionBalancerService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_partition_balancer_with_limits(self):
        self.start_redpanda(num_nodes=5)
        client = DefaultClient(self.redpanda)
        client.alter_broker_config(
            {
                "enable_controller_log_rate_limiting": True,
                "rps_limit_topic_operations": 25,
                "rps_limit_acls_and_users_operations": 25,
                "rps_limit_node_management_operations": 25,
                "rps_limit_move_operations": 25,
                "rps_limit_configuration_operations": 25
            },
            incremental=True)

        self.topic = TopicSpec(partition_count=200)
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        with self.NodeStopper(self) as ns:
            node = self.redpanda.nodes[1]
            ns.make_unavailable(node)
            self.wait_until_ready(expected_unavailable_node=node,
                                  timeout_sec=240)
            self.check_no_replicas_on_node(node)
            ns.make_available()
            self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)
