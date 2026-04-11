# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import json
from datetime import datetime
from typing import Any

from ducktape.tests.test import TestContext

from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.admin.v2 import cluster_pb, kafka_connections_pb
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SecurityConfig
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until


class AdminV2ListKafkaConnectionsTest(RedpandaTest):
    """
    Tests the AdminV2 ListKafkaConnections endpoint by verifying active Kafka connections are correctly reported.
    """

    test_topic: str = "test-list-connections"
    test_group: str = "test-cg-group"

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        security = SecurityConfig()
        security.enable_sasl = True

        super().__init__(test_ctx, *args, security=security, **kwargs)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = Admin(
            self.redpanda, auth=(self.superuser.username, self.superuser.password)
        )
        self.consumer = RpkConsumer(
            test_ctx,
            self.redpanda,
            self.test_topic,
            group=self.test_group,
            username=self.superuser.username,
            password=self.superuser.password,
            mechanism=self.superuser.mechanism,
        )
        self.super_rpk = RpkTool(
            self.redpanda,
            username=self.superuser.username,
            password=self.superuser.password,
            sasl_mechanism=self.superuser.algorithm,
        )

    def setUp(self):
        super().setUp()

        self.super_rpk.create_topic(self.test_topic)

    @cluster(num_nodes=4)
    def test_list_kafka_connections(self):
        """
        Tests the AdminV2 list_connections endpoint by verifying active Kafka connections are correctly reported
        """

        self.logger.debug("Start a consumer to open some kafka connections")
        self.consumer.start()

        admin_v2 = AdminV2(
            self.redpanda,
            auth=(self.superuser.username, self.superuser.password),
        )
        int32_max = 2_147_483_647
        req = cluster_pb.ListKafkaConnectionsRequest(
            page_size=int32_max, order_by="source.port desc"
        )

        def valid_response() -> bool:
            resp = admin_v2.cluster().list_kafka_connections(req)

            self.logger.info(
                f"ListKafkaConnectionsResponse: total_size={resp.total_size}, connections={len(resp.connections)}"
            )
            self.logger.debug(f"ListKafkaConnectionsResponse: {resp}")

            # Sanity check the response
            assert len(resp.connections) >= 2

            # Check the ordering is correct
            assert resp.connections[0].source.port >= resp.connections[1].source.port

            # Find the connection used for franz-go consumer group requests
            # Note: this is different from the connection used for fetch requests
            matching_conns = [
                conn
                for conn in resp.connections
                if conn.group_id == self.test_group
                and conn.state == kafka_connections_pb.KAFKA_CONNECTION_STATE_OPEN
                and conn.open_time.ToDatetime() > datetime(year=2025, month=1, day=1)
                and len(conn.source.ip_address) > 0
                and conn.source.port != 0
                and conn.authentication_info.state
                == kafka_connections_pb.AUTHENTICATION_STATE_SUCCESS
                and conn.authentication_info.mechanism
                == kafka_connections_pb.AUTHENTICATION_MECHANISM_SASL_SCRAM
                and conn.authentication_info.user_principal == self.superuser.username
                and not conn.tls_info.enabled
                and conn.client_id == "rpk"
                and conn.client_software_name == "kgo"
                and len(conn.client_software_version) > 0
                and len(conn.group_member_id) > 0
                and len(conn.api_versions) > 0
                and conn.total_request_statistics.request_count > 0
            ]

            assert matching_conns, (
                f"No connection in response matched expected criteria for group_id={self.test_group}"
            )

            return True

        wait_until(
            valid_response,
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="Did not observe a valid ListKafkaConnectionsResponse",
        )

        self.logger.info(
            "Test the filtering integration by filtering for an unknown UUID, expect an empty response"
        )
        filtered_resp = admin_v2.cluster().list_kafka_connections(
            cluster_pb.ListKafkaConnectionsRequest(
                filter='uid = "ba26cadd-90f6-4999-b2c9-a89b5f033507"',
            )
        )
        self.logger.debug(f"Filtered response: {filtered_resp}")
        assert len(filtered_resp.connections) == 0
        assert filtered_resp.total_size == 0

        self.consumer.stop()

        self.logger.info(
            "Test that closed connections can also be included in the response"
        )
        closed_conns_resp = admin_v2.cluster().list_kafka_connections(
            cluster_pb.ListKafkaConnectionsRequest(
                filter="state = KAFKA_CONNECTION_STATE_CLOSED",
            )
        )
        self.logger.debug(f"Closed connections response: {closed_conns_resp}")
        assert len(closed_conns_resp.connections) > 0
        conn = closed_conns_resp.connections[0]
        assert conn.state == kafka_connections_pb.KAFKA_CONNECTION_STATE_CLOSED
        assert conn.close_time.ToDatetime() > datetime(year=2025, month=1, day=1)

    @cluster(num_nodes=4)
    def test_list_kafka_connections_page_size(self):
        self.logger.debug("Start a consumer to open some kafka connections")
        self.consumer.start()

        admin_v2 = AdminV2(
            self.redpanda,
            auth=(self.superuser.username, self.superuser.password),
        )

        for order_by in ["", "source.port asc"]:
            self.logger.info(f"Testing with order_by='{order_by}'")

            def validate():
                self.logger.debug(
                    f"Sending request with page_size=3 and order_by='{order_by}'"
                )
                first_req = cluster_pb.ListKafkaConnectionsRequest(
                    page_size=3, order_by=order_by
                )

                first_resp = admin_v2.cluster().list_kafka_connections(first_req)
                self.logger.debug(f"First response: {first_resp}")
                assert len(first_resp.connections) == 3

                self.logger.debug(
                    f"Sending request with page_size=1000 and order_by='{order_by}'"
                )
                second_req = cluster_pb.ListKafkaConnectionsRequest(
                    page_size=1000, order_by=order_by
                )
                second_resp = admin_v2.cluster().list_kafka_connections(second_req)
                self.logger.debug(f"Second response: {second_resp}")
                assert len(second_resp.connections) > 3

                def is_ascending_order(
                    resp: cluster_pb.ListKafkaConnectionsResponse,
                ) -> bool:
                    for i in range(len(resp.connections) - 1):
                        if (
                            resp.connections[i].source.port
                            > resp.connections[i + 1].source.port
                        ):
                            return False
                    return True

                # Validate consistency between limited and full responses
                assert first_resp.total_size == second_resp.total_size

                if order_by:
                    assert is_ascending_order(first_resp)
                    assert is_ascending_order(second_resp)

                    for i in range(len(first_resp.connections)):
                        assert (
                            first_resp.connections[i].source.port
                            == second_resp.connections[i].source.port
                        )

                return True

            # Retry to avoid flakiness due to connection changes between the two requests
            wait_until(
                validate,
                timeout_sec=30,
                retry_on_exc=True,
                err_msg="Pagination validation failed",
            )

        self.consumer.stop()

    @cluster(num_nodes=4)
    def test_list_kafka_connections_rpk(self):
        """
        Tests that rpk cluster connections list works correctly.

        This specifically exercises the code path where rpk calls ListBrokers
        to check version compatibility before listing connections.
        """

        self.logger.debug("Start a consumer to open some kafka connections")
        self.consumer.start()

        def valid_response() -> bool:
            raw_resp = self.super_rpk.cluster_connections_list(
                limit=100,
                filter_raw="state = KAFKA_CONNECTION_STATE_OPEN",
            )
            resp = json.loads(raw_resp)
            conns = resp.get("connections", [])

            self.logger.info(
                f"rpk cluster connections list returned {len(conns)} connections"
            )

            return len(conns) >= 2

        wait_until(
            valid_response,
            timeout_sec=30,
            retry_on_exc=True,
            err_msg="rpk cluster connections list failed",
        )

        self.consumer.stop()


class AdminV2ListKafkaConnectionsLicenseTest(RedpandaTest):
    """
    Tests that list_kafka_connections requires a valid license.
    """

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_ctx, *args, **kwargs)

    def setUp(self):
        self.redpanda.set_environment(
            {"__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE": "true"}
        )
        super().setUp()

    @cluster(num_nodes=1)
    def test_without_license(self):
        admin = AdminV2(self.redpanda)
        resp = admin.cluster().call_list_kafka_connections(
            cluster_pb.ListKafkaConnectionsRequest()
        )
        err = resp.error()
        assert err is not None, f"expected error response without license, got {err}"
        assert "license" in err.message, (
            f"expected license in error message, got {err.message}"
        )
