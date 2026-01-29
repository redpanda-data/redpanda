# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import time
from typing import Any

from ducktape.mark import parametrize
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.consumer_swarm import ConsumerSwarm
from rptest.services.redpanda import SecurityConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until


class AdminV2ListKafkaConnectionsScaleTest(RedpandaTest):
    """
    Tests the scaling of AdminV2 ListKafkaConnections endpoint for many connections.
    """

    test_topic: str = "test-list-connections"
    test_group_base: str = "test-cg-group-"
    test_group_max: str = "test-cg-group."

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        security = SecurityConfig()
        security.enable_sasl = True

        assert test_ctx.injected_args is not None
        consumers_per_group = int(test_ctx.injected_args["consumers_per_group"])
        group_count = int(test_ctx.injected_args["group_count"])

        client_count = consumers_per_group * group_count

        super().__init__(
            test_ctx,
            *args,
            security=security,
            num_brokers=3,
            extra_rp_conf={
                "kafka_connection_rate_limit": 10000,
                "kafka_connections_max": 2 * client_count + 2048,
                "kafka_connections_max_per_ip": 2 * client_count + 2048,
            },
            **kwargs,
        )
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = Admin(
            self.redpanda, auth=(self.superuser.username, self.superuser.password)
        )

        self.consumers: list[ConsumerSwarm] = []
        for i in range(group_count):
            consumer = ConsumerSwarm(
                context=test_ctx,
                redpanda=self.redpanda,
                topic=self.test_topic,
                group=f"{self.test_group_base}{i}",
                consumers=consumers_per_group,
                records_per_consumer=1,
                properties={
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanism": self.superuser.mechanism,
                    "sasl.username": self.superuser.username,
                    "sasl.password": self.superuser.password,
                },
                unique_topics=False,
                unique_groups=True,
                topics_per_client=1,
            )
            self.consumers.append(consumer)

        self.super_rpk = RpkTool(
            self.redpanda,
            username=self.superuser.username,
            password=self.superuser.password,
            sasl_mechanism=self.superuser.algorithm,
        )

    def setUp(self):
        super().setUp()

        self.super_rpk.create_topic(self.test_topic)

    @cluster(num_nodes=7)
    @parametrize(consumers_per_group=2500, group_count=4, ordered=False)
    @parametrize(consumers_per_group=2500, group_count=4, ordered=True)
    def test_list_kafka_connections_scale(
        self, consumers_per_group: int, group_count: int, ordered: bool
    ):
        """
        Tests the AdminV2 list_connections endpoint at scale
        """

        client_count = consumers_per_group * group_count

        for swarm_client in self.consumers:
            self.logger.info(f"Starting swarm client on node {swarm_client}")
            swarm_client.start()

        self.logger.info("Waiting for consumer swarm to become ready")

        def consumers_alive() -> bool:
            alive = 0
            for c in self.consumers:
                alive += c.get_metrics_summary().clients_alive

            self.logger.debug(f"{alive} consumers alive so far")
            return alive >= client_count

        wait_until(
            consumers_alive,
            timeout_sec=30 + client_count // 20,
            retry_on_exc=True,
            err_msg="Did not observe expected number of alive consumers",
        )

        self.logger.info("All consumers alive, proceed to list kafka connections")

        conn_filter = (
            f"state = KAFKA_CONNECTION_STATE_OPEN "
            f'AND group_id > "{self.test_group_base}" '
            f'AND group_id < "{self.test_group_max}" '
            f'AND group_member_id != ""'
        )
        conn_order_by = "group_id asc, group_member_id asc" if ordered else None

        def valid_response() -> bool:
            start = time.perf_counter()
            raw_resp = self.super_rpk.cluster_connections_list(
                limit=client_count + 1,
                filter_raw=conn_filter,
                order_by=conn_order_by,
            )
            duration = time.perf_counter() - start

            resp = json.loads(raw_resp)
            conns = resp.get("connections", [])

            matching_conns = [
                conn for conn in conns if len(conn.get("group_member_id", "")) > 0
            ]
            non_matching = [conn for conn in conns if conn not in matching_conns]

            succeeded = len(matching_conns) == client_count and len(non_matching) == 0
            if succeeded:
                self.logger.info(
                    f"Successfully listed all {client_count} expected connections in {duration:.2f}s"
                )
            else:
                self.logger.debug(f"Found {len(matching_conns)} of {client_count}")
                if len(non_matching) > 0:
                    self.logger.debug(f"Non-matching connections: {non_matching}")
            return succeeded

        wait_until(
            valid_response,
            timeout_sec=60,
            retry_on_exc=True,
            err_msg="Did not observe a valid ListKafkaConnectionsResponse",
        )

        for swarm_client in self.consumers:
            self.logger.info(f"Stopping swarm client on node {swarm_client}")
            swarm_client.stop()
